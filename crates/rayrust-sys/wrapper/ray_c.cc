/// C ABI wrapper implementation — bridges Ray C++ SDK to plain C functions.
///
/// This file wraps `ray::internal::GetRayRuntime()` methods directly,
/// bypassing the C++ template API. Serialization uses msgpack.
///
/// IMPORTANT: Ray object/actor IDs are binary strings that may contain
/// null bytes. All ID parameters use (ptr, len) pairs, and all ID
/// returns use ray_bytes_t (ptr + len).

#include "ray_c.h"

#include <ray/api.h>
#include <ray/api/ray_exception.h>
#include <ray/api/ray_runtime.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/serializer.h>
#include <ray/api/function_manager.h>
#include <ray/api/internal_api.h>

// `RayIntentionalSystemExitException` 定义在 `cpp/src/ray/runtime/abstract_ray_runtime.h`，
// wheel 只带 `include/ray/api/*`，所以这里按上游定义重述一次：与 `RayException` 同构，
// 只有一个构造函数。
//
// 它能工作靠的是 C++ ABI 的异常匹配规则。本仓库 `.so` 里这份重述类的 typeinfo 是本地
// 符号：Rust cdylib 的 version script 把编进去的 C++ 符号全部本地化，
// `nm -D --defined-only target/release/librayrust_worker.so` 里没有任何
// `_ZTIN3ray8internal33...` 条目，所以动态链接器没有把它与 libray_api.so 导出的那份统一
// 过，两份 typeinfo 的地址不同。`GetExecuteResult` 的
// `catch (RayIntentionalSystemExitException &)` 仍然能接住，是因为 libstdc++ 在 typeinfo
// 指针不同时退到比较 mangled name：`type_info::operator==` 走
// `strcmp(__name, __arg.name())`，前提是 `__GXX_MERGED_TYPEINFO_NAMES` 为 0（本机
// libstdc++ 的默认值）。两份 typeinfo 的名字都是
// `N3ray8internal33RayIntentionalSystemExitExceptionE`。
//
// 前提是重述类与 SDK 的类**同命名空间、同类名、同布局**。命名空间与类名由名字本身
// 保证；布局用下面的 static_assert 卡住——本类没有数据成员，布局就是基类的布局，SDK
// 给它加成员或换基类时这里编译不过。改名、换基类的重述实测都停在
// `task_executor.cc:101` 的通用 catch（它的日志行在 `:110`）。
//
// 前提不成立时的可观察现象：`__GXX_MERGED_TYPEINFO_NAMES` 为 1 的平台不做名字比较，
// 异常掉到 `GetExecuteResult` 的通用 catch（`cpp/src/ray/runtime/task/task_executor.cc:101`，
// 日志行 `:110`），actor 不会退出、那次方法调用报
// `RayError::Task`。两组对照（C1 抛基类 `RayException`、C2 只把重述类改名）都停在那里，
// actor 继续活着；`crates/rayrust/examples/runtime_api_test.rs` 第 4 项
// （`exit()` 之后的方法必须报错）在那种环境下会失败。
namespace ray {
namespace internal {
class RayIntentionalSystemExitException : public RayException {
 public:
  RayIntentionalSystemExitException(const std::string &msg) : RayException(msg){};
};

static_assert(std::is_base_of_v<RayException, RayIntentionalSystemExitException>,
              "the restated exception must derive from ray::internal::RayException: the "
              "SDK's catch hierarchy is built on that base");
static_assert(sizeof(RayIntentionalSystemExitException) == sizeof(RayException),
              "the restated exception has no data members, so its layout is the base "
              "class's; a member added to the SDK's class would make the thrown object's "
              "layout wrong while the mangled name still matches");
static_assert(alignof(RayIntentionalSystemExitException) == alignof(RayException),
              "the restated exception must keep the SDK class's alignment");
}  // namespace internal
}  // namespace ray

// ─── Thread-local error storage ─────────────────────────────────
// Instead of fprintf(stderr), we store the error message in a thread-local
// string so the Rust side can retrieve it via ray_last_error_bytes().
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <string>
#include <cstring>
#include <type_traits>

static thread_local std::string g_last_error;
static thread_local int g_last_error_kind = RAY_ERROR_KIND_NONE;

static constexpr const char *kRuntimeClosingMessage =
    "the Ray runtime is shutting down or has shut down: calls are refused until a new runtime is initialized";

/// Store an error byte string and its class.
static void set_last_error_kind(const std::string &msg, int kind) {
    g_last_error = msg;
    g_last_error_kind = kind;
}

/// Store a NUL-terminated error message and its class.
static void set_last_error_kind(const char *msg, int kind) {
    set_last_error_kind(std::string(msg ? msg : ""), kind);
}

/// Store an error message (called from catch blocks).
static void set_last_error(const char *msg) {
    set_last_error_kind(msg, RAY_ERROR_KIND_GENERIC);
}

/// Classify a C++ exception by the Ray exception hierarchy.
static int ray_exception_kind(const std::exception &e) {
    using namespace ray::internal;
    if (dynamic_cast<const RayActorException *>(&e)) return RAY_ERROR_KIND_ACTOR;
    if (dynamic_cast<const RayTaskException *>(&e)) return RAY_ERROR_KIND_TASK;
    if (dynamic_cast<const RayWorkerException *>(&e)) return RAY_ERROR_KIND_WORKER;
    if (dynamic_cast<const UnreconstructableException *>(&e)) {
        return RAY_ERROR_KIND_UNRECONSTRUCTABLE;
    }
    if (dynamic_cast<const RayTimeoutException *>(&e)) return RAY_ERROR_KIND_TIMEOUT;
    if (dynamic_cast<const RayException *>(&e)) return RAY_ERROR_KIND_RAY;
    return RAY_ERROR_KIND_GENERIC;
}

/// Store an error message from a C++ exception, with its class.
/// `RayException::what()` returns `msg_.c_str()`, which stops at the first NUL
/// byte; a failed Python task carries a binary payload whose header padding
/// holds such a byte a few bytes in, so `what()` keeps almost nothing and the
/// rest of the message is lost. Read `RayException::msg_`
/// (`ray/api/ray_exception.h:29`) directly instead.
static void set_last_error_exception(const std::exception &e) {
    const int kind = ray_exception_kind(e);
    if (const auto *ray_ex = dynamic_cast<const ray::internal::RayException *>(&e)) {
        set_last_error_kind(ray_ex->msg_, kind);
        return;
    }
    set_last_error_kind(e.what(), kind);
}

// Macro to reduce boilerplate in catch blocks.
#define RAY_CATCH \
    catch (const std::exception &e) { \
        set_last_error_exception(e); \
    }

// Declare the exported function from libray_api.so.
// This returns the SAME FunctionManager singleton that the worker uses.
//
// IMPORTANT: Do NOT use extern "C" here. GetFunctionManager is a C++ function.
// With extern "C", the linker resolves the symbol to the BOOST_DLL_ALIAS
// variable (V type, in .data section), not the actual function (T type, in .text).
namespace ray { namespace internal {
    FunctionManager &GetFunctionManager();
}}

// Forward declarations for the private symbols ray_cancel calls. Neither type
// is declared in the headers the wheel ships (ray/api/*.h), so the layout below
// is copied by hand from the Ray 2.51.1 sources. The static_asserts compare it
// against constants restated in this same file: they catch a mistyped constant,
// not a layout change in the SDK. Only a version check at connect time (#12) or
// a build against two adjacent releases (#14) can catch such a change.
//   - ray::ObjectID is BaseID<ObjectID> under #pragma pack(1): a mutable
//     size_t hash_ at offset 0, then uint8_t id_[kLength], with
//     kLength = sizeof(uint32_t) + TaskID::kLength = 28 (src/ray/common/id.h).
//     BaseID<T>::Data() is `this + sizeof(hash_)`, i.e. id_ at offset 8.
//   - ray::Status is a single State* that is nullptr iff the status is ok
//     (src/ray/common/status.h).
namespace ray {
#pragma pack(push, 1)
    class ObjectID {
    public:
        static constexpr size_t kLength = 28;

        // hash_ stays 0, which BaseID<T>::Hash() reads as "not computed yet".
        ObjectID(const char *data, size_t len) {
            hash_ = 0;
            std::memcpy(id_, data, len);
        }

    private:
        mutable size_t hash_;
        uint8_t id_[kLength];
    };
#pragma pack(pop)

    static_assert(sizeof(ray::ObjectID) == ObjectID::kLength + sizeof(size_t),
                  "ray::ObjectID stand-in does not match the SDK layout");

    class Status {
    public:
        bool ok() const { return state_ == nullptr; }
        // Defined in libray_api.so (the SDK emits it as a weak symbol) and
        // deleted through `delete state_`.
        ~Status();

    private:
        struct State;
        State *state_;
    };

    static_assert(sizeof(ray::Status) == sizeof(void *),
                  "ray::Status stand-in does not match the SDK layout");
}
namespace ray::core {
    class CoreWorker {
    public:
        ray::Status CancelTask(const ray::ObjectID &object_id,
                               bool force_kill,
                               bool recursive);
    };
    class CoreWorkerProcess {
    public:
        static CoreWorker& GetCoreWorker();
    };
}

#include <cstring>
#include <msgpack.hpp>
#include <new>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ─── Function Registration ────────────────────────────────────

// Global map of Rust function callbacks, indexed by function name.
// Uses a function-local static to guarantee initialization order:
// the map is constructed on first call, which is always before any
// #[ctor] registration (which calls ray_register_function).
static std::unordered_map<std::string, ray_func_callback_t> &get_rust_functions() {
    static std::unordered_map<std::string, ray_func_callback_t> map;
    return map;
}

/// Rust 回调用哨兵长度表示「这次调用要按退出处理」。判断必须在
/// `std::free(result.data)` 之前：哨兵没有 data，长度也不能进 `msgpack::sbuffer`。
/// 抛出的异常穿过 C++ 帧回到 `GetExecuteResult` 的 try，那里才是决定成败的地方。
static void throw_for_exit_sentinel(size_t len) {
    if (len == RAY_INTENTIONAL_EXIT_SENTINEL) {
        // 与 AbstractRayRuntime::ExitActor 抛的是同一个类：GetExecuteResult 的
        // `catch (RayIntentionalSystemExitException &)`（cpp/src/ray/runtime/task/task_executor.cc:97，
        // 日志行 `:98`）用它把「故意退出」与「失败」分开。
        throw ray::internal::RayIntentionalSystemExitException("SystemExit");
    }
    if (len == RAY_EXIT_OUTSIDE_ACTOR_SENTINEL) {
        // 任务与 actor 构造函数走这条：GetExecuteResult 的通用
        // `catch (const std::exception &)`（同文件 `:101`，日志行 `:110`）把它变成该次
        // 调用的失败。
        throw ray::internal::RayException(
            "exit_actor() can only be called from inside an actor method");
    }
}

static msgpack::sbuffer invoke_rust_function(const std::string &func_name,
                                              const ray::internal::ArgsBufferList &args_buffer) {
    auto &fns = get_rust_functions();
    auto it = fns.find(func_name);
    if (it == fns.end()) {
        throw std::runtime_error("Rust function not found: " + func_name);
    }

    // Build ray_bytes_t array from ArgsBufferList
    std::vector<ray_bytes_t> args_arr;
    args_arr.reserve(args_buffer.size());
    for (const auto &buf : args_buffer) {
        args_arr.push_back(ray_bytes_t{buf.data(), buf.size()});
    }

    // Call the Rust callback
    ray_bytes_t result = it->second(args_arr.data(), args_arr.size());

    // actor 构造函数通过这个回调进入（ray_register_function），所以哨兵在这里截住。
    throw_for_exit_sentinel(result.len);

    // Copy result into msgpack::sbuffer and free the C-allocated data
    msgpack::sbuffer sbuf(result.len);
    sbuf.write(result.data, result.len);
    std::free(const_cast<char *>(result.data));

    return sbuf;
}

void ray_register_function(const char *func_name, ray_func_callback_t callback) {
    std::string name(func_name);
    auto &fns = get_rust_functions();
    fns[name] = callback;

    auto &fm = ray::internal::GetFunctionManager();
    auto [map_ref, _] = fm.GetRemoteFunctions();
    auto &map = const_cast<ray::internal::RemoteFunctionMap_t &>(map_ref);

    map.emplace(name, [name](const ray::internal::ArgsBufferList &args) -> msgpack::sbuffer {
        return invoke_rust_function(name, args);
    });
}

// ─── Actor Member Function Registration ───────────────────────

// Global map of Rust member function callbacks.
static std::unordered_map<std::string, ray_member_callback_t> &get_rust_member_functions() {
    static std::unordered_map<std::string, ray_member_callback_t> map;
    return map;
}

// Invoke a Rust member function: deserialize actor pointer, forward to callback.
static msgpack::sbuffer invoke_rust_member_function(
    const std::string &func_name,
    msgpack::sbuffer *actor_ptr_buf,
    const ray::internal::ArgsBufferList &args_buffer) {
    auto &fns = get_rust_member_functions();
    auto it = fns.find(func_name);
    if (it == fns.end()) {
        throw std::runtime_error("Rust member function not found: " + func_name);
    }

    // Deserialize the actor pointer (uint64_t) from actor_ptr_buf
    uint64_t actor_ptr = ray::internal::Serializer::Deserialize<uint64_t>(
        actor_ptr_buf->data(), actor_ptr_buf->size());

    // Build ray_bytes_t array from ArgsBufferList
    std::vector<ray_bytes_t> args_arr;
    args_arr.reserve(args_buffer.size());
    for (const auto &buf : args_buffer) {
        args_arr.push_back(ray_bytes_t{buf.data(), buf.size()});
    }

    // Call the Rust callback
    ray_bytes_t result = it->second(actor_ptr, args_arr.data(), args_arr.size());

    // 只有成员回调会返回 RAY_INTENTIONAL_EXIT_SENTINEL。
    throw_for_exit_sentinel(result.len);

    // Copy result into msgpack::sbuffer and free the C-allocated data
    msgpack::sbuffer sbuf(result.len);
    sbuf.write(result.data, result.len);
    std::free(const_cast<char *>(result.data));

    return sbuf;
}

void ray_register_member_function(const char *func_name, ray_member_callback_t callback) {
    std::string name(func_name);
    auto &fns = get_rust_member_functions();
    fns[name] = callback;

    // Register with FunctionManager's map_mem_func_invokers_ via const_cast.
    auto &fm = ray::internal::GetFunctionManager();
    auto [_, mem_map_ref] = fm.GetRemoteFunctions();
    auto &mem_map = const_cast<ray::internal::RemoteMemberFunctionMap_t &>(mem_map_ref);

    auto [it, inserted] = mem_map.emplace(name,
        [name](msgpack::sbuffer *actor_ptr,
               const ray::internal::ArgsBufferList &args) -> msgpack::sbuffer {
            return invoke_rust_member_function(name, actor_ptr, args);
        });

    if (!inserted) {
        mem_map[name] = [name](msgpack::sbuffer *actor_ptr,
               const ray::internal::ArgsBufferList &args) -> msgpack::sbuffer {
            return invoke_rust_member_function(name, actor_ptr, args);
        };
    }

    // Verify registration
    auto *verify = fm.GetMemberFunction(name);
    if (!verify) {
        std::fprintf(stderr,
                     "[rayrust] register_member_fn: GetMemberFunction('%s') returned null (%s, map_size=%zu)\n",
                     name.c_str(),
                     inserted ? "inserted" : "overwritten",
                     mem_map.size());
    }
}

// ─── Helpers ──────────────────────────────────────────────────

/// Copy a std::string (may contain null bytes) into a heap-allocated ray_bytes_t.
///
/// 空串也要拿到非空指针：`malloc(0)` 允许返回 null，而 Rust 侧的 `CBytesGuard`
/// 把 null 当成错误（crates/rayrust-sys/src/lib.rs:218-225），本地模式未配置
/// namespace 时就会把 `""` 读成失败。
static ray_bytes_t dup_bytes(const std::string &s) {
    char *data = static_cast<char *>(std::malloc(s.size() ? s.size() : 1));
    if (!data) return ray_bytes_t{nullptr, 0};
    std::memcpy(data, s.data(), s.size());
    return ray_bytes_t{data, s.size()};
}

/// Decode a hex-encoded string into raw bytes (binary-safe).
/// Returns empty string on malformed input.
static std::string hex_decode(const std::string &hex) {
    if (hex.size() % 2 != 0) return {};
    std::string out;
    out.reserve(hex.size() / 2);
    auto hex_val = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < hex.size(); i += 2) {
        int hi = hex_val(hex[i]);
        int lo = hex_val(hex[i + 1]);
        if (hi < 0 || lo < 0) return {};
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return out;
}

/// `ray::PlacementGroupID::kLength`：14 个唯一字节 + 4 字节 JobID
/// （src/ray/common/id.h:300-306）。wheel 不带 id.h（见 #19），常量在此手抄；
/// `PlacementGroupID::FromBinary` 对非空且长度不符的输入 RAY_CHECK 终止进程，
/// 所以每个拿到 group id 的入口都在进 SDK 之前按它检查长度。
static constexpr size_t kPlacementGroupIdLength = 18;

/// `ray::ActorID` 的长度：12 个唯一字节 + 4 字节 JobID
/// （src/ray/common/id.h:124-142）。手抄的原因与 group id 相同：
/// `ActorID::FromBinary` 对非空且长度不符的输入 RAY_CHECK。
static constexpr size_t kActorIdLength = 16;

/// `ray::JobID` 的长度：`JobID::kLength` 是 4（src/ray/common/id.h:105）。
/// `JobID::FromBinary` 同样 RAY_CHECK 长度；`rpc::ActorHandle` 的
/// `creation_job_id`（字段 4）在第一次调用时按它解码，所以载荷校验按它检查。
/// 字段缺省时解码成 nil job id，调用会在 actor 侧终止 worker（见下面的载荷校验注释）。
static constexpr size_t kJobIdLength = 4;

/// group id 的共同前置检查：非空且正好 18 字节。
static int check_placement_group_id(const char *group_id_data, size_t group_id_len) {
    if (!group_id_data || group_id_len == 0) {
        set_last_error("placement group id is empty");
        return -1;
    }
    if (group_id_len != kPlacementGroupIdLength) {
        std::string msg = "placement group id must be " +
                          std::to_string(kPlacementGroupIdLength) + " bytes, got " +
                          std::to_string(group_id_len) + " bytes";
        set_last_error(msg.c_str());
        return -1;
    }
    return 0;
}

/// object id 的共同前置检查：非空且正好 28 字节。
/// `ObjectID::FromBinary` 对非空且长度不符的输入 RAY_CHECK 终止进程，对空串返回
/// nil（src/ray/common/id.h:430-440），长度必须先在这里拒掉：空串变成 nil 之后
/// `get` 报 TimedOut、`wait` 把它当成永不就绪的对象。
static int check_object_id(const char *id_data, size_t id_len) {
    if (!id_data) {
        set_last_error("object id is null");
        return -1;
    }
    if (id_len != ray::ObjectID::kLength) {
        std::string msg = "object id must be "
            + std::to_string(ray::ObjectID::kLength) + " bytes, got "
            + std::to_string(id_len) + " bytes";
        set_last_error(msg.c_str());
        return -1;
    }
    return 0;
}

/// actor id 的共同前置检查：非空且正好 16 字节。理由与 object id 相同：
/// `ActorID::FromBinary` 对长度不符的输入 RAY_CHECK 终止进程
/// （src/ray/common/id.h:430-441）。
static int check_actor_id(const char *id_data, size_t id_len) {
    if (!id_data) {
        set_last_error("actor id is null");
        return -1;
    }
    if (id_len != kActorIdLength) {
        std::string msg = "actor id must be " + std::to_string(kActorIdLength) +
                          " bytes, got " + std::to_string(id_len) + " bytes";
        set_last_error(msg.c_str());
        return -1;
    }
    return 0;
}

/// 校验 `host:port`，`field` 只用于错误消息（`"address"` 或 `"RAY_ADDRESS"`）。
///
/// 分段方式与上游 `ParseAddress`（src/ray/util/network_util.cc:53-73）一致：按最后一个
/// `:` 分成 host 与 port，host 含 `:` 时必须是 `[` 开头 `]` 结尾再把方括号去掉。剩下的每条都必须自己判，
/// 因为下游 `ConfigInternal::SetBootstrapAddress`
/// （cpp/src/ray/config_internal.cc:253-262）对解析不出来的地址 RAY_CHECK 终止进程，
/// `std::from_chars` 只吃前缀且不查范围：`"127.0.0.1:6399abc"` 静默变成端口 6399，
/// `"127.0.0.1:65536"` 交给后续连接，`RAY_CATCH` 拦不住这两条。
///
/// 空串走调用方：集群模式的空地址表示「没有地址」，不是非法形状；环境变量那一路由
/// `ray_init` 无条件调用，空串在那里按「没有 `:`」拒掉，与上游一致。
static int check_bootstrap_address(const char *field, const std::string &address) {
    const std::string prefix =
        std::string(field) + " \"" + address + "\" is not host:port: ";
    const size_t pos = address.find_last_of(':');
    if (pos == std::string::npos) {
        set_last_error((prefix + "no ':' separator").c_str());
        return -1;
    }
    std::string host = address.substr(0, pos);
    const std::string port = address.substr(pos + 1);
    if (host.find(':') != std::string::npos) {
        if (host.size() < 2 || host.front() != '[' || host.back() != ']') {
            set_last_error(
                (prefix + "an IPv6 host must be bracketed, e.g. \"[::1]:6379\"").c_str());
            return -1;
        }
        host = host.substr(1, host.size() - 2);
    }
    if (host.empty()) {
        set_last_error((prefix + "the host is empty").c_str());
        return -1;
    }
    if (port.empty()) {
        set_last_error((prefix + "the port is empty").c_str());
        return -1;
    }
    // 边加边判：超出 65535 立即拒，`value` 不会在多位数上溢出成负数。
    int value = 0;
    for (const char c : port) {
        if (c < '0' || c > '9') {
            set_last_error((prefix + "the port must be decimal digits").c_str());
            return -1;
        }
        value = value * 10 + (c - '0');
        if (value > 65535) {
            set_last_error((prefix + "the port must be 0..=65535").c_str());
            return -1;
        }
    }
    return 0;
}

/// `bundle_index` 的 JSON 解码与范围检查：-1，或 0 .. INT_MAX。
///
/// wheel 里的 nlohmann 是 3.9.1，`get<int>()` 走 `get_arithmetic_value`
/// （include/nlohmann/json.hpp:3440-3463），里面只有 `static_cast`，没有范围检查。
/// 超出 int 的值被截到低 32 位：`-4294967296` 变成 0（钉在第一个 bundle）、
/// `-4294967297` 变成 -1（当成「组内任意 bundle」），于是「必须是 -1 或非负」的
/// 检查看到的是截断后的值，输入从来没有被拒。所以先按 64 位读出原始值再判断。
///
/// `number_unsigned` 单独走一遍：大于 INT64_MAX 的无符号值读成 `int64_t` 同样只是
/// `static_cast`，`18446744073709551615` 会回绕成 -1 并被当成合法值。调用方已判过
/// `is_number_integer()`，它对 `number_integer` 与 `number_unsigned` 都返回真
/// （json.hpp:18973-18976）。
static int parse_bundle_index(const nlohmann::json &j) {
    const std::string out_of_range =
        "bundle_index must be -1 or between 0 and " +
        std::to_string(std::numeric_limits<int>::max()) + ", got ";
    if (j.is_number_unsigned()) {
        uint64_t raw = j.get<uint64_t>();
        if (raw > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            throw std::runtime_error(out_of_range + std::to_string(raw));
        }
        return static_cast<int>(raw);
    }
    int64_t raw = j.get<int64_t>();
    if (raw < -1 || raw > static_cast<int64_t>(std::numeric_limits<int>::max())) {
        throw std::runtime_error(out_of_range + std::to_string(raw));
    }
    return static_cast<int>(raw);
}

/// `placement_group_id` 与 `bundle_index` 两个字段的解码与校验，任务
/// （`CallOptions`）与 actor（`ActorCreationOptions`）共用。
///
/// 两个字段在进 SDK 之前逐项校验，因为每条兼容路径都会静默换掉语义或终止进程：
/// - 不是 18 字节的 `placement_group_id` 会进
///   `AbstractRayRuntime::GetPlacementGroupById`（cpp/src/ray/runtime/
///   abstract_ray_runtime.cc:353-361），`PlacementGroupID::FromBinary` 对长度不符的
///   输入 RAY_CHECK 终止驱动进程（src/ray/common/id.h:435），RAY_CATCH 拦不住；
/// - `bundle_index` 小于 -1 会进 `ray::AddPlacementGroupConstraint`
///   （src/ray/common/bundle_spec.cc:167）的 RAY_CHECK，同样终止进程；
/// - 查不到的合法 id 拿到空的 `PlacementGroup`（`id_.empty()`，
///   cpp/include/ray/api/task_options.h:85），下游当成「没有组」，任务或 actor 在组外
///   建出来且不报错；
/// - 本地模式没有 GCS accessor，`GetPlacementGroupById` 会解引用空指针，所以本地模式的
///   检查必须在这个调用之前。
///
/// `bundle_index` 由调用方先置 -1：`CallOptions` 与 `ActorCreationOptions` 里的这个
/// 成员都没有默认成员初始化器（cpp/include/ray/api/task_options.h:100、:111），而
/// `GetBundleID`（cpp/src/ray/runtime/task/native_task_submitter.cc:57-65）在组非空时
/// 会读它。JSON 里没有这个键时就保持 -1。
static void parse_placement_group_fields(const nlohmann::json &j,
                                         ray::PlacementGroup &group,
                                         int &bundle_index) {
    const bool has_group = j.contains("placement_group_id");
    if (j.contains("bundle_index")) {
        if (!has_group) {
            throw std::runtime_error(
                "bundle_index requires placement_group_id, but no group was given");
        }
        if (!j["bundle_index"].is_number_integer()) {
            throw std::runtime_error(
                "bundle_index must be a JSON integer, got " +
                j["bundle_index"].dump());
        }
        bundle_index = parse_bundle_index(j["bundle_index"]);
    }
    if (!has_group) return;
    if (!j["placement_group_id"].is_string()) {
        throw std::runtime_error(
            "placement_group_id must be a JSON string of hex, got " +
            j["placement_group_id"].dump());
    }
    std::string pg_hex = j["placement_group_id"].get<std::string>();
    // hex_decode 对空串、奇数个字符、任意非 hex 字符都返回空串。
    std::string pg_id = hex_decode(pg_hex);
    if (pg_id.empty()) {
        throw std::runtime_error(
            "placement group id must be 18 bytes of hex, got \"" + pg_hex +
            "\"");
    }
    if (pg_id.size() != kPlacementGroupIdLength) {
        throw std::runtime_error(
            "placement group id must be " +
            std::to_string(kPlacementGroupIdLength) + " bytes, got " +
            std::to_string(pg_id.size()));
    }
    auto runtime = ray::internal::GetRayRuntime();
    if (!runtime) {
        // 现有调用点都先判过 runtime，这条分支走不到；留着是给以后新增的调用点
        // 兜底，避免它拿着空 runtime 进 GetPlacementGroupById。
        throw std::runtime_error("Ray runtime is not initialized");
    }
    if (runtime->IsLocalMode()) {
        throw std::runtime_error(
            "placement groups are not supported in local mode");
    }
    auto found = runtime->GetPlacementGroupById(pg_id);
    if (found.Empty()) {
        throw std::runtime_error("placement group not found for id " + pg_hex);
    }
    group = found;
}

/// 把 options JSON 里的 `runtime_env` 转成 SDK 的 runtime env info。
///
/// Rust 侧把这个字段当成 JSON 字符串值放进外层 options，所以外层文档没有拼接机会，
/// 标量也不会被当成对象。内容必须是 JSON 对象：转换走
/// `ray::RuntimeEnv::Deserialize(...).SerializeToRuntimeEnvInfo()`
/// （cpp/src/ray/runtime/runtime_env.cc:57-76），它把用户文档的 `dump()` 放进
/// `serialized_runtime_env`，再用 `MessageToJsonString` 生成 proto-JSON，交给 core
/// worker 的永远是合法 proto-JSON。core worker 自己用
/// `RAY_CHECK(JsonStringToMessage(...).ok())` 解析这个字段
/// （src/ray/core_worker/core_worker.cc:1792-1796），原文不是 proto-JSON 时 RAY_CHECK
/// 直接终止驱动进程，RAY_CATCH 拦不住，所以校验必须在这里做完（抛异常由调用侧的
/// catch 转成 `RayError::Ffi`）。任务与 actor 两个入口共用这一份判定。
static void apply_runtime_env(const nlohmann::json &j, std::string &out) {
    if (!j.contains("runtime_env")) return;
    if (!j["runtime_env"].is_string()) {
        throw std::runtime_error(
            "runtime_env must be a JSON string holding a runtime env object, "
            "got " + j["runtime_env"].dump());
    }
    std::string runtime_env_json = j["runtime_env"].get<std::string>();
    auto runtime_env = nlohmann::json::parse(runtime_env_json);
    if (!runtime_env.is_object()) {
        throw std::runtime_error(
            "runtime_env must be a JSON object, got " + runtime_env.dump());
    }
    out = ray::RuntimeEnv::Deserialize(runtime_env_json)
              .SerializeToRuntimeEnvInfo();
}

/// 解析 options JSON 并填充 ActorCreationOptions。`options_json` 可以为 NULL 或
/// 空串（全部取默认值）。
static void parse_actor_options(const char *options_json,
                                ray::internal::ActorCreationOptions &options) {
    if (!options_json || options_json[0] == '\0') return;
    auto j = nlohmann::json::parse(options_json);
    if (j.contains("name")) {
        options.name = j["name"].get<std::string>();
    }
    if (j.contains("ray_namespace")) {
        options.ray_namespace = j["ray_namespace"].get<std::string>();
    }
    if (j.contains("resources") && j["resources"].is_object()) {
        for (auto it = j["resources"].begin(); it != j["resources"].end(); ++it) {
            options.resources[it.key()] = it.value().get<double>();
        }
    }
    if (j.contains("max_restarts")) {
        options.max_restarts = j["max_restarts"].get<int>();
    }
    if (j.contains("max_concurrency")) {
        options.max_concurrency = j["max_concurrency"].get<int>();
    }
    parse_placement_group_fields(j, options.group, options.bundle_index);
    apply_runtime_env(j, options.serialized_runtime_env_info);
}

/// Parse an options JSON string and fill a CallOptions struct.
/// `options_json` may be NULL or empty (returns default options).
///
/// The placement group fields go through `parse_placement_group_fields` and the
/// runtime env through `apply_runtime_env`; both throw instead of letting the SDK
/// terminate the driver or run the task somewhere the caller did not ask for.
static void parse_call_options(const char *options_json,
                               ray::internal::CallOptions &options) {
    if (!options_json || options_json[0] == '\0') return;
    auto j = nlohmann::json::parse(options_json);
    if (j.contains("name")) {
        if (!j["name"].is_string()) {
            throw std::runtime_error(
                "name must be a JSON string, got " + j["name"].dump());
        }
        options.name = j["name"].get<std::string>();
    }
    if (j.contains("resources")) {
        if (!j["resources"].is_object()) {
            throw std::runtime_error("resources must be a JSON object");
        }
        for (auto it = j["resources"].begin(); it != j["resources"].end(); ++it) {
            options.resources[it.key()] = it.value().get<double>();
        }
    }
    parse_placement_group_fields(j, options.group, options.bundle_index);
    apply_runtime_env(j, options.serialized_runtime_env_info);
}

/// 补上任务的默认 CPU：Python 的 `@ray.remote` 给每个任务要 1 个 CPU
/// （`ray/_common/ray_option_utils.py:179` 的 `num_cpus` 默认 1，由
/// `ray/remote_function.py:125-126` 填进 `_num_cpus`），调用方写了别的资源而没写
/// CPU 时也照样合并 1.0（`ray/_common/utils.py:130,147-154` 先复制 `resources` 再按
/// `num_cpus` 写 `CPU`）。C++ SDK 的 `CallOptions` 没有这个默认
/// （`include/ray/api/task_options.h:96-102`），少了这一步任务不占 CPU，
/// `--num-cpus` 限不住并发。
///
/// `emplace` 不覆盖已有的键：显式写 `"CPU":0` 的调用方保留 0。本地模式的
/// `LocalModeTaskSubmitter::SubmitTask` 丢掉整个 `CallOptions`
/// （`local_mode_task_submitter.cc:138-140` 调 `Submit(invocation, {})`，任务规格里的资源表是
/// 空的），这一步在本地模式没有效果。
static void apply_default_task_cpu(ray::internal::CallOptions &options) {
    options.resources.emplace("CPU", 1.0);
}

/// 补上 actor 的默认 CPU：Python 的 `@ray.remote` 在 `resources` 里出现除
/// `"memory"` 与 `"object_store_memory"` 之外的键时，给 actor 的 lifetime 加 1 个 CPU
/// （`ray/actor.py:1660` 用 `not set(resources.keys()).difference({"memory",
/// "object_store_memory"})` 分成两支，`else` 那支取常量
/// `DEFAULT_ACTOR_CREATION_CPU_SPECIFIED = 1`，见 `ray/_private/ray_constants.py:179`）。
/// 内存键进得了这张表却不算资源：
/// `ray/_common/utils.py:136-140` 不允许在 `resources=` 里写 `memory` 与
/// `object_store_memory`，它们只能经 `memory=` 与 `object_store_memory=` 写进同一张表
/// （同文件 `:157-160`），所以「只有内存键」这一支在本绑定里对应写了 `memory` 而没写别的
/// 资源的 `resources`。C++ SDK 的 `ActorCreationOptions` 没有这个默认
/// （`cpp/include/ray/api/task_options.h:104-112`，`resources` 就在这几行），少了这一步
/// actor 只占它自己写的资源，`--num-cpus` 限不住它。
///
/// `emplace` 不覆盖已有的键：显式写 `"CPU":0` 是退出这一默认的方式，显式写
/// `"CPU":2` 保留 2。本地模式的提交器给 actor 的任务规格传的是空资源表
/// （`local_mode_task_submitter.cc:42-43`、`:66-67`），资源字段在本地模式没有效果；
/// `name`、`max_restarts`、`max_concurrency` 仍然会读（同文件 `:146-149`、`:81`、`:84`）。
static void apply_default_actor_cpu(ray::internal::ActorCreationOptions &options) {
    bool names_other_resources = false;
    for (const auto &entry : options.resources) {
        if (entry.first != "memory" && entry.first != "object_store_memory") {
            names_other_resources = true;
            break;
        }
    }
    if (names_other_resources) options.resources.emplace("CPU", 1.0);
}

// ─── Lifecycle ────────────────────────────────────────────────

/// `ray_init` 收到的 job namespace。
///
/// 本地模式不启动 core worker，`AbstractRayRuntime::GetNamespace`
/// （cpp/src/ray/runtime/abstract_ray_runtime.cc:374-377）第一句
/// `CoreWorkerProcess::GetCoreWorker()` 在 core worker 不存在时走
/// `EnsureInitialized` 的 `QuickExit()` 终止进程
/// （src/ray/core_worker/core_worker_process.cc:842-850）。本地模式也没有 job config，
/// 而 `ConfigInternal` 的类定义（`config_internal.h`）不在 wheel 的 `ray/cpp/include`
/// 里：`ConfigInternal::Instance()` 虽然是 `libray_api.so` 导出的弱符号，但没有成员
/// 布局就定位不到 `ray_namespace`，所以这里留一份 init 时传入的值给
/// `ray_get_namespace` 在本地模式返回。
namespace {

std::mutex &init_namespace_mutex() {
    static std::mutex mutex;
    return mutex;
}

std::string &init_namespace() {
    static std::string value;
    return value;
}

}  // namespace

/// 读回 `ray_init` 记下的 namespace（空串表示没有配置过）。
static std::string saved_init_namespace() {
    std::lock_guard<std::mutex> lock(init_namespace_mutex());
    return init_namespace();
}

static void open_runtime_gate(void);

namespace {

/// 本地模式提交过的任务结果 id。线程池在 `~LocalModeTaskSubmitter` 里 join，而
/// `object_store_` 先于 `task_submitter_` 析构（cpp/src/ray/runtime/abstract_ray_runtime.h:122-124），
/// 在途任务会往已释放的 store 写结果。`ray_shutdown` 因此要等这些 id 全部就绪。
struct LocalTaskBookkeeping {
    std::mutex mutex;
    std::vector<std::string> ids;
    /// 关闭已经开始，从取快照一直到 `ray::Shutdown()` 返回：拒绝新的本地任务提交，
    /// 拒绝排空期间的 `ray_init`，拒绝第二个并发的 `ray_shutdown`。
    bool shutting_down = false;
};

LocalTaskBookkeeping &local_tasks() {
    static LocalTaskBookkeeping bookkeeping;
    return bookkeeping;
}

/// id 列表的裁剪阈值：长跑的本地驱动每提交一个任务就增加 28 字节，达到这个大小时先丢掉已就绪的。
constexpr size_t kLocalTaskPruneThreshold = 1024;

/// 持 `local_tasks().mutex` 调用。用一次零期限的 `Wait` 取就绪快照，把已经就绪的 id 删掉。
/// 本地模式下 `Wait` 的 `MemoryStore::Wait` 会忽略 TimedOut 并把找到的都报回来
/// （src/ray/core_worker/store_provider/memory_store/memory_store.cc:450-486）。
void prune_ready_local_tasks(ray::internal::RayRuntime &runtime,
                             std::vector<std::string> &ids) {
    if (ids.empty()) return;
    auto ready = runtime.Wait(ids, static_cast<int>(ids.size()), 0);
    std::vector<std::string> remaining;
    remaining.reserve(ids.size());
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i >= ready.size() || !ready[i]) {
            remaining.push_back(std::move(ids[i]));
        }
    }
    ids.swap(remaining);
}

}  // namespace

int ray_init(const char *address, int local_mode, const char *node_ip,
             const char *code_search_path,
             const char *runtime_env_json,
             const char *log_dir,
             int actor_lifetime,
             const char *ns) {
    const bool local = local_mode != 0;
    ray::RayConfig config;
    // 本地模式不看地址：`ConfigInternal::Init` 只在 `config.address` 非空时调
    // `SetBootstrapAddress`（cpp/src/ray/config_internal.cc:124-126），而 `SINGLE_PROCESS`
    // 建的是 `LocalModeRayRuntime`（cpp/src/ray/runtime/abstract_ray_runtime.cc:48-51），
    // 本地模式没有 bootstrap 地址可用。
    //
    // 交给 SDK 的不合规地址在上游分四种结局：`ParseAddress` 解析不出来的停在
    // `config_internal.cc:255`，端口为空或不是十进制数字的停在 `:261`，端口超出 65535
    // 或带尾缀被 `std::from_chars` 截断的交给后续连接、约 35 秒后停在
    // `process_helper.cc:110`，负端口停在 `util.cc:42`；空 host 让上游把 bootstrap 地址
    // 当成没设置，转头去起 head 节点（`process_helper.cc:84-92`）。
    //
    // 两处校验都在 `init_namespace()` 那次写入与 `local_tasks().mutex` 之前：被拒的
    // init 不该改掉 `ray_get_namespace` 读的那份值，也不该在关闭闸门上排队。
    if (!local && address && address[0] != '\0') {
        if (check_bootstrap_address("address", address) != 0) return -1;
        config.address = address;
    }
    config.local_mode = local;
    // 空地址时下游会读 `RAY_ADDRESS`（cpp/src/ray/config_internal.cc:207-215）并走
    // 同一个 `SetBootstrapAddress`，所以环境变量按同一规则校验；被设置时（包括空串）
    // 都要判，空串在那里同样是解析不出地址的输入。
    if (!local && config.address.empty()) {
        const char *env = std::getenv("RAY_ADDRESS");
        if (env && check_bootstrap_address("RAY_ADDRESS", env) != 0) return -1;
    }

    // Set default actor lifetime: 0 = NON_DETACHED (default), 1 = DETACHED.
    if (actor_lifetime == 1) {
        config.default_actor_lifetime = ray::ActorLifetime::DETACHED;
    }

    // Set job-level namespace for named actors.
    if (ns && ns[0] != '\0') {
        config.ray_namespace = ns;
    }

    // 同一处留一份给本地模式的 `ray_get_namespace`；没传就是空串。
    {
        std::lock_guard<std::mutex> lock(init_namespace_mutex());
        init_namespace() = config.ray_namespace;
    }

    // Set code search path for worker to find .so with remote functions.
    if (code_search_path && code_search_path[0] != '\0') {
        std::string paths(code_search_path);
        size_t start = 0, end;
        while ((end = paths.find(':', start)) != std::string::npos) {
            if (end > start) {
                config.code_search_path.push_back(paths.substr(start, end - start));
            }
            start = end + 1;
        }
        if (start < paths.size()) {
            config.code_search_path.push_back(paths.substr(start));
        }
    }

    // Set runtime_env from JSON string
    if (runtime_env_json && runtime_env_json[0] != '\0') {
        config.runtime_env = ray::RuntimeEnv::Deserialize(std::string(runtime_env_json));
    }

    // Build argv to pass node_ip_address and log_dir via command-line flags.
    int argc = 1;
    std::vector<std::string> arg_strings;
    std::vector<const char *> arg_ptrs;

    arg_strings.push_back("rayrust");
    arg_ptrs.push_back(arg_strings[0].c_str());

    if (node_ip && node_ip[0] != '\0') {
        arg_strings.push_back("--ray_node_ip_address");
        arg_strings.push_back(node_ip);
        arg_ptrs.push_back(arg_strings[1].c_str());
        arg_ptrs.push_back(arg_strings[2].c_str());
        argc = 3;
    }

    if (log_dir && log_dir[0] != '\0') {
        arg_strings.push_back("--ray_logs_dir");
        arg_strings.push_back(log_dir);
        arg_ptrs.push_back(arg_strings[argc].c_str());
        arg_ptrs.push_back(arg_strings[argc + 1].c_str());
        argc += 2;
    }

    try {
        std::lock_guard<std::mutex> lock(local_tasks().mutex);
        if (local_tasks().shutting_down) {
            set_last_error("a ray_shutdown is still in progress");
            return -1;
        }
        // 上游 `ray::Init` 在已经初始化时什么都不做（raysrc/cpp/src/ray/api.cc:25-30）：
        // 运行时没有换，登记必须保留，否则在途任务不会被排空。
        const bool already_initialized = ray::IsInitialized();
        ray::Init(config, argc, const_cast<char**>(arg_ptrs.data()));
        open_runtime_gate();
        if (!already_initialized) {
            local_tasks().ids.clear();
            local_tasks().shutting_down = false;
        }
        return 0;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return -1;
    }
}

bool ray_is_initialized(void) {
    return ray::IsInitialized();
}

/// 运行时关闭闸门。
///
/// `CoreWorkerProcess::Shutdown()`（`src/ray/core_worker/core_worker_process.cc:101-108`）
/// 先调 `ShutdownDriver()` 把 `core_worker_` 置空（同文件 `:945-957`），再执行
/// `core_worker_process.reset()`；`RayRuntimeHolder::runtime_`
/// （`cpp/include/ray/api/ray_runtime_holder.h:22-45`）在 `DoShutdown()`
/// （`cpp/src/ray/runtime/abstract_ray_runtime.cc:72-77`）里不会被清空，
/// `GetRayRuntime()` 在关闭开始与结束之后仍返回非空指针。关闭开始后访问 core worker
/// 会在 `CoreWorkerProcessImpl::GetCoreWorker()`（`core_worker_process.cc:964-984`）或
/// `CoreWorkerProcess::EnsureInitialized`（同文件 `:842-856`）里调用 `QuickExit()`
/// （`_Exit(1)`，`src/ray/util/process.cc:845-848`）直接终止进程，Rust 侧没有捕获机会。
///
/// 关闭标志 `runtime_closing()` 有两类读者：
/// - 24 个取运行时的入口经 `runtime_for_call()` 读这个原子标志，命中时写入
///   `RAY_ERROR_KIND_RUNTIME` 并立即拒绝，不等待也不计入在途计数（`get`/`put`/`wait`
///   等阻塞调用不能让关闭等待，例如无期限的 `Get`）；
/// - 两个引用计数入口在 `ref_count_gate_mutex()` 下读这个标志并计入 `ref_count_in_flight()`，
///   关闭一开始拒绝新的引用计数调用，并等待已经进入的调用退出（微秒级），采用「在途计数 +
///   关闭标志」可保证关闭线程不被持续涌入的读者饿住。
namespace {

std::mutex &ref_count_gate_mutex() {
    static std::mutex mutex;
    return mutex;
}

std::condition_variable &ref_count_gate_drained() {
    static std::condition_variable drained;
    return drained;
}

int &ref_count_in_flight() {
    static int in_flight = 0;
    return in_flight;
}

/// 关闭标志：`ray_shutdown` 一开始置位，`ray_init` 成功之后清掉。
/// 每个取运行时的入口在碰运行时之前读一次，命中就拒。
std::atomic<bool> &runtime_closing() {
    static std::atomic<bool> closing{false};
    return closing;
}

}  // namespace

/// 进入一次引用计数调用。关闭已经开始时返回 false，调用方跳过计数。
static bool enter_ref_count(void) {
    std::lock_guard<std::mutex> lock(ref_count_gate_mutex());
    if (runtime_closing()) return false;
    ++ref_count_in_flight();
    return true;
}

static void leave_ref_count(void) {
    std::lock_guard<std::mutex> lock(ref_count_gate_mutex());
    --ref_count_in_flight();
    if (ref_count_in_flight() == 0) ref_count_gate_drained().notify_all();
}

/// `ray_shutdown` 一开始调用：置位关闭标志以拒绝所有取运行时的入口与新的引用计数
/// 调用，并等待已经进入的引用计数调用退出。
static void close_runtime_gate(void) {
    std::unique_lock<std::mutex> lock(ref_count_gate_mutex());
    runtime_closing() = true;
    ref_count_gate_drained().wait(lock, [] { return ref_count_in_flight() == 0; });
}

/// `ray_init` 成功之后调用：清除关闭标志，重新放行所有取运行时的入口与引用计数调用。
///
/// 只清关闭标志。在途计数不能在这里抹平：`ray_init` 与引用计数调用没有互斥，
/// 清零之后那几次在途调用的 `leave` 会把计数减成负数，`close_runtime_gate`
/// 的谓词从此不成立，关闭永远等下去。不变量成立时它本来就是 0。
static void open_runtime_gate(void) {
    std::lock_guard<std::mutex> lock(ref_count_gate_mutex());
    runtime_closing() = false;
}

/// 一次引用计数调用的在途凭证：析构时退出闸门，异常路径也不会漏掉。
struct RefCountTicket {
    RefCountTicket() : entered(enter_ref_count()) {}
    ~RefCountTicket() {
        if (entered) leave_ref_count();
    }
    RefCountTicket(const RefCountTicket &) = delete;
    RefCountTicket &operator=(const RefCountTicket &) = delete;

    bool entered;
};

/// 取这次调用要用的运行时。没有运行时、或运行时正在关闭时返回空，并写好
/// ray_last_error_bytes()。关闭标志用 RAY_ERROR_KIND_RUNTIME 的类别，Rust 侧据此得到
/// RayError::Runtime。
static std::shared_ptr<ray::internal::RayRuntime> runtime_for_call() {
    auto runtime = ray::internal::GetRayRuntime();
    if (!runtime) {
        set_last_error("Ray runtime is not initialized");
        return nullptr;
    }
    if (runtime_closing()) {
        set_last_error_kind(kRuntimeClosingMessage, RAY_ERROR_KIND_RUNTIME);
        return nullptr;
    }
    return runtime;
}

namespace {

/// 提交普通任务；本地模式持锁检查关闭标志、按阈值裁剪并记录返回的 object id。
/// 集群模式直接提交，不加锁、不记录。
ray_bytes_t submit_task_and_record(
    ray::internal::RayRuntime &runtime,
    const ray::internal::RemoteFunctionHolder &holder,
    std::vector<ray::internal::TaskArg> &task_args,
    const ray::internal::CallOptions &options) {
    if (!runtime.IsLocalMode()) {
        std::string id = runtime.Call(holder, task_args, options);
        return dup_bytes(id);
    }
    // 提交与 `ray_shutdown` 的排空用同一把锁互斥：排空取走快照之后不会再有 id 进来，
    // 关闭开始之后提交的本地任务直接被拒。
    std::string id;
    {
        std::lock_guard<std::mutex> lock(local_tasks().mutex);
        // 关闭已经开始；或者调用方取到 `runtime` 之后已经发生了关闭（关闭已结束，
        // 或关闭之后又建了另一个运行时）。两种情况都不能再用这条运行时：它的线程池
        // 已经 join 过，再投递任务会让进程在退出析构时崩溃。
        if (local_tasks().shutting_down || !ray::IsInitialized() ||
            ray::internal::GetRayRuntime().get() != &runtime) {
            set_last_error("Ray runtime is shutting down; refusing new local-mode task");
            return ray_bytes_t{nullptr, 0};
        }
        auto &ids = local_tasks().ids;
        if (ids.size() >= kLocalTaskPruneThreshold) {
            prune_ready_local_tasks(runtime, ids);
        }
        id = runtime.Call(holder, task_args, options);
        if (!id.empty()) {
            ids.push_back(id);
        }
    }
    return dup_bytes(id);
}

}  // namespace

void ray_shutdown(void) {
    std::shared_ptr<ray::internal::RayRuntime> runtime;
    std::vector<std::string> pending;
    {
        std::lock_guard<std::mutex> lock(local_tasks().mutex);
        // 另一个线程已经在关闭：这一次直接返回，由那一次把运行时关掉。
        if (local_tasks().shutting_down) return;
        local_tasks().shutting_down = true;
        // 关闭标志必须在本地任务排空之前置位：排空期间放进来的调用可能活过排空、
        // 随后撞上 `ray::Shutdown()`。锁的次序固定为 `local_tasks().mutex` ->
        // `ref_count_gate_mutex()`；`leave_ref_count` 只取后者，不会反过来取前者。
        close_runtime_gate();
        pending.swap(local_tasks().ids);
        // 与快照同一把锁：排空的 id 一定属于这个运行时；这条引用也让它在排空期间活下来。
        runtime = ray::internal::GetRayRuntime();
    }
    if (runtime && runtime->IsLocalMode() && !pending.empty()) {
        try {
            runtime->Wait(pending, static_cast<int>(pending.size()), -1);
        } catch (const std::exception &e) {
            // `ray_shutdown` 是 extern "C" 且没有错误通道；排空失败时任务可能仍在运行，
            // 让运行时消失会导致悬垂访问，就地终止并打印原因。
            fprintf(stderr,
                    "ray_shutdown: waiting for submitted local-mode tasks failed: %s\n",
                    e.what());
            std::abort();
        } catch (...) {
            fprintf(stderr,
                    "ray_shutdown: waiting for submitted local-mode tasks failed: unknown exception\n");
            std::abort();
        }
    }
    {
        // 关闭完成之前一直持锁：`ray_init` 也取这把锁，于是「初始化」与「关闭」互斥。
        std::lock_guard<std::mutex> lock(local_tasks().mutex);
        runtime.reset();
        ray::Shutdown();
        local_tasks().shutting_down = false;
    }
}

// ─── Object Store ─────────────────────────────────────────────

/// 零长度结果：Ray 将该对象标记为失败，其错误类型不在 C++ SDK 翻译的七种之内且未附带错误文本。
///
/// 上游 `MakeErrorMetadataBuffer`（`src/ray/common/ray_object.cc:36-40`）将 `ErrorType`
/// 写成十进制元数据字符串；无 `RayErrorInfo` 的失败对象由
/// `RayObject::RayObject(rpc::ErrorType, const rpc::RayErrorInfo *)`
/// （`src/ray/common/ray_object.cc:97-106`，无载荷分支在 `:98-101`）构造，只有元数据、
/// 没有数据缓冲区。`NativeObjectStore::CheckException`
/// （`cpp/src/ray/runtime/object/native_object_store.cc:70-88`）只把 27 种 `ErrorType`
/// 中的七种翻译成 C++ 异常，其余不抛出异常；随后 `NativeObjectStore::GetRaw`
/// （同文件 `:90-134`，逐元素处理在 `:104-132`）把空数据缓冲区复制成长度为 0 的
/// `msgpack::sbuffer` 并丢弃元数据。公开运行时接口（`ray/api/ray_runtime.h:58-68`）的
/// `Get` 只返回 `msgpack::sbuffer`，没有读取对象元数据的入口。
/// 本 crate 写入对象存储的每个值都经过 msgpack 序列化，至少占一字节（`nil` 为 `0xc0`），
/// 因此 `Get` 成功返回零字节缓冲区只来自这类无错误文本的未翻译失败对象。
/// 携带 `RayErrorInfo` 的未翻译类型（如 `RUNTIME_ENV_SETUP_FAILED`、
/// `TASK_UNSCHEDULABLE_ERROR` 与 actor 任务取消，见 `ray_object.cc:103-105`、
/// `src/ray/core_worker/task_submission/normal_task_submitter.cc:371-386` 及
/// `src/ray/core_worker/task_submission/actor_task_submitter.cc:846-853`、`:982-990`）
/// 载荷为 `9 + N` 字节的 protobuf 帧，由 Rust 侧 `error_payload::extract` 在读取时
/// （解码之前）识别并转为 `RayError::Unclassified`，不进入零长度分支。
static const char *kUntranslatedObjectError =
    "the object carries no data and Ray raised no exception: Ray failed it with an "
    "error type the C++ SDK does not translate and that records no error text "
    "(TASK_CANCELLED of a task is the common case; the SDK translates seven of the 27 "
    "rpc::ErrorType values)";

ray_bytes_t ray_put(const char *data, size_t len) {
    ray_clear_error();
    try {
        // 零长度在进入 SDK 之前就被拒：`msgpack::sbuffer(0).data()` 是空指针，
        // `LocalMemoryBuffer` 对空指针 RAY_CHECK（`src/ray/common/buffer.h:77`），
        // 两种模式都会终止进程。读侧的「零长度只可能是未翻译失败对象」这条前提
        // 因此有了写侧保证。
        if (len == 0) {
            set_last_error("the object store cannot store a zero-length value: Ray's "
                           "LocalMemoryBuffer requires a non-null data pointer "
                           "(src/ray/common/buffer.h:77), and every serialized value is "
                           "at least one byte");
            return ray_bytes_t{nullptr, 0};
        }
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        // The data from Rust is already msgpack-serialized (by rmp-serde).
        // Write it directly into the sbuffer without wrapping in pack_bin,
        // because the C++ SDK stores the buffer as-is and returns it as-is
        // on Get. Wrapping would add an extra Bin8 marker that breaks
        // deserialization on the Rust side.
        auto buffer = std::make_shared<msgpack::sbuffer>(len);
        buffer->write(data, len);

        std::string id = runtime->Put(buffer);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

ray_bytes_t ray_get(const char *id_data, size_t id_len, int timeout_ms) {
    ray_clear_error();
    try {
        // `-1` 表示永久等待，更小的值会让上游 memory_store.cc 的
        // RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1) 终止进程。
        if (timeout_ms < -1) {
            set_last_error("timeout_ms must be -1 or non-negative");
            return ray_bytes_t{nullptr, 0};
        }
        if (check_object_id(id_data, id_len) != 0) return ray_bytes_t{nullptr, 0};
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string id(id_data, id_len);
        auto buffer = runtime->Get(id, timeout_ms);
        if (!buffer) {
            set_last_error("the object store returned no buffer");
            return ray_bytes_t{nullptr, 0};
        }

        const size_t size = buffer->size();
        if (size == 0) {
            set_last_error_kind(kUntranslatedObjectError, RAY_ERROR_KIND_UNCLASSIFIED);
            return ray_bytes_t{nullptr, 0};
        }
        char *data = static_cast<char *>(std::malloc(size));
        if (!data) {
            set_last_error("out of memory copying the object");
            return ray_bytes_t{nullptr, 0};
        }
        std::memcpy(data, buffer->data(), size);
        return ray_bytes_t{data, size};
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

// ─── Reference Counting ──────────────────────────────────────

int ray_add_local_reference(const char *id_data, size_t id_len) {
    ray_clear_error();
    RefCountTicket ticket;
    if (!ticket.entered) return 0;  // 关闭已经开始：core worker 即将消失，无需计数
    try {
        if (check_object_id(id_data, id_len) != 0) return -1;
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return -1;
        }
        runtime->AddLocalReference(std::string(id_data, id_len));
        return 0;
    } RAY_CATCH
    return -1;
}

int ray_remove_local_reference(const char *id_data, size_t id_len) {
    ray_clear_error();
    RefCountTicket ticket;
    if (!ticket.entered) return 0;  // 关闭已经开始：core worker 即将消失，无需计数
    try {
        if (check_object_id(id_data, id_len) != 0) return -1;
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return -1;
        }
        runtime->RemoveLocalReference(std::string(id_data, id_len));
        return 0;
    } RAY_CATCH
    return -1;
}

bool *ray_wait(const ray_bytes_t *ids, size_t count, size_t num_objects, int timeout_ms) {
    ray_clear_error();
    try {
        if (timeout_ms < -1) {
            set_last_error("timeout_ms must be -1 or non-negative");
            return nullptr;
        }
        if (count > 0 && !ids) {
            set_last_error("object id array is null");
            return nullptr;
        }
        for (size_t i = 0; i < count; i++) {
            if (check_object_id(ids[i].data, ids[i].len) != 0) return nullptr;
        }
        // 数量与重复 id 这两条是 Ray 自己的规则（src/ray/core_worker/core_worker.cc:1475-1483）：
        // 集群模式的 core worker 不看对象是否就绪，两类参数一律拒绝；本地模式没有 core
        // worker，永远走不到那里，同一个输入在那里要么给出错误答案、要么终止进程。
        // 终止那一路要两条同时成立：数量大于去重后的集合大小，且去重后的 id 里还有没进
        // 本地存储的。本地模式把 id 收进 flat_hash_set 去重之后仍把原数量传给 memory_store
        // （local_mode_object_store.cc:82-97），这时 GetImpl 构造的 GetRequest 数量大于它
        // 持有的 id 个数，memory_store.cc:89 的 RAY_CHECK 终止整个进程，RAY_CATCH 抓不到；
        // 全部 id 都已经在本地存储里时 GetImpl 在构造 GetRequest 之前就返回 Status::OK，
        // 同一个输入得到的是 Ok。两种情形都要在碰 runtime 之前拒掉，才能与集群模式一致。
        //
        // 上界同时挡住超过 int 的值：SDK 的 `Wait` 只收 int，下面的 static_cast 因此安全。
        if (num_objects < 1 || num_objects > count ||
            num_objects > static_cast<size_t>(std::numeric_limits<int>::max())) {
            set_last_error(
                "Number of objects to wait for must be between 1 and the number of ids.");
            return nullptr;
        }
        // 28 字节的 id 没有额外信息，直接按字节比较。
        std::unordered_set<std::string> seen_ids;
        for (size_t i = 0; i < count; i++) {
            if (!seen_ids.emplace(ids[i].data, ids[i].len).second) {
                set_last_error("Duplicate object IDs not supported in wait.");
                return nullptr;
            }
        }
        auto runtime = runtime_for_call();
        if (!runtime) return nullptr;

        std::vector<std::string> id_vec;
        id_vec.reserve(count);
        for (size_t i = 0; i < count; i++) {
            id_vec.emplace_back(ids[i].data, ids[i].len);
        }

        auto results = runtime->Wait(id_vec, static_cast<int>(num_objects), timeout_ms);

        bool *out = static_cast<bool *>(std::malloc(count * sizeof(bool)));
        if (!out) return nullptr;
        for (size_t i = 0; i < count && i < results.size(); i++) {
            out[i] = results[i];
        }
        return out;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return nullptr;
    }
}

// ─── Task ─────────────────────────────────────────────────────

/// 引用参数的宽度检查：`is_ref[i]` 为真时 `args[i]` 必须是 28 字节的 ObjectID。
/// 宽度不符的 id 交给 Ray 会在 `ObjectID::FromBinary` 的 RAY_CHECK 终止进程
/// （src/ray/common/id.h:430-441）；本地模式更早一步用无期限的 `runtime->Get(id)`
/// 读值，0 字节在那里永久等待。所以每个引用参数都要在构造 TaskArg 之前拒掉，
/// 抛出的消息由入口的 RAY_CATCH 存进 ray_last_error_bytes()。
static void check_reference_arg_ids(const ray_bytes_t *args, size_t arg_count,
                                    const bool *is_ref) {
    if (!is_ref) return;
    if (arg_count > 0 && !args) {
        throw std::invalid_argument("argument array is null");
    }
    for (size_t i = 0; i < arg_count; i++) {
        if (!is_ref[i]) continue;
        if (!args[i].data) {
            throw std::invalid_argument(
                "argument " + std::to_string(i) +
                " is passed by reference, but its id is null");
        }
        if (args[i].len != ray::ObjectID::kLength) {
            throw std::invalid_argument(
                "argument " + std::to_string(i) +
                " is passed by reference, so its id must be " +
                std::to_string(ray::ObjectID::kLength) + " bytes, got " +
                std::to_string(args[i].len) + " bytes");
        }
    }
}

/// Helper: build TaskArg vector for CPP tasks.
/// `is_ref` array marks which args are ObjectRef references (pass by ID)
/// vs value args (pass by serialized msgpack). It must be NULL or have one
/// entry per arg; the Rust side rejects any other length.
///
/// In local mode there is no core worker to deliver the referenced object, so a
/// reference arg is read into a value before submission, the same thing the SDK
/// does in local mode (ray/api/arguments.h:43-52). That read has no deadline,
/// like `get()`: a two-step graph submits a reference whose task has not
/// finished yet, and waiting for it is the point of the feature. The `!buffer`
/// branch therefore only covers the case where Ray itself cannot produce a
/// value; an id of unknown origin never gets here, the Rust side rejects it.
/// In cluster mode the id stays an id and the core worker delivers the value to
/// the worker.
static std::vector<ray::internal::TaskArg>
build_task_args_cpp(ray::internal::RayRuntime *runtime,
                    const ray_bytes_t *args, size_t arg_count,
                    const bool *is_ref = nullptr) {
    check_reference_arg_ids(args, arg_count, is_ref);
    std::vector<ray::internal::TaskArg> task_args;
    task_args.reserve(arg_count);
    for (size_t i = 0; i < arg_count; i++) {
        ray::internal::TaskArg arg;
        if (is_ref && is_ref[i]) {
            // ObjectRef argument: pass by reference (object ID)
            std::string id(args[i].data, args[i].len);
            if (runtime->IsLocalMode()) {
                auto buffer = runtime->Get(id);
                if (!buffer) {
                    throw std::runtime_error(
                        "cannot read the object referenced by arg " + std::to_string(i));
                }
                arg.buf = std::move(*buffer);
            } else {
                arg.id = std::move(id);
            }
        } else {
            // Value argument: raw msgpack
            arg.buf = msgpack::sbuffer(args[i].len);
            arg.buf->write(args[i].data, args[i].len);
        }
        task_args.push_back(std::move(arg));
    }
    return task_args;
}

/// Helper: build TaskArg vector for Python tasks (xlang wrapped).
/// Args from Rust are ALREADY msgpack-serialized by rmp-serde.
/// We must NOT call Serializer::Serialize on them again — that would
/// wrap the data in pack_bin/bin8, which Python's msgpack.unpackb
/// cannot decode (expects the original msgpack type markers).
///
/// XLANG format per C++ SDK's Arguments::WrapArgsImpl:
///   [dummy_kwargs (RAW metadata)]  +  [xlang_header + raw_data (XLANG metadata)]
/// where xlang_header = [msgpack_int(data_len)] [zero_padding to XLANG_HEADER_LEN]
static std::vector<ray::internal::TaskArg>
build_task_args_python(const ray_bytes_t *args, size_t arg_count,
                       const bool *is_ref = nullptr) {
    check_reference_arg_ids(args, arg_count, is_ref);
    std::vector<ray::internal::TaskArg> task_args;
    task_args.reserve(arg_count * 2);
    for (size_t i = 0; i < arg_count; i++) {
        // Check if this arg is an ObjectRef (pass by reference)
        if (is_ref && is_ref[i]) {
            // ObjectRef argument: pass by reference (object ID)
            // Ray's core will fetch the data from the store and deliver to Python.
            // We set meta_str = XLANG so Python knows to use xlang deserialization.
            // The stored data must have xlang header (use put_xlang() on Rust side).
            ray::internal::TaskArg dummy;
            dummy.buf = msgpack::sbuffer(ray::internal::METADATA_STR_DUMMY.size());
            dummy.buf->write(ray::internal::METADATA_STR_DUMMY.data(),
                             ray::internal::METADATA_STR_DUMMY.size());
            dummy.meta_str = ray::internal::METADATA_STR_RAW;
            task_args.push_back(std::move(dummy));

            ray::internal::TaskArg ref_arg;
            ref_arg.id = std::string(args[i].data, args[i].len);
            ref_arg.meta_str = ray::internal::METADATA_STR_XLANG;
            task_args.push_back(std::move(ref_arg));
            continue;
        }

        // Value argument: wrap with xlang header
        // 1. Dummy kwargs (for Python)
        ray::internal::TaskArg dummy;
        dummy.buf = msgpack::sbuffer(ray::internal::METADATA_STR_DUMMY.size());
        dummy.buf->write(ray::internal::METADATA_STR_DUMMY.data(),
                         ray::internal::METADATA_STR_DUMMY.size());
        dummy.meta_str = ray::internal::METADATA_STR_RAW;
        task_args.push_back(std::move(dummy));

        // 2. Data with xlang header
        //    data_len encoded as msgpack int, then zero-padded to XLANG_HEADER_LEN
        //    Then raw msgpack bytes (already serialized by Rust's rmp-serde)
        auto len_buf = ray::internal::Serializer::Serialize(args[i].len);

        ray::internal::TaskArg xlang_arg;
        xlang_arg.buf = msgpack::sbuffer(ray::internal::XLANG_HEADER_LEN + args[i].len);
        xlang_arg.buf->write(len_buf.data(), len_buf.size());
        for (size_t j = len_buf.size(); j < ray::internal::XLANG_HEADER_LEN; ++j) {
            xlang_arg.buf->write("", 1);
        }
        // Write raw msgpack bytes directly — no Serializer::Serialize wrapping
        xlang_arg.buf->write(args[i].data, args[i].len);
        xlang_arg.meta_str = ray::internal::METADATA_STR_XLANG;
        task_args.push_back(std::move(xlang_arg));
    }
    return task_args;
}

/// Call a remote task with full CallOptions.
/// `options_json` is a JSON object string; see ray_c.h for the field set.
ray_bytes_t ray_task_call_with_options(const char *func_name,
                                         const ray_bytes_t *args,
                                         size_t arg_count,
                                         const bool *is_ref,
                                         const char *options_json) {
    ray_clear_error();
    if (!func_name) {
        set_last_error("func_name is null");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder{std::move(fn)};
        // 引用参数先在这里查一次（`build_task_args_cpp` 内部还会再查，幂等），让引用参数
        // 错误优先于关闭错误，与 `ray_c.h` 的判定次序一致。
        check_reference_arg_ids(args, arg_count, is_ref);

        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        auto task_args = build_task_args_cpp(runtime.get(), args, arg_count, is_ref);

        // The SDK leaves bundle_index uninitialized in CallOptions; -1 means
        // "any bundle" and must be set before parse_call_options may leave it.
        ray::internal::CallOptions options;
        options.bundle_index = -1;
        parse_call_options(options_json, options);
        apply_default_task_cpu(options);
        return submit_task_and_record(*runtime, holder, task_args, options);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

/// 提交 Python 任务的共同实现：名字检查、本地模式判定、参数构造与提交都在这里，
/// 两个入口只差一个 options JSON。`options_json` 可以是 NULL 或空串（不设任何选项）。
static ray_bytes_t submit_python_task(const char *module_name,
                                      const char *function_name,
                                      const ray_bytes_t *args,
                                      size_t arg_count,
                                      const bool *is_ref,
                                      const char *options_json) {
    ray_clear_error();
    try {
        // 次序按参数次序：module 是第一个参数，它的两条检查先报。这两条检查都排在取
        // 运行时之前，被拒的调用不建运行时、不建 holder、不提交任务、不碰 SDK，也能在
        // 没有运行时的进程里测；它们还排在下面对本地模式的判定之前，所以本地模式 +
        // 空 module 名报名字错误，本地模式 + 注册过同名 Rust 函数的名字报的还是那条本地
        // 模式冲突错误。空 module 名永远跑不通：importlib 对空名字无条件抛 ValueError
        // （详见 ray_c.h），任务必然在 worker 里失败并且 worker 退出。function 名不在这里
        // 判定：空 function 名查到的是 module 对象本身，那个对象可以可调用，这种情况下
        // 任务真能跑，拒它就会拒掉能跑通的调用。只拦空指针（std::string(nullptr) 是未定义
        // 行为）。带内部 NUL 的名字经 to_cstring 变成空串
        // （crates/rayrust-sys/src/lib.rs:253-255），所以带 NUL 的 module 名归到空名字那条
        // 检查上，带 NUL 的 function 名就是一个空 function 名，照原样提交。
        if (!module_name) {
            set_last_error("module_name is null");
            return ray_bytes_t{nullptr, 0};
        }
        if (module_name[0] == '\0') {
            set_last_error(
                "the module name is empty: the worker imports the module by name "
                "(python/ray/_private/function_manager.py:138, importlib.import_module) "
                "and importlib raises ValueError: Empty module name for an empty "
                "string, so the task fails inside the worker and the worker exits with "
                "SYSTEM_ERROR; the caller's first read gets a worker error whose "
                "message Rust extracts from the recorded error payload (or renders "
                "lossily as UTF-8). This entry "
                "refuses the name before submission instead.");
            return ray_bytes_t{nullptr, 0};
        }
        if (!function_name) {
            set_last_error("function_name is null");
            return ray_bytes_t{nullptr, 0};
        }
        // 引用参数先在这里查一次（`build_task_args_python` 内部还会再查，幂等），让引用
        // 参数错误优先于关闭错误，与 `ray_c.h` 的判定次序一致。
        check_reference_arg_ids(args, arg_count, is_ref);
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            // 本地 submitter 丢掉 module 与语言，只按函数名建 C++ descriptor
            // （cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40），任务规格的
            // 语言也写成 CPP（同文件 :52-56）。进程里注册过同名 Rust 函数时，这个「Python
            // 任务」执行的是那个 Rust 回调。
            auto &fns = get_rust_functions();
            if (fns.find(std::string(function_name)) != fns.end()) {
                set_last_error(
                    ("task_call_python cannot submit \"" + std::string(module_name) + "." +
                     std::string(function_name) +
                     "\" in local mode: the local submitter drops the module and the "
                     "language and resolves the function name in the registered Rust "
                     "function map "
                     "(cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40), so the "
                     "Rust function registered under that name would run instead of the "
                     "Python function")
                        .c_str());
                return ray_bytes_t{nullptr, 0};
            }
        }

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), std::string(function_name),
            "", ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count, is_ref);

        // 与 ray_task_call_with_options 同一顺序：SDK 把 CallOptions 的 bundle_index
        // 留成未初始化，先置 -1（group 内任意 bundle）再解析 options。
        //
        // 调用方没写 CPU 时补 1.0，Python 的 `@ray.remote` 就是这个默认值
        // （来历与上游行号见 apply_default_task_cpu 的说明）；显式的 `"CPU":0` 经
        // emplace 保留 0，是退出这一默认的唯一方式。不收 options 的入口传 NULL，所以
        // 它提交的任务照样要 1 个 CPU。
        ray::internal::CallOptions options;
        options.bundle_index = -1;
        parse_call_options(options_json, options);
        apply_default_task_cpu(options);
        return submit_task_and_record(*runtime, holder, task_args, options);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

/// Call a Python remote function.
/// `module_name` and `function_name` are null-terminated C strings.
/// `args` are msgpack-serialized arguments (will be wrapped with xlang header).
/// 这个入口不收 options，提交的任务保持默认的 1 个 CPU；要 options 用
/// ray_task_call_python_with_options。
ray_bytes_t ray_task_call_python(const char *module_name,
                                  const char *function_name,
                                  const ray_bytes_t *args,
                                  size_t arg_count,
                                  const bool *is_ref) {
    return submit_python_task(module_name, function_name, args, arg_count, is_ref,
                              nullptr);
}

/// Call a Python remote function with full CallOptions.
/// `options_json` is a JSON object string; see ray_c.h for the field set.
ray_bytes_t ray_task_call_python_with_options(const char *module_name,
                                              const char *function_name,
                                              const ray_bytes_t *args,
                                              size_t arg_count,
                                              const bool *is_ref,
                                              const char *options_json) {
    return submit_python_task(module_name, function_name, args, arg_count, is_ref,
                              options_json);
}

// ─── Actor ────────────────────────────────────────────────────

/// Create an actor with full ActorCreationOptions via options JSON.
///
/// 函数名的前置检查放在取运行时之前：参数错误先于状态错误，与同文件 `ray_get_actor`、
/// `wait` 的参数检查一致，被拒的调用不建运行时、不碰 SDK，而且不需要运行时就能测。
/// 空函数名必须在这里拒掉，它在两种模式下都交出一个跑不起来的 id。本地模式只构造驱动
/// （`cpp/src/ray/runtime/local_mode_ray_runtime.cc:30-34`），创建任务在调用线程里跑完了
/// `CreateActor` 才返回（`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:115-121`、
/// `:143-151`），`TaskExecutionHandler` 在任何查找之前抛 `Task function name is empty`
/// （`cpp/src/ray/runtime/task/task_executor.cc:32-34`），`Invoke` 的 catch 把这段文本
/// 放进创建任务的返回值、跳过 actor context 登记（同文件 `:336-339`，`:327-331` 那条
/// 分支没有走到），第一次调用在 `actor_contexts_.at` 上抛裸的 `_Map_base::at`
/// （`local_mode_task_submitter.cc:110-112`）。集群模式在创建任务提交之后就返回，actor
/// worker 在自己的进程里解析这个名字：空名字在
/// `cpp/src/ray/util/function_helper.cc:163-168` 抛 `RayFunctionNotFound`，
/// `task_executor.cc:101-111` 把它转成 `Status::Invalid`，actor worker 停在
/// `src/ray/core_worker/core_worker_shutdown_executor.cc:40`，驱动第一次读值得到
/// `RAY_ERROR_KIND_ACTOR` 错误。
ray_bytes_t ray_actor_create_with_options(const char *func_name,
                                            const ray_bytes_t *args,
                                            size_t arg_count,
                                            const char *options_json) {
    ray_clear_error();
    if (!func_name) {
        set_last_error("func_name is null");
        return ray_bytes_t{nullptr, 0};
    }
    if (func_name[0] == '\0') {
        set_last_error(
            "the function name is empty: every name a #[rayrust::actor] type registers is "
            "non-empty (the constructor is __rayrust_actor_factory_<type>, a method is "
            "__rayrust_actor_factory_<type>::<method>), and an empty one can never run. "
            "Local mode runs the creation task in the calling thread before CreateActor "
            "returns (cpp/src/ray/runtime/task/local_mode_task_submitter.cc:115-121) and "
            "throws \"Task function name is empty\" before any lookup "
            "(cpp/src/ray/runtime/task/task_executor.cc:32-34); the error text goes into "
            "the creation task's return value and no actor context is registered (same "
            "file, :336-339, so the branch at :327-331 is skipped), and the first call "
            "throws a bare _Map_base::at from actor_contexts_.at "
            "(local_mode_task_submitter.cc:110-112). Cluster mode returns as soon as the "
            "creation task is submitted, and the worker fails it with RayFunctionNotFound "
            "(cpp/src/ray/util/function_helper.cc:163-168, caught at "
            "task_executor.cc:101-111): the actor worker stops at "
            "src/ray/core_worker/core_worker_shutdown_executor.cc:40 and the driver's "
            "first read fails with an actor error whose message Rust extracts from the "
            "recorded error payload (or renders lossily as UTF-8). A name with an interior NUL byte "
            "arrives here as an empty C string as well (#27). A whitespace-only name is "
            "not trimmed and is submitted as it is, as ray_get_actor does; a "
            "non-empty name nobody registered keeps Ray's asynchronous semantics, "
            "because the driver has no way to learn which names are registered.");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder("", std::string(func_name),
                                                   std::string(func_name),
                                                   ray::internal::LangType::CPP);

        auto task_args = build_task_args_cpp(runtime.get(), args, arg_count);

        // ActorCreationOptions::bundle_index 没有默认成员初始化器
        // （cpp/include/ray/api/task_options.h:111），GetBundleID
        // （cpp/src/ray/runtime/task/native_task_submitter.cc:57-65）在组非空时会读
        // 它。没有 options JSON 的路径也走这里，先置 -1（组内任意 bundle），
        // 不留未初始化的值。
        ray::internal::ActorCreationOptions options;
        options.bundle_index = -1;
        parse_actor_options(options_json, options);
        apply_default_actor_cpu(options);
        std::string id = runtime->CreateActor(holder, task_args, options);
        return dup_bytes(id);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

/// Create a Python actor with full ActorCreationOptions via options JSON.
///
/// 两个名字的前置检查放在取运行时之前：参数错误先于状态错误，与同文件的
/// `ray_actor_create_with_options`、`ray_get_actor`、`wait` 一致，被拒的调用不建运行时、
/// 不碰 SDK，而且不需要运行时就能测。集群模式下空 module 名与空 class 名都交出一个跑不
/// 起来的 actor（名字由 worker 的 Python 侧解析，见两条错误文本里的行号）；本地模式根本
/// 建不出 Python actor，这个入口在那里直接返回错误，所以那两条检查在本地模式只决定报哪
/// 一条错误。检查排在下面的本地模式拒绝之前，因此空名字在两种模式下报的都是名字错误。
ray_bytes_t ray_actor_create_python_with_options(const char *module_name,
                                                   const char *class_name,
                                                   const ray_bytes_t *args,
                                                   size_t arg_count,
                                                   const char *options_json) {
    ray_clear_error();
    if (!module_name) {
        set_last_error("module_name is null");
        return ray_bytes_t{nullptr, 0};
    }
    if (module_name[0] == '\0') {
        set_last_error(
            "the module name is empty: a Python actor is named by its module and its "
            "class, neither of which may be empty. The driver only submits the "
            "creation task, and the worker resolves the name in Python: "
            "python/ray/_private/function_manager.py:531 (load_actor_class) calls "
            ":589 (_load_actor_class_from_local), which calls :138 "
            "(load_function_or_class_from_local), where "
            "importlib.import_module(module_name) raises \"ValueError: Empty module "
            "name\"; the worker exits with SYSTEM_ERROR and the 16-byte id the caller "
            "already got fails on its first read with an actor error whose message "
            "Rust extracts from the recorded error payload (or renders lossily as "
            "UTF-8). Either empty name is refused here. Local mode refuses "
            "every Python actor creation, and these checks run before that refusal, "
            "so an empty name reports this error in both modes. A name with an "
            "interior NUL byte arrives here as an empty C "
            "string as well (crates/rayrust-sys/src/lib.rs:253-255, #27).");
        return ray_bytes_t{nullptr, 0};
    }
    if (!class_name) {
        set_last_error("class_name is null");
        return ray_bytes_t{nullptr, 0};
    }
    if (class_name[0] == '\0') {
        set_last_error(
            "the class name is empty: a Python actor is named by its module and its "
            "class, neither of which may be empty. With a module that imports, the "
            "empty class name reaches python/ray/_raylet.pyx:2253, which raises "
            "\"TypeError: module.__new__(X): X is not a type object (module)\" before "
            "load_actor_class is reached (the measured traceback holds only "
            "_raylet.pyx:2432 and :2253); the worker exits with SYSTEM_ERROR and the "
            "16-byte id the caller already got fails on its first read with an actor "
            "error whose message Rust extracts from the recorded error payload (or "
            "renders lossily as UTF-8). Either empty name is refused "
            "here. Local mode refuses every Python actor creation, and these checks "
            "run before that refusal, so an empty name reports this error in both "
            "modes. A name with an interior NUL byte "
            "arrives here as an empty C string as well "
            "(crates/rayrust-sys/src/lib.rs:253-255, #27).");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            // LocalModeTaskSubmitter::Submit 忽略语言与 module，只用函数名建 C++
            // descriptor（cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40），
            // 任务规格的语言也写成 CPP（同文件 :52-56）。这个入口放进 holder 的函数名是
            // "__init__"，TaskExecutor 拿它在 C++ FunctionManager 里查，查不到就把
            // PackError("unknown function: __init__") 的结果当成 actor 状态存起来
            // （cpp/src/ray/runtime/task/task_executor.cc:44-52、:313-331），既不抛异常也不
            // 返回错误，调用方只看到 Ok。进程里恰好注册了同名函数时被执行的是那个函数，
            // Python 类的构造函数仍然没有跑过。本地模式也没有 Python 执行器，且带名字创建
            // 会把名字写进本地名字表（local_mode_task_submitter.cc:143-152），
            // ray_get_actor 从同一张表里读回来（同文件 :159-168）交出一个谁都调不到的
            // handle。因此这个入口在本地模式直接返回错误，不进入 CreateActor。
            set_last_error(
                "Python actors cannot be created in local mode: local mode never starts "
                "a core worker process and has no Python executor, and its task "
                "submitter builds every task as a C++ descriptor from the function "
                "name, so the constructor never runs");
            return ray_bytes_t{nullptr, 0};
        }

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), "__init__",
            std::string(class_name), ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count);

        // 与 ray_actor_create_with_options 同一理由：先给 bundle_index 一个确定值。
        ray::internal::ActorCreationOptions options;
        options.bundle_index = -1;
        parse_actor_options(options_json, options);
        apply_default_actor_cpu(options);
        std::string id = runtime->CreateActor(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

ray_bytes_t ray_actor_call(const char *actor_id_data, size_t actor_id_len,
                            const char *func_name,
                            const ray_bytes_t *args,
                            size_t arg_count,
                            const bool *is_ref = nullptr) {
    ray_clear_error();
    try {
        if (check_actor_id(actor_id_data, actor_id_len) != 0)
            return ray_bytes_t{nullptr, 0};
        if (!func_name) {
            set_last_error("func_name is null");
            return ray_bytes_t{nullptr, 0};
        }

        // For CPP actor calls, RemoteFunctionHolder should use single-argument
        // constructor (only function_name_, class_name_ stays empty).
        // This matches ActorHandle::Task() behavior in the SDK.
        // The function_name_ must match the registered member function name
        // (e.g. "__rayrust_actor_factory_counter::increment").
        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder(fn);
        // 引用参数先在这里查一次（`build_task_args_cpp` 内部还会再查，幂等），让引用参数
        // 错误优先于关闭错误，与 `ray_c.h` 的判定次序一致。
        check_reference_arg_ids(args, arg_count, is_ref);

        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        auto task_args = build_task_args_cpp(runtime.get(), args, arg_count, is_ref);

        std::string actor_id(actor_id_data, actor_id_len);
        ray::internal::CallOptions options;
        std::string id = runtime->CallActor(holder, actor_id, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

/// Call a method on a Python actor.
/// `method_name` is the Python method name (without `self`).
ray_bytes_t ray_actor_call_python(const char *actor_id_data, size_t actor_id_len,
                                    const char *method_name,
                                    const ray_bytes_t *args,
                                    size_t arg_count,
                                    const bool *is_ref) {
    ray_clear_error();
    try {
        // 次序按参数次序：actor id 是第一个参数，宽度检查先报；id 合法之后才轮到方法
        // 名。两条方法名检查都排在取运行时之前，被拒的调用不建运行时、不碰 SDK，也能
        // 在没有运行时的进程里测；它们还排在下面的本地模式拒绝之前，所以本地模式 +
        // 空方法名报名字错误，本地模式 + 合法方法名报的还是那条本地模式错误。
        if (check_actor_id(actor_id_data, actor_id_len) != 0)
            return ray_bytes_t{nullptr, 0};
        if (!method_name) {
            set_last_error("method_name is null");
            return ray_bytes_t{nullptr, 0};
        }
        if (method_name[0] == '\0') {
            set_last_error(
                "the method name is empty: this entry refuses it, although an empty "
                "name is not always fatal. A Python instance can carry an attribute "
                "named with the empty string (setattr(Class, \"\", method) puts one "
                "into the class namespace), and a cross-language call with that name "
                "reaches it: measured on Ray 2.51.1, that call read 42 and the actor "
                "kept answering. The driver cannot tell such an actor apart from one "
                "without that attribute, and there the call ends the actor's worker: "
                "it exits with SYSTEM_ERROR (\"Worker exits unexpectedly\"), the "
                "traceback holds python/ray/_raylet.pyx:2432 (task_execution_handler "
                "calls execute_task_with_cancellation_handler), :2291 (which calls "
                "execute_task) and :1873, where execute_task runs "
                "function_name = execution_info.function_name and raises "
                "\"AttributeError: 'dict' object has no attribute "
                "'function_name'\"; the caller's first read then fails with an actor "
                "error whose message Rust extracts from the recorded error payload (or "
                "renders lossily as UTF-8) and that actor cannot be "
                "called again. The C++ entry ray_actor_call refuses an empty name in "
                "the driver as well: its single-argument RemoteFunctionHolder "
                "constructor throws \"Function not found. Please use RAY_REMOTE to "
                "register this function.\" "
                "(cpp/include/ray/api/ray_runtime.h:41-45) before any task is "
                "submitted, so its actor keeps running. A name with an interior NUL "
                "byte arrives here as an empty C string as well "
                "(crates/rayrust-sys/src/lib.rs:253-255, #27). A non-empty method "
                "name the actor does not have is not refused here: the driver holds "
                "no method table and cannot tell a valid name from an invalid one, "
                "and measured on 2.51.1 such a name also ends the actor's worker with "
                "the same traceback, so callers must pass a method name that exists.");
            return ray_bytes_t{nullptr, 0};
        }
        // 引用参数先在这里查一次（`build_task_args_python` 内部还会再查，幂等），让引用
        // 参数错误优先于关闭错误，与 `ray_c.h` 的判定次序一致。
        check_reference_arg_ids(args, arg_count, is_ref);
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            // AbstractRayRuntime::CallActor 的 PYTHON 分支在构造调用之前读取 actor handle
            // 上的 PythonFunctionDescriptor，把其中记录的 module 与 class 写入
            // ACTOR_TASK 的 RemoteFunctionHolder
            // （cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211）。这份登记表只存在
            // 于 core worker 内，而读取它会调用 CoreWorkerProcess::GetCoreWorker()；本地模式
            // 从不启动 core worker 进程，那里的 EnsureInitialized 会执行 QuickExit()，最终
            // 调用 _Exit(1)（src/ray/core_worker/core_worker_process.cc:842-850、
            // src/ray/util/process.cc:845-848），RAY_CATCH 无法捕获 _Exit。即便外部传入
            // module 与 class 也走不通：LocalModeTaskSubmitter::Submit 固定用
            // CppFunctionDescriptor 构造每个任务（local_mode_task_submitter.cc:39-55），
            // 本地模式没有任何能执行 Python actor 方法的执行器，因此这个入口在本地模式
            // 直接返回错误，不进入 CallActor。
            set_last_error(
                "Python actors cannot be called in local mode: the SDK reads the actor's "
                "module and class from the core worker's actor handle, and local mode "
                "never starts a core worker process");
            return ray_bytes_t{nullptr, 0};
        }

        ray::internal::RemoteFunctionHolder holder(
            "", std::string(method_name), "",
            ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count, is_ref);

        std::string actor_id(actor_id_data, actor_id_len);
        ray::internal::CallOptions options;
        std::string id = runtime->CallActor(holder, actor_id, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

void ray_actor_kill(const char *actor_id_data, size_t actor_id_len, bool no_restart) {
    ray_clear_error();
    try {
        // 宽度检查排在本地模式分支之前，与 ray_cancel 的次序一致：本地模式 + 3 字节
        // actor id 报的是宽度错误。
        if (check_actor_id(actor_id_data, actor_id_len) != 0) return;
        auto runtime = runtime_for_call();
        if (!runtime) return;
        if (runtime->IsLocalMode()) {
            // AbstractRayRuntime::KillActor 的第一句是
            // CoreWorkerProcess::GetCoreWorker()
            // （cpp/src/ray/runtime/abstract_ray_runtime.cc:265-266），本地模式从不
            // 启动 core worker 进程，那里的 EnsureInitialized 走 QuickExit()，最终
            // _Exit(1)（src/ray/core_worker/core_worker_process.cc:842-856、
            // src/ray/util/process.cc:845-848），RAY_CATCH 抓不到 _Exit。
            // LocalModeRayRuntime 没有覆盖 KillActor
            // （cpp/src/ray/runtime/local_mode_ray_runtime.h:24-36），本地 actor 活在
            // 本进程的 LocalModeTaskSubmitter::actor_contexts_ 里
            // （cpp/src/ray/runtime/task/local_mode_task_submitter.h:53），随进程结束。
            set_last_error(
                "actors cannot be killed in local mode: local mode never starts a "
                "core worker process, and a local actor lives in the calling process "
                "and ends with it");
            return;
        }
        std::string actor_id(actor_id_data, actor_id_len);
        runtime->KillActor(actor_id, no_restart);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
    }
}

ray_bytes_t ray_actor_handle_serialize(const char *actor_id_data, size_t actor_id_len) {
    ray_clear_error();
    if (check_actor_id(actor_id_data, actor_id_len) != 0) return ray_bytes_t{nullptr, 0};
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            // AbstractRayRuntime::SerializeActorHandle 的第一句就是
            // CoreWorkerProcess::GetCoreWorker()，本地模式从不启动 core worker 进程，
            // 那里直接 QuickExit()（_Exit(1)，src/ray/util/process.cc）。
            set_last_error(
                "actor handles cannot be serialized in local mode: local mode never "
                "starts a core worker process");
            return ray_bytes_t{nullptr, 0};
        }
        std::string serialized =
            runtime->SerializeActorHandle(std::string(actor_id_data, actor_id_len));
        if (serialized.empty()) {
            set_last_error("the core worker returned no serialized actor handle");
            return ray_bytes_t{nullptr, 0};
        }
        return dup_bytes(serialized);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

// ─── rpc::ActorHandle 载荷校验 ──────────────────────────────────
//
// `ray_actor_handle_deserialize` 收下的字节是一段 `rpc::ActorHandle` protobuf
// （src/ray/protobuf/core_worker.proto:27-79）。wheel 只带 `ray/api/*` 头文件，没有
// `core_worker.pb.h`（#19），所以这里按 protobuf 的 wire format 自己走一遍：已知字段
// 按 schema 校验内容（嵌套消息递归进去，proto3 的 `string` 校验 UTF-8），未知字段与
// wire type 与 schema 不符的字段按 wire type 跳过。
//
// 两个字段在上游都按固定尺寸读成 id，长度不对就 RAY_CHECK 终止进程：
// - 字段 1 `actor_id`：登记阶段 `ActorManager::RegisterActorHandle`
//   （src/ray/core_worker/actor_manager.cc:33）调 `ActorHandle::GetActorID`
//   （src/ray/core_worker/actor_handle.h:64）→ `ActorID::FromBinary`，要求 16 字节
//   （src/ray/common/id.h:124-142、:430-441）。
// - 字段 4 `creation_job_id`：第一次 `actor_call` 走 `CoreWorker::SubmitActorTask`
//   （src/ray/core_worker/core_worker.cc:2374）→ `ActorHandle::CreationJobID`
//   （actor_handle.h:72）→ `JobID::FromBinary`，要求 4 字节（src/ray/common/id.h:105）。
//   字段缺省时 `FromBinary("")` 给出 nil job id：任务带着 nil job id 发到 actor，
//   `WorkerContext::SetCurrentTask` 的 `RAY_CHECK(current_job_id_ == task_spec.JobId())`
//   （src/ray/core_worker/context.cc:295）终止 actor 的 worker 进程，调用方收到
//   `ActorError`（实测：一个进程建 detached actor 并写出载荷，另一个进程删掉字段 4 后
//   登记并调用，actor worker 在 `:295` 终止，调用立即失败）。actor id 不属于任何
//   actor 时则是另一种后果：订阅永远收不到 ALIVE，调用留在队列里到超时。
//
// 上游把 `ParseFromString` 的返回值丢弃了（src/ray/core_worker/actor_handle.cc:66），
// 被中止的解析还把已经写入的字段留在 `inner` 里，长度问题只能在进 SDK 之前发现。
// 所以只量顶层的长度不够：先放一个短的 `actor_id`，再用一段损坏的嵌套消息或非法
// UTF-8 的 `string` 让上游的解析在那里中止，登记的 `actor_id` 仍然是那个短值，登记
// 阶段照样在 `id.h:435` 终止进程；排在损坏字段之后的 `creation_job_id` 同样读不到，
// 登记出 nil job id 的 handle。这两类载荷由下面的递归校验拒掉。

/// `rpc::ActorHandle` 的字段号：`actor_id = 1`、`creation_job_id = 4`、
/// `actor_language = 5`、`actor_creation_task_function_descriptor = 6`
/// （src/ray/protobuf/core_worker.proto:29、:39、:42、:45）。
static constexpr uint64_t kActorHandleActorIdField = 1;
static constexpr uint64_t kActorHandleCreationJobIdField = 4;
static constexpr uint64_t kActorHandleActorLanguageField = 5;
static constexpr uint64_t kActorHandleCreationDescriptorField = 6;

/// `actor_language` 的取值来自 `ray::Language`（src/ray/protobuf/common.proto:26-30）。
/// proto3 省略等于默认值 0 的字段，所以载荷里没有字段 5 时读出来的也是 Python。
static constexpr int kLanguagePython = 0;
static constexpr int kLanguageJava = 1;
static constexpr int kLanguageCpp = 2;

/// 创建描述符的类型就是 `rpc.FunctionDescriptor` 那个 oneof 成员的字段号：java 1、
/// python 2、cpp 3，0 表示字段 6 缺席（`src/ray/protobuf/common.proto:158-164`）。
/// 本地模式建出来的就是 Cpp 描述符
/// （`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40`）。
static constexpr int kCreationDescriptorCpp = 3;

/// protobuf 的长度前缀是 int32（上游 `ReadVarintSizeAsInt` 的界限）。
static constexpr uint64_t kPayloadMaxLengthVarint = 0x7fffffff;

/// 允许的最深嵌套层数。真实载荷最多三层（`rpc.ActorHandle` →
/// `actor_creation_task_function_descriptor` → `python_function_descriptor`），更深
/// 的消息都是构造出来的。每层至少占两个字节，总长度因此仍受输入长度约束。
static constexpr size_t kPayloadMaxNestingDepth = 8;

/// varint 的读取结果。
enum class VarintResult { kOk, kTruncated, kTooLong, kNotMinimal, kAboveLimit };

/// 字段内容的校验方式。
enum class PayloadFieldKind {
    /// 定长 id 或任意字节：只看长度，内容交给上游。
    kBytes,
    /// proto3 的 `string`：内容必须是合法 UTF-8，上游的解析器同样校验。
    kString,
    /// 嵌套消息：内容按子 schema 递归校验。
    kMessage,
    /// varint 编码的标量：只看 varint 是否完整，取值不校验（proto3 不校验枚举值）。
    kVarint,
};

struct PayloadSchema;

/// schema 的一个字段：`number` 是 protobuf 字段号，`kind` 决定内容怎么校验，
/// `message` 指向嵌套消息的 schema（只有 `kMessage` 用它）。
struct PayloadField {
    uint64_t number;
    const char *name;
    PayloadFieldKind kind;
    const PayloadSchema *message;
};

/// 一条消息的字段表。wire type 由 `kind` 决定：`kVarint` 是 0，其余三种是 2。
struct PayloadSchema {
    const PayloadField *fields;
    size_t field_count;
};

/// 从字段数组造 schema，字段数由数组长度推出。
template <size_t N>
static constexpr PayloadSchema payload_schema(const PayloadField (&fields)[N]) {
    return PayloadSchema{fields, N};
}

// 字段表逐条抄自 Ray 2.51.1 的 proto：`rpc.ActorHandle` 在
// src/ray/protobuf/core_worker.proto:27-79，`rpc.Address` 与三个函数描述符在
// src/ray/protobuf/common.proto:126-164。

static const PayloadField kJavaFunctionDescriptorFields[] = {
    {1, "class_name", PayloadFieldKind::kString, nullptr},
    {2, "function_name", PayloadFieldKind::kString, nullptr},
    {3, "signature", PayloadFieldKind::kString, nullptr},
};
static const PayloadSchema kJavaFunctionDescriptorSchema =
    payload_schema(kJavaFunctionDescriptorFields);

static const PayloadField kPythonFunctionDescriptorFields[] = {
    {1, "module_name", PayloadFieldKind::kString, nullptr},
    {2, "class_name", PayloadFieldKind::kString, nullptr},
    {3, "function_name", PayloadFieldKind::kString, nullptr},
    {4, "function_hash", PayloadFieldKind::kString, nullptr},
};
static const PayloadSchema kPythonFunctionDescriptorSchema =
    payload_schema(kPythonFunctionDescriptorFields);

static const PayloadField kCppFunctionDescriptorFields[] = {
    {1, "function_name", PayloadFieldKind::kString, nullptr},
    {2, "caller", PayloadFieldKind::kString, nullptr},
    {3, "class_name", PayloadFieldKind::kString, nullptr},
};
static const PayloadSchema kCppFunctionDescriptorSchema =
    payload_schema(kCppFunctionDescriptorFields);

static const PayloadField kFunctionDescriptorFields[] = {
    {1, "java_function_descriptor", PayloadFieldKind::kMessage,
     &kJavaFunctionDescriptorSchema},
    {2, "python_function_descriptor", PayloadFieldKind::kMessage,
     &kPythonFunctionDescriptorSchema},
    {3, "cpp_function_descriptor", PayloadFieldKind::kMessage,
     &kCppFunctionDescriptorSchema},
};
static const PayloadSchema kFunctionDescriptorSchema =
    payload_schema(kFunctionDescriptorFields);

static const PayloadField kAddressFields[] = {
    {1, "node_id", PayloadFieldKind::kBytes, nullptr},
    {2, "ip_address", PayloadFieldKind::kString, nullptr},
    {3, "port", PayloadFieldKind::kVarint, nullptr},
    {4, "worker_id", PayloadFieldKind::kBytes, nullptr},
};
static const PayloadSchema kAddressSchema = payload_schema(kAddressFields);

/// `map<string, string> labels`（core_worker.proto:73-75）的每个 entry 都是一条
/// 这样的子消息。
static const PayloadField kActorHandleLabelsEntryFields[] = {
    {1, "key", PayloadFieldKind::kString, nullptr},
    {2, "value", PayloadFieldKind::kString, nullptr},
};
static const PayloadSchema kActorHandleLabelsEntrySchema =
    payload_schema(kActorHandleLabelsEntryFields);

static const PayloadField kActorHandleFields[] = {
    {1, "actor_id", PayloadFieldKind::kBytes, nullptr},
    {2, "owner_id", PayloadFieldKind::kBytes, nullptr},
    {3, "owner_address", PayloadFieldKind::kMessage, &kAddressSchema},
    {4, "creation_job_id", PayloadFieldKind::kBytes, nullptr},
    {5, "actor_language", PayloadFieldKind::kVarint, nullptr},
    {6, "actor_creation_task_function_descriptor", PayloadFieldKind::kMessage,
     &kFunctionDescriptorSchema},
    {7, "actor_cursor", PayloadFieldKind::kBytes, nullptr},
    {8, "extension_data", PayloadFieldKind::kBytes, nullptr},
    {9, "max_task_retries", PayloadFieldKind::kVarint, nullptr},
    {10, "name", PayloadFieldKind::kString, nullptr},
    {11, "ray_namespace", PayloadFieldKind::kString, nullptr},
    {12, "allow_out_of_order_execution", PayloadFieldKind::kVarint, nullptr},
    {13, "max_pending_calls", PayloadFieldKind::kVarint, nullptr},
    {14, "enable_task_events", PayloadFieldKind::kVarint, nullptr},
    {15, "labels", PayloadFieldKind::kMessage, &kActorHandleLabelsEntrySchema},
    {16, "enable_tensor_transport", PayloadFieldKind::kVarint, nullptr},
    {17, "is_detached", PayloadFieldKind::kVarint, nullptr},
};
static const PayloadSchema kActorHandlePayloadSchema = payload_schema(kActorHandleFields);

/// 顶层定长 id 字段、语言字段与创建描述符类型的最后一次出现（protobuf 合并同一
/// 字段时取最后一次；字段 6 里的 oneof 同理）。
struct ActorHandlePayloadFields {
    bool has_actor_id = false;
    size_t actor_id_len = 0;
    bool has_creation_job_id = false;
    size_t creation_job_id_len = 0;
    /// 字段 5 缺席时是 proto3 的默认值 `PYTHON = 0`，与上游解析出来的语言一致。
    int actor_language = kLanguagePython;
    /// 字段 6 里那个 oneof 成员的字段号，0 表示字段 6 缺席。
    int creation_descriptor = 0;
};

/// 从 `payload[pos]` 起读一个 protobuf varint，`end` 是本条消息的末尾。`limit` 是
/// 允许的最大值，`require_minimal` 要求最短编码。失败时 `pos` 已经前移，调用方
/// 只用来报错。
static VarintResult read_varint(const std::string &payload, size_t end, size_t &pos,
                               uint64_t limit, bool require_minimal, uint64_t &value) {
    size_t start = pos;
    uint64_t result = 0;
    size_t bytes = 0;
    bool complete = false;
    for (int shift = 0; shift <= 63; shift += 7) {
        if (pos >= end) return VarintResult::kTruncated;
        unsigned char byte = static_cast<unsigned char>(payload[pos]);
        ++pos;
        ++bytes;
        result |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) {
            complete = true;
            break;
        }
    }
    if (!complete) return VarintResult::kTooLong;
    if (require_minimal) {
        size_t minimal = 1;
        for (uint64_t rest = result >> 7; rest != 0; rest >>= 7) ++minimal;
        if (bytes != minimal) {
            pos = start;
            return VarintResult::kNotMinimal;
        }
    }
    if (result > limit) {
        pos = start;
        return VarintResult::kAboveLimit;
    }
    value = result;
    return VarintResult::kOk;
}

/// `read_varint` 失败的共同说法。
static std::string varint_failure(VarintResult result, const std::string &subject,
                                  size_t offset) {
    const std::string at = subject + " at offset " + std::to_string(offset);
    switch (result) {
        case VarintResult::kTruncated:
            return at + " is truncated";
        case VarintResult::kTooLong:
            return at + " is longer than 10 bytes";
        case VarintResult::kNotMinimal:
            return at + " is not minimally encoded";
        case VarintResult::kAboveLimit:
            return at + " does not fit in 32 bits";
        case VarintResult::kOk:
            break;
    }
    return at + " is malformed";
}

/// proto3 的 `string` 内容校验：拒绝过长编码、代理区间（U+D800-U+DFFF）与超过
/// U+10FFFF 的码位，与上游 `ParseFromString` 的校验一致。
static bool is_valid_utf8(const char *data, size_t len) {
    size_t i = 0;
    while (i < len) {
        unsigned char lead = static_cast<unsigned char>(data[i]);
        if (lead < 0x80) {
            ++i;
            continue;
        }
        size_t extra = 0;
        uint32_t code_point = 0;
        if ((lead & 0xe0) == 0xc0) {
            extra = 1;
            code_point = lead & 0x1f;
        } else if ((lead & 0xf0) == 0xe0) {
            extra = 2;
            code_point = lead & 0x0f;
        } else if ((lead & 0xf8) == 0xf0) {
            extra = 3;
            code_point = lead & 0x07;
        } else {
            return false;
        }
        if (len - i - 1 < extra) return false;
        for (size_t k = 1; k <= extra; ++k) {
            unsigned char next = static_cast<unsigned char>(data[i + k]);
            if ((next & 0xc0) != 0x80) return false;
            code_point = (code_point << 6) | (next & 0x3f);
        }
        if (extra == 1 && code_point < 0x80) return false;
        if (extra == 2 && code_point < 0x800) return false;
        if (extra == 3 && code_point < 0x10000) return false;
        if (code_point > 0x10ffff) return false;
        if (code_point >= 0xd800 && code_point <= 0xdfff) return false;
        i += extra + 1;
    }
    return true;
}

/// 载荷校验的错误说法：统一的前缀，`where` 指出嵌套在哪条子消息里（顶层为空串）。
static void set_payload_error(const std::string &where, const std::string &detail) {
    set_last_error(("the serialized actor handle is not a well-formed rpc.ActorHandle "
                    "protobuf: " +
                    where + detail)
                       .c_str());
}

/// 按 `schema` 校验 `payload[pos, end)` 里的一条消息，嵌套消息递归进来。
/// `path` 是出错时指路的字段路径，顶层为空串；`fields` 只在顶层非空，用来记两个 id
/// 字段、语言字段与创建描述符类型的最后一次出现。`oneof_member` 只在顶层字段 6 的
/// 递归里非空：那条消息的 oneof 成员字段号就是创建描述符的类型。
static int check_payload_message(const std::string &payload, size_t &pos, size_t end,
                                 const PayloadSchema &schema, const std::string &path,
                                 size_t depth, ActorHandlePayloadFields *fields,
                                 int *oneof_member) {
    if (depth > kPayloadMaxNestingDepth) {
        set_payload_error(std::string(), path + " nests deeper than " +
                                              std::to_string(kPayloadMaxNestingDepth) +
                                              " levels");
        return -1;
    }
    const std::string where = path.empty() ? std::string() : path + ": ";
    while (pos < end) {
        size_t key_offset = pos;
        uint64_t key = 0;
        VarintResult key_result = read_varint(payload, end, pos, UINT32_MAX, true, key);
        if (key_result != VarintResult::kOk) {
            set_payload_error(where, varint_failure(key_result, "the field key", key_offset));
            return -1;
        }
        uint64_t field_number = key >> 3;
        uint64_t wire_type = key & 0x7;
        if (field_number == 0) {
            set_payload_error(where,
                              "field number 0 at offset " + std::to_string(key_offset));
            return -1;
        }
        const PayloadField *field = nullptr;
        for (size_t i = 0; i < schema.field_count; ++i) {
            if (schema.fields[i].number == field_number) {
                field = &schema.fields[i];
                break;
            }
        }
        // wire type 与 schema 不符的字段，protobuf 当作未知字段跳过，这里同样跳过。
        if (field != nullptr) {
            uint64_t schema_wire_type = field->kind == PayloadFieldKind::kVarint ? 0 : 2;
            if (wire_type != schema_wire_type) field = nullptr;
        }
        if (wire_type == 0) {
            uint64_t value = 0;
            size_t value_offset = pos;
            VarintResult value_result =
                read_varint(payload, end, pos, UINT64_MAX, false, value);
            if (value_result != VarintResult::kOk) {
                set_payload_error(where, varint_failure(value_result,
                    "the varint value of field " + std::to_string(field_number),
                    value_offset));
                return -1;
            }
            if (fields != nullptr && field_number == kActorHandleActorLanguageField) {
                // proto3 的 enum 字段是 int32：上游把取值读成 `Language` 的整数形式，
                // 未知取值照样保留。这里不拒绝未知码，由调用方按实际取值报错。
                fields->actor_language = static_cast<int>(value);
            }
            continue;
        }
        if (wire_type == 1) {
            if (end - pos < 8) {
                set_payload_error(where, "the 8-byte value of field " +
                    std::to_string(field_number) + " at offset " + std::to_string(pos) +
                    " is truncated, " + std::to_string(end - pos) + " bytes remain");
                return -1;
            }
            pos += 8;
            continue;
        }
        if (wire_type == 5) {
            if (end - pos < 4) {
                set_payload_error(where, "the 4-byte value of field " +
                    std::to_string(field_number) + " at offset " + std::to_string(pos) +
                    " is truncated, " + std::to_string(end - pos) + " bytes remain");
                return -1;
            }
            pos += 4;
            continue;
        }
        if (wire_type != 2) {
            set_payload_error(where, "wire type " + std::to_string(wire_type) +
                " of field " + std::to_string(field_number) + " at offset " +
                std::to_string(key_offset) + " is not a protobuf wire type");
            return -1;
        }
        size_t length_offset = pos;
        uint64_t length = 0;
        VarintResult length_result =
            read_varint(payload, end, pos, kPayloadMaxLengthVarint, true, length);
        if (length_result != VarintResult::kOk) {
            set_payload_error(where, varint_failure(length_result,
                "the length prefix of field " + std::to_string(field_number),
                length_offset));
            return -1;
        }
        if (length > end - pos) {
            set_payload_error(where, "the length prefix of field " +
                std::to_string(field_number) + " at offset " +
                std::to_string(length_offset) + " says " + std::to_string(length) +
                " bytes, only " + std::to_string(end - pos) + " remain");
            return -1;
        }
        size_t content_end = pos + static_cast<size_t>(length);
        if (field != nullptr) {
            const std::string label =
                path.empty() ? std::string(field->name) : path + "." + field->name;
            if (field->kind == PayloadFieldKind::kString) {
                if (!is_valid_utf8(payload.data() + pos, static_cast<size_t>(length))) {
                    set_payload_error(std::string(), label + " is not valid UTF-8");
                    return -1;
                }
            } else if (field->kind == PayloadFieldKind::kMessage) {
                int *nested_oneof = nullptr;
                if (fields != nullptr &&
                    field_number == kActorHandleCreationDescriptorField) {
                    nested_oneof = &fields->creation_descriptor;
                }
                if (oneof_member != nullptr) {
                    *oneof_member = static_cast<int>(field_number);
                }
                if (check_payload_message(payload, pos, content_end, *field->message, label,
                                          depth + 1, nullptr, nested_oneof) != 0) {
                    return -1;
                }
            } else if (fields != nullptr) {
                if (field_number == kActorHandleActorIdField) {
                    fields->has_actor_id = true;
                    fields->actor_id_len = static_cast<size_t>(length);
                } else if (field_number == kActorHandleCreationJobIdField) {
                    fields->has_creation_job_id = true;
                    fields->creation_job_id_len = static_cast<size_t>(length);
                }
            }
        }
        pos = content_end;
    }
    return 0;
}

/// 校验载荷是结构良好的 `rpc::ActorHandle`，并且 `actor_id` 正好 16 字节、
/// `creation_job_id` 正好 4 字节，把顶层的 id 长度、语言字段与创建描述符类型从
/// `fields` 交出去。
/// 失败返回 -1 并设置错误：载荷畸形、缺字段、字段长度不对三种情形各有一句自己的
/// 说法，`fields` 不被写入。
///
/// 字段 key 与长度前缀要求最短编码且能放进 32 位：上游把 tag 读成 `uint32`，
/// 5 字节但超过 32 位的 key 会被截断成别的字段（实测 `0x10000000a` 被当成
/// `actor_id`，短内容直接终止进程），长度前缀超过 32 位时上游的 `ParseFromString`
/// 失败。这两类编码不会出现在 protobuf 自己的序列化结果里，直接当载荷畸形拒掉。
static int check_actor_handle_payload(const char *data, size_t len,
                                      ActorHandlePayloadFields *fields) {
    const std::string payload(data, len);
    ActorHandlePayloadFields parsed;
    size_t pos = 0;
    if (check_payload_message(payload, pos, payload.size(), kActorHandlePayloadSchema,
                              std::string(), 0, &parsed, nullptr) != 0) {
        return -1;
    }
    if (!parsed.has_actor_id) {
        set_last_error(
            "the serialized actor handle has no actor_id field (protobuf field 1 of "
            "rpc.ActorHandle): a handle without it registers as the nil actor");
        return -1;
    }
    if (parsed.actor_id_len != kActorIdLength) {
        set_last_error(("the actor_id field of the serialized actor handle is " +
                        std::to_string(parsed.actor_id_len) + " bytes, must be " +
                        std::to_string(kActorIdLength) +
                        " bytes (the Ray 2.51.1 ActorID length)")
                           .c_str());
        return -1;
    }
    if (!parsed.has_creation_job_id) {
        set_last_error(
            "the serialized actor handle has no creation_job_id field (protobuf field 4 "
            "of rpc.ActorHandle): the nil job id it decodes to ends the actor's worker at "
            "the job id check in WorkerContext::SetCurrentTask "
            "(src/ray/core_worker/context.cc:295) on the first call");
        return -1;
    }
    if (parsed.creation_job_id_len != kJobIdLength) {
        set_last_error(("the creation_job_id field of the serialized actor handle is " +
                        std::to_string(parsed.creation_job_id_len) + " bytes, must be " +
                        std::to_string(kJobIdLength) +
                        " bytes (the Ray 2.51.1 JobID length)")
                           .c_str());
        return -1;
    }
    *fields = parsed;
    return 0;
}

/// 从序列化字节里复原并登记一个 actor handle，同时交出它的语言与创建描述符类型。
///
/// `out_meta` 必须是调用方提供的非空指针：载荷校验通过、handle 真正登记成功之后写入
/// 语言与创建描述符类型。语言是 `rpc.ActorHandle.actor_language`
/// （`src/ray/protobuf/core_worker.proto:42`，字段 5），取值 `PYTHON = 0`、`JAVA = 1`、
/// `CPP = 2`（`src/ray/protobuf/common.proto:26-30`）；字段 5 缺席时写
/// `kLanguagePython`：proto3 省略等于默认值的字段，上游解析出来的也是 `PYTHON`。
/// 创建描述符类型是字段 6 里那个 oneof 成员的字段号（字段 6 缺席时是 0）。
ray_bytes_t ray_actor_handle_deserialize(const char *serialized_data,
                                         size_t serialized_len,
                                         ray_actor_handle_meta_t *out_meta) {
    ray_clear_error();
    if (!out_meta) {
        set_last_error("out_meta is null");
        return ray_bytes_t{nullptr, 0};
    }
    if (!serialized_data || serialized_len == 0) {
        set_last_error("the serialized actor handle is empty");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        ActorHandlePayloadFields fields;
        if (check_actor_handle_payload(serialized_data, serialized_len, &fields) != 0) {
            return ray_bytes_t{nullptr, 0};
        }
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            // 本地模式从不创建 core worker 进程：DeserializeAndRegisterActorHandle 走
            // CoreWorkerProcess::GetCoreWorker()，那里的 EnsureInitialized 直接
            // QuickExit()，也就是 _Exit(1)（src/ray/util/process.cc），Rust 侧没有
            // 任何捕获机会。
            set_last_error(
                "actor handles cannot be deserialized in local mode: local mode never "
                "starts a core worker process");
            return ray_bytes_t{nullptr, 0};
        }
        std::string actor_id = runtime->DeserializeAndRegisterActorHandle(
            std::string(serialized_data, serialized_len));
        if (actor_id.size() != kActorIdLength) {
            set_last_error(
                ("the core worker returned a " + std::to_string(actor_id.size()) +
                 " byte actor id instead of " + std::to_string(kActorIdLength) + " bytes")
                    .c_str());
            return ray_bytes_t{nullptr, 0};
        }
        // 载荷校验只保证长度：`actor_id` 的 16 个字节原样登记，内容为空时上游给出
        // `ActorID::Nil()`（全 0xff），全 0x00 则是另一串没有 actor 会有的字节。合法
        // actor id 是 `ActorID::Of(job_id, task_id, counter)` 算出来的散列，不会是这
        // 两种。两种都拒掉，因为这种 handle 在调用方手里没有任何明确的失败：调用提交
        // 成功但永远不就绪（`get` 到期才报超时），`serialize()` 把登记的那份 protobuf
        // 再交出来（长度与登记时相同，protobuf 重新序列化可能改写长度前缀；重新序列化
        // 结果为空时报错），坏 handle 因此会一路传播。
        bool all_ff = true;
        bool all_zero = true;
        for (char byte : actor_id) {
            if (byte != static_cast<char>(0xff)) all_ff = false;
            if (byte != '\0') all_zero = false;
        }
        if (all_ff) {
            set_last_error(
                "the bytes register an actor id of 16 bytes that is all 0xff, the nil id: "
                "such a handle never answers a call");
            return ray_bytes_t{nullptr, 0};
        }
        if (all_zero) {
            set_last_error(
                "the bytes register an actor id of 16 bytes that is all 0x00: no actor has "
                "that id, and a handle for it never answers a call");
            return ray_bytes_t{nullptr, 0};
        }
        out_meta->language = fields.actor_language;
        out_meta->creation_descriptor = fields.creation_descriptor;
        return dup_bytes(actor_id);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

// ─── Placement Group ──────────────────────────────────────────

// nlohmann/json is shipped with the Ray C++ SDK headers.
#include <nlohmann/json.hpp>

ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy) {
    ray_clear_error();
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::PlacementGroupCreationOptions options;
        if (name) options.name = name;
        options.strategy = static_cast<ray::PlacementStrategy>(strategy);

        // Parse bundles from JSON: [{"CPU": 1}, {"GPU": 1}]
        if (bundles_json && bundles_json[0] != '\0') {
            using json = nlohmann::json;
            auto j = json::parse(std::string(bundles_json));
            for (auto &bundle : j) {
                std::unordered_map<std::string, double> resources;
                for (auto it = bundle.begin(); it != bundle.end(); ++it) {
                    resources[it.key()] = it.value().get<double>();
                }
                options.bundles.push_back(resources);
            }
        }

        auto group = runtime->CreatePlacementGroup(options);
        return dup_bytes(group.GetID());
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

int ray_placement_group_remove(const char *group_id_data, size_t group_id_len) {
    ray_clear_error();
    if (check_placement_group_id(group_id_data, group_id_len) != 0) return -1;
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return -1;
        // 本地模式不拒绝这一个：`LocalModeTaskSubmitter::CreatePlacementGroup` 把组插进
        // 自己的 map（cpp/src/ray/runtime/task/local_mode_task_submitter.cc:170-180，id 是
        // `PlacementGroupID::Of(job_id)`，18 字节），`RemovePlacementGroup` 就从那个 map 里
        // `erase`（同文件 :182-184）。这个 map 在本地模式下没有读取方（三个查询都抛异常，
        // wait_ready 用接口默认实现，任务与 actor 丢掉组绑定），所以删不删外部看不到，
        // `Ok(())` 是唯一可观察结果。兄弟入口不同：wait_ready 拒绝本地模式是因为接口默认
        // 实现对任何 id 都答 ready，那是一个看得见的假答案
        // （cpp/src/ray/runtime/task/task_submitter.h:49-52）；三个查询拒绝是因为 GCS
        // accessor 在本地模式里是空指针（cpp/src/ray/runtime/abstract_ray_runtime.cc:342-372）。
        std::string group_id(group_id_data, group_id_len);
        runtime->RemovePlacementGroup(group_id);
        return 0;
    } RAY_CATCH
    return -1;
}

/// 把一个组打包成 4 元数组 [id(bin), name(str), state(int), strategy(int)]。
/// id 用 bin、name 用 str：id 是二进制、可能含 0 字节，name 是 UTF-8 文本。
static void pack_placement_group(msgpack::packer<msgpack::sbuffer> &packer,
                                 ray::PlacementGroup &group) {
    std::string id = group.GetID();
    std::string name = group.GetName();
    packer.pack_array(4);
    packer.pack_bin(id.size());
    packer.pack_bin_body(id.data(), id.size());
    packer.pack_str(name.size());
    packer.pack_str_body(name.data(), name.size());
    packer.pack_int(static_cast<int>(group.GetState()));
    packer.pack_int(static_cast<int>(group.GetStrategy()));
}

/// 把若干组打包成 msgpack 数组，复制到 C 堆上交给 Rust。
static ray_bytes_t dup_placement_groups(std::vector<ray::PlacementGroup> &groups) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(groups.size());
    for (auto &group : groups) {
        pack_placement_group(packer, group);
    }
    return dup_bytes(std::string(buffer.data(), buffer.size()));
}

int ray_placement_group_wait_ready(const char *group_id_data, size_t group_id_len,
                                   int timeout_seconds) {
    ray_clear_error();
    if (check_placement_group_id(group_id_data, group_id_len) != 0) return -1;
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return -1;
        if (runtime->IsLocalMode()) {
            // 接口默认实现是 `return true`（task_submitter.h:49-52），本地模式没有
            // 覆盖它，报「ready」等于对任何 id 都说就绪。
            set_last_error(
                "placement groups are not supported in local mode: waiting would "
                "report every group as ready without waiting");
            return -1;
        }
        bool ready = runtime->WaitPlacementGroupReady(
            std::string(group_id_data, group_id_len),
            static_cast<int64_t>(timeout_seconds));
        return ready ? 1 : 0;
    } RAY_CATCH
    return -1;
}

ray_bytes_t ray_placement_group_get_all(void) {
    ray_clear_error();
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            set_last_error(
                "placement group queries are not supported in local mode: the GCS "
                "accessor they read is null there");
            return ray_bytes_t{nullptr, 0};
        }
        auto groups = runtime->GetAllPlacementGroups();
        return dup_placement_groups(groups);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

ray_bytes_t ray_placement_group_get_by_id(const char *group_id_data, size_t group_id_len) {
    ray_clear_error();
    if (check_placement_group_id(group_id_data, group_id_len) != 0) {
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            set_last_error(
                "placement group queries are not supported in local mode: the GCS "
                "accessor they read is null there");
            return ray_bytes_t{nullptr, 0};
        }
        // 查不到时上游返回空组（`PlacementGroup::Empty()`），空数组让调用方把
        // 「查不到」与「查到了」分开。
        std::vector<ray::PlacementGroup> groups;
        auto group = runtime->GetPlacementGroupById(std::string(group_id_data, group_id_len));
        if (!group.Empty()) {
            groups.push_back(std::move(group));
        }
        return dup_placement_groups(groups);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

ray_bytes_t ray_placement_group_get_by_name(const char *name) {
    ray_clear_error();
    if (!name) {
        set_last_error("placement group name is null");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            set_last_error(
                "placement group queries are not supported in local mode: the GCS "
                "accessor they read is null there");
            return ray_bytes_t{nullptr, 0};
        }
        std::vector<ray::PlacementGroup> groups;
        auto group = runtime->GetPlacementGroup(std::string(name));
        if (!group.Empty()) {
            groups.push_back(std::move(group));
        }
        return dup_placement_groups(groups);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

// ─── Misc ─────────────────────────────────────────────────────

bool ray_was_current_actor_restarted(void) {
    ray_clear_error();
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return false;
        return runtime->WasCurrentActorRestarted();
    } catch (const std::exception &e) {
        set_last_error_exception(e);
    } catch (...) {
        set_last_error("unknown exception");
    }
    return false;
}

bool ray_is_local_mode(void) {
    auto runtime = ray::internal::GetRayRuntime();
    return runtime ? runtime->IsLocalMode() : false;
}

ray_bytes_t ray_get_namespace(void) {
    ray_clear_error();
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) return dup_bytes(saved_init_namespace());
        return dup_bytes(runtime->GetNamespace());
    } catch (const std::exception &e) {
        set_last_error_exception(e);
    } catch (...) {
        set_last_error("unknown exception");
    }
    return ray_bytes_t{nullptr, 0};
}

/// 按名字查一个 actor，同时交出它的语言与创建描述符类型。
///
/// `out_meta` 必须是调用方提供的非空指针。查到 actor 时写这两项：集群模式从
/// `runtime->SerializeActorHandle(actor_id)` 拿回的 `rpc.ActorHandle` 里读
/// `actor_language`（`src/ray/protobuf/core_worker.proto:42`，字段 5，取值 `PYTHON = 0`、
/// `JAVA = 1`、`CPP = 2`，`src/ray/protobuf/common.proto:26-30`）与字段 6 里那个
/// oneof 成员的字段号（java 1、python 2、cpp 3，字段 6 缺席时是 0）；本地模式写
/// `kLanguageCpp` 与 `kCreationDescriptorCpp`。查不到时不写。语言只能从序列化字节里
/// 读：`GetActorId` 只给回 16 字节 id
/// （`cpp/src/ray/runtime/abstract_ray_runtime.cc:255-263`）。
ray_bytes_t ray_get_actor(const char *name, const char *ray_namespace,
                          ray_actor_handle_meta_t *out_meta) {
    ray_clear_error();
    if (!out_meta) {
        set_last_error("out_meta is null");
        return ray_bytes_t{nullptr, 0};
    }
    if (!name) {
        set_last_error("actor name is null");
        return ray_bytes_t{nullptr, 0};
    }
    if (name[0] == '\0') {
        // 集群模式把名字交给 `CoreWorker::GetNamedActorHandle`，它的第一句是
        // `RAY_CHECK(!name.empty())`（src/ray/core_worker/core_worker.cc:2594），
        // RAY_CATCH 拦不住，进程直接终止。带内部 NUL 的名字经 Rust 侧的 to_cstring
        // 也变成空串（#27），所以两者都在这里拒掉。只有空白字符的名字是合法的，
        // 不做 trim。
        set_last_error(
            "actor name is empty: the core worker requires a non-empty name "
            "(RAY_CHECK(!name.empty()) at src/ray/core_worker/core_worker.cc:2594) and "
            "stops the process on the empty one. A name with an interior NUL byte "
            "arrives here as an empty C string as well (#27); a whitespace-only name "
            "is not trimmed and is looked up as it is");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = runtime_for_call();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        // namespace 允许为空：`NativeTaskSubmitter::GetActor` 在 namespace 为空时用
        // job config 的 namespace 顶替（cpp/src/ray/runtime/task/native_task_submitter.cc:181-186），
        // 空指针按空串处理（现状就是 `ray_namespace ? ... : ""`）。
        std::string ns_str = ray_namespace ? std::string(ray_namespace) : "";
        std::string actor_id = runtime->GetActorId(std::string(name), ns_str);
        if (actor_id.empty()) return ray_bytes_t{nullptr, 0};
        if (runtime->IsLocalMode()) {
            // 本地模式只建得出 C++ actor（`ray_actor_create_python_with_options` 在
            // 本地模式拒绝），submitter 也只执行注册过的 Rust 函数
            // （cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40），所以名字
            // 表里给出的 handle 语言是 C++、创建描述符是 Cpp
            // （同文件 :52-56 把任务规格的语言写成 CPP）。
            out_meta->language = kLanguageCpp;
            out_meta->creation_descriptor = kCreationDescriptorCpp;
            return dup_bytes(actor_id);
        }
        // 集群模式：这个 id 刚刚由 `GetNamedActorHandle` 的 `AddNewActorHandle` 登记
        // 进 actor manager（src/ray/core_worker/actor_manager.cc:68-83），所以下面这句
        // 不会再走到「查不到 handle」的 RAY_CHECK。语言只能从序列化字节里读：
        // `AbstractRayRuntime` 只给回 16 字节 id。
        std::string serialized = runtime->SerializeActorHandle(actor_id);
        if (serialized.empty()) {
            set_last_error("the core worker returned no serialized actor handle");
            return ray_bytes_t{nullptr, 0};
        }
        ActorHandlePayloadFields fields;
        if (check_actor_handle_payload(serialized.data(), serialized.size(), &fields) != 0) {
            return ray_bytes_t{nullptr, 0};
        }
        out_meta->language = fields.actor_language;
        out_meta->creation_descriptor = fields.creation_descriptor;
        return dup_bytes(actor_id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

int ray_cancel(const char *id_data, size_t id_len, bool force_kill, bool recursive) {
    ray_clear_error();
    try {
        if (!id_data) {
            set_last_error("object id is null");
            return -1;
        }
        if (id_len != ray::ObjectID::kLength) {
            std::string msg = "object id must be "
                + std::to_string(ray::ObjectID::kLength) + " bytes, got "
                + std::to_string(id_len) + " bytes";
            set_last_error(msg.c_str());
            return -1;
        }

        auto runtime = runtime_for_call();
        if (!runtime) return -1;
        if (runtime->IsLocalMode()) {
            // 这个入口不经过 task_submitter_：C++ SDK 没有 AbstractRayRuntime::CancelTask，
            // CancelTask 只存在于 core worker 上，所以下面直接向 CoreWorkerProcess 要。本地模式
            // 从不启动 core worker 进程，那里的 EnsureInitialized 走 QuickExit()，最终 _Exit(1)
            // （src/ray/core_worker/core_worker_process.cc:842-850、
            // src/ray/util/process.cc:845-848），RAY_CATCH 抓不到 _Exit。本地也没有取消路径：
            // LocalModeTaskSubmitter 提交任务后不保留任何句柄：普通任务直接 post 给
            // boost::asio::thread_pool，actor 创建与 actor 方法任务在调用线程里同步跑完
            // （cpp/src/ray/runtime/task/local_mode_task_submitter.cc:115-135），
            // LocalModeRayRuntime 也没有覆盖任何取消接口
            // （cpp/src/ray/runtime/local_mode_ray_runtime.h:24-36）。
            set_last_error(
                "tasks cannot be cancelled in local mode: local mode never starts a core "
                "worker process, and the local submitter keeps no handle to withdraw a "
                "task once it is submitted");
            return -1;
        }

        ray::ObjectID object_id(id_data, id_len);
        auto &core_worker = ray::core::CoreWorkerProcess::GetCoreWorker();
        ray::Status status = core_worker.CancelTask(object_id, force_kill, recursive);
        if (!status.ok()) {
            // The SDK exports ~Status but not ToString/CodeAsString/message(), so
            // no Ray failure text is readable here, and cancelling an already
            // finished task returns OK, so a no-op cancel is not reported either.
            set_last_error("the core worker refused the cancellation");
            return -1;
        }
        return 0;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return -1;
    }
}

/// 单调毫秒时钟，用于计算多元素批次里每个就绪元素的剩余期限。
static int64_t steady_clock_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

/// 读取单个已就绪对象并填入 `slot`；失败时把 `slot` 置空，且仅在整批首个失败元素上
/// 写入线程局部错误通道。
static void fetch_single_element(ray::internal::RayRuntime &runtime,
                                 const std::string &id,
                                 int timeout_ms,
                                 ray_bytes_t &slot,
                                 bool &failure_recorded) {
    slot = ray_bytes_t{nullptr, 0};
    try {
        auto buffer = runtime.Get(id, timeout_ms);
        if (!buffer) {
            if (!failure_recorded) {
                set_last_error(
                    "the object store returned no buffer for an object in the batch");
                failure_recorded = true;
            }
            return;
        }
        const size_t size = buffer->size();
        if (size == 0) {
            if (!failure_recorded) {
                set_last_error_kind(kUntranslatedObjectError,
                                    RAY_ERROR_KIND_UNCLASSIFIED);
                failure_recorded = true;
            }
            return;
        }
        char *data = static_cast<char *>(std::malloc(size));
        if (!data) {
            if (!failure_recorded) {
                set_last_error("out of memory copying an object in the batch");
                failure_recorded = true;
            }
            return;
        }
        std::memcpy(data, buffer->data(), size);
        slot = ray_bytes_t{data, size};
    } catch (const std::exception &e) {
        if (!failure_recorded) {
            set_last_error_exception(e);
            failure_recorded = true;
        }
    }
}

/// 上游 C++ SDK 的批量 `Get` 在含有异常对象的混合批次下会留下空 `RayObject` 指针并触发
/// SIGSEGV：
/// - `CoreWorkerMemoryStore::GetImpl`
///   （`src/ray/core_worker/store_provider/memory_store/memory_store.cc:276-419`）在
///   `:308-333` 发现批次中已有异常对象（`IsException()` 为真且非 plasma 占位错误）时提前
///   返回 `Status::OK()`，其余尚未就绪的元素在 `results` 中保持 `nullptr`。
/// - `CoreWorker::GetObjects`（`src/ray/core_worker/core_worker.cc:1337-1426`）在
///   `:1375-1382` 把已转入 plasma 的元素从 `result_map` 移到 `plasma_object_ids`，`:1384`
///   的 `if (!got_exception && !plasma_object_ids.empty())` 在批次含有异常对象时跳过
///   plasma 取数；`:1404-1423` 让未取到的元素在 `results[i]` 保持 `nullptr`，且
///   `timeout_ms < 0 && will_throw_exception` 时跳过 `RAY_CHECK(!missing_result)`，返回
///   `Status::OK()` 与空指针。
/// - `NativeObjectStore::GetRaw`
///   （`cpp/src/ray/runtime/object/native_object_store.cc:90-134`）在 `:104-105` 未检查空
///   指针便直接执行 `results[i]->GetMetadata()`，进程收到 SIGSEGV 终止。
/// - `CoreWorker::Wait`（`src/ray/core_worker/core_worker.cc:1461-1566`）经
///   `CoreWorkerMemoryStore::Wait`（`memory_store.cc:450-481`，另见 `:315` 注释以及
///   `memory_store.cc:421-448` 的 `Get`）把 `abort_if_any_object_is_exception` 与
///   `at_most_num_objects` 均设为 `false`，仅返回就绪标志，不触发上述空指针路径；由于
///   `CoreWorker::Wait` 在 `:1475-1484` 拒绝重复 ID，非空批次先按 28 字节 ObjectID
///   去重后调用一次 `Wait` 确定截止时间，再按原顺序对每个就绪元素执行单对象 `Get`。
///   `Wait` 不因批次里的异常对象提前返回，因此多元素批次会等到全部就绪或期限到期。
ray_bytes_t *ray_get_many(const ray_bytes_t *ids, size_t count, int timeout_ms) {
    ray_clear_error();
    try {
        if (timeout_ms < -1) {
            set_last_error("timeout_ms must be -1 or non-negative");
            return nullptr;
        }
        if (count > 0 && !ids) {
            set_last_error("object id array is null");
            return nullptr;
        }
        // 上界在遍历 ID 与任何 reserve(count) 之前拒绝，避免与 count 成正比的工作；
        // SDK 的 `Wait` 只收 int，后面的 static_cast 因此安全。
        if (count > static_cast<size_t>(std::numeric_limits<int>::max())) {
            set_last_error("the batch holds more object ids than Ray's Wait accepts");
            return nullptr;
        }
        for (size_t i = 0; i < count; i++) {
            if (check_object_id(ids[i].data, ids[i].len) != 0) return nullptr;
        }
        auto runtime = runtime_for_call();
        if (!runtime) return nullptr;

        if (count == 0) {
            auto *out = static_cast<ray_bytes_t *>(std::malloc(sizeof(ray_bytes_t)));
            if (!out) {
                set_last_error("out of memory allocating the result array");
                return nullptr;
            }
            return out;
        }

        std::vector<std::string> unique_ids;
        unique_ids.reserve(count);
        std::vector<size_t> unique_of_element;
        unique_of_element.reserve(count);
        std::unordered_map<std::string, size_t> index_by_id;
        index_by_id.reserve(count);
        for (size_t i = 0; i < count; i++) {
            std::string id(ids[i].data, ids[i].len);
            auto [it, inserted] = index_by_id.emplace(std::move(id), unique_ids.size());
            if (inserted) {
                unique_ids.push_back(it->first);
            }
            unique_of_element.push_back(it->second);
        }
        if (unique_ids.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
            set_last_error("too many unique object IDs in batch");
            return nullptr;
        }

        const int64_t start = steady_clock_ms();
        auto ready = runtime->Wait(
            unique_ids, static_cast<int>(unique_ids.size()), timeout_ms);

        auto *out = static_cast<ray_bytes_t *>(std::malloc(count * sizeof(ray_bytes_t)));
        if (!out) {
            set_last_error("out of memory allocating the result array");
            return nullptr;
        }

        bool failure_recorded = false;
        for (size_t i = 0; i < count; i++) {
            const size_t unique_idx = unique_of_element[i];
            if (unique_idx >= ready.size() || !ready[unique_idx]) {
                out[i] = ray_bytes_t{nullptr, 0};
                if (!failure_recorded) {
                    set_last_error_kind("Get timed out: some object(s) not ready.",
                                        RAY_ERROR_KIND_TIMEOUT);
                    failure_recorded = true;
                }
                continue;
            }
            int element_timeout = timeout_ms;
            if (timeout_ms >= 0) {
                element_timeout = static_cast<int>(
                    std::max<int64_t>(0, timeout_ms - (steady_clock_ms() - start)));
            }
            fetch_single_element(
                *runtime, unique_ids[unique_idx], element_timeout, out[i], failure_recorded);
        }
        return out;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return nullptr;
    }
}

void ray_free_bytes_array(ray_bytes_t *array, size_t count) {
    if (!array) return;
    for (size_t i = 0; i < count; i++) {
        if (array[i].data) std::free(const_cast<char *>(array[i].data));
    }
    std::free(array);
}

// ─── Memory Management ────────────────────────────────────────

void ray_free_bytes(ray_bytes_t *ptr) {
    if (ptr && ptr->data) {
        std::free(const_cast<char *>(ptr->data));
        ptr->data = nullptr;
        ptr->len = 0;
    }
}

void ray_free_bools(bool *ptr) {
    if (ptr) std::free(ptr);
}

// ─── Error Handling ───────────────────────────────────────────

void ray_last_error_bytes(const char **data, size_t *len) {
    if (g_last_error_kind == RAY_ERROR_KIND_NONE) {
        if (data) *data = nullptr;
        if (len) *len = 0;
        return;
    }
    if (data) *data = g_last_error.data();
    if (len) *len = g_last_error.size();
}

int ray_last_error_kind(void) {
    return g_last_error_kind;
}

void ray_clear_error(void) {
    g_last_error.clear();
    g_last_error_kind = RAY_ERROR_KIND_NONE;
}
