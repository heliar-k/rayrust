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
// string so the Rust side can retrieve it via ray_last_error().
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <string>
#include <cstring>
#include <type_traits>

static thread_local std::string g_last_error;
static thread_local int g_last_error_kind = RAY_ERROR_KIND_NONE;

/// Store an error message (called from catch blocks).
static void set_last_error(const char *msg) {
    g_last_error = msg ? msg : "";
    g_last_error_kind = RAY_ERROR_KIND_GENERIC;
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
static void set_last_error_exception(const std::exception &e) {
    g_last_error = e.what();
    g_last_error_kind = ray_exception_kind(e);
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

    FILE *dbg = fopen("/tmp/rayrust_debug.log", "a");
    if (dbg) {
        fprintf(dbg, "[rayrust] register_fn: '%s' map_size=%zu\n", name.c_str(), map.size());
        fclose(dbg);
    }
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
    FILE *dbg = fopen("/tmp/rayrust_debug.log", "a");
    if (dbg) {
        fprintf(dbg, "[rayrust] register_member_fn: '%s' %s verify=%s map_size=%zu\n",
                name.c_str(),
                inserted ? "inserted" : "overwritten",
                verify ? "OK" : "FAIL",
                mem_map.size());
        fclose(dbg);
    }
}

// ─── Helpers ──────────────────────────────────────────────────

/// Copy a std::string (may contain null bytes) into a heap-allocated ray_bytes_t.
static ray_bytes_t dup_bytes(const std::string &s) {
    char *data = static_cast<char *>(std::malloc(s.size()));
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
    if (j.contains("serialized_runtime_env_info")) {
        options.serialized_runtime_env_info =
            j["serialized_runtime_env_info"].get<std::string>();
    }
    parse_placement_group_fields(j, options.group, options.bundle_index);
}

/// Parse an options JSON string and fill a CallOptions struct.
/// `options_json` may be NULL or empty (returns default options).
///
/// The placement group fields go through `parse_placement_group_fields`, which
/// throws instead of letting the task run outside the group the caller asked
/// for or letting an SDK RAY_CHECK abort the driver. `runtime_env` keeps its own
/// check below because the actor path has no such field.
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
    if (j.contains("runtime_env")) {
        // The Rust side sends the runtime env as a JSON string value, so the
        // outer document has no splicing opportunity and a scalar never looks
        // like an object.
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
        options.serialized_runtime_env_info =
            ray::RuntimeEnv::Deserialize(runtime_env_json)
                .SerializeToRuntimeEnvInfo();
    }
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
/// `LocalModeTaskSubmitter` 丢弃整个 options，这一步在本地模式没有效果。
static void apply_default_task_cpu(ray::internal::CallOptions &options) {
    options.resources.emplace("CPU", 1.0);
}

// ─── Lifecycle ────────────────────────────────────────────────

static void reopen_ref_count_gate(void);

int ray_init(const char *address, int local_mode, const char *node_ip,
             const char *code_search_path,
             const char *runtime_env_json,
             const char *log_dir,
             int actor_lifetime,
             const char *ns) {
    ray::RayConfig config;
    if (address && address[0] != '\0') {
        config.address = address;
    }
    config.local_mode = local_mode != 0;

    // Set default actor lifetime: 0 = NON_DETACHED (default), 1 = DETACHED.
    if (actor_lifetime == 1) {
        config.default_actor_lifetime = ray::ActorLifetime::DETACHED;
    }

    // Set job-level namespace for named actors.
    if (ns && ns[0] != '\0') {
        config.ray_namespace = ns;
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
        ray::Init(config, argc, const_cast<char**>(arg_ptrs.data()));
        reopen_ref_count_gate();
        return 0;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return -1;
    }
}

bool ray_is_initialized(void) {
    return ray::IsInitialized();
}

/// 引用计数调用与 `ray_shutdown` 互斥。
///
/// `CoreWorkerProcess::Shutdown()` 先 `ShutdownDriver()` 把 `core_worker_` 置空，
/// 再 `core_worker_process.reset()`（src/ray/core_worker/core_worker_process.cc:101-108、
/// 952-956）；在这两步之间 `IsInitialized()` 仍为真，而 `GetCoreWorker()` 发现
/// `core_worker_` 为空就走 `QuickExit()` 直接终止进程（同文件 964-981），Rust 侧
/// 没有捕获机会。
///
/// 闸门用「在途计数 + 关闭标志」而不是读写锁：`std::shared_mutex` 在 glibc 上是
/// 读者优先，读者在共享锁里等 Ray 自己的引用计数互斥锁，关闭线程会一直拿不到独占
/// 锁。这里关闭一开始就拒绝新的引用计数调用，随后等已进入的调用退出，后者只有微秒
/// 级，所以关闭一定能推进。
///
/// 只有引用计数这两个函数进闸门：`get`/`put`/`wait` 这类阻塞调用不能进，
/// 否则关闭要等一个可能无限的 `Get`。
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

bool &ref_count_closing() {
    static bool closing = false;
    return closing;
}

}  // namespace

/// 进入一次引用计数调用。关闭已经开始时返回 false，调用方跳过计数。
static bool enter_ref_count(void) {
    std::lock_guard<std::mutex> lock(ref_count_gate_mutex());
    if (ref_count_closing()) return false;
    ++ref_count_in_flight();
    return true;
}

static void leave_ref_count(void) {
    std::lock_guard<std::mutex> lock(ref_count_gate_mutex());
    --ref_count_in_flight();
    if (ref_count_in_flight() == 0) ref_count_gate_drained().notify_all();
}

/// 关闭之前调用：拒绝新的引用计数调用，等已经进入的那些退出。
static void close_ref_count_gate(void) {
    std::unique_lock<std::mutex> lock(ref_count_gate_mutex());
    ref_count_closing() = true;
    ref_count_gate_drained().wait(lock, [] { return ref_count_in_flight() == 0; });
}

/// `ray_init` 成功之后调用：重新打开闸门。
///
/// 只清关闭标志。在途计数不能在这里抹平：`ray_init` 与引用计数调用没有互斥，
/// 清零之后那几次在途调用的 `leave` 会把计数减成负数，`close_ref_count_gate`
/// 的谓词从此不成立，关闭永远等下去。不变量成立时它本来就是 0。
static void reopen_ref_count_gate(void) {
    std::lock_guard<std::mutex> lock(ref_count_gate_mutex());
    ref_count_closing() = false;
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

void ray_shutdown(void) {
    close_ref_count_gate();
    ray::Shutdown();
}

// ─── Object Store ─────────────────────────────────────────────

ray_bytes_t ray_put(const char *data, size_t len) {
    ray_clear_error();
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

        std::string id(id_data, id_len);
        auto buffer = runtime->Get(id, timeout_ms);
        if (!buffer) {
            set_last_error("the object store returned no buffer");
            return ray_bytes_t{nullptr, 0};
        }

        char *data = static_cast<char *>(std::malloc(buffer->size()));
        if (!data) {
            set_last_error("out of memory copying the object");
            return ray_bytes_t{nullptr, 0};
        }
        std::memcpy(data, buffer->data(), buffer->size());
        return ray_bytes_t{data, buffer->size()};
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

// ─── Reference Counting ──────────────────────────────────────

/// 引用计数入口的共同前置检查：id 非空且正好 28 字节。
/// `ObjectID::FromBinary` 对非空且长度不符的输入 RAY_CHECK 终止进程，对空串返回
/// nil（src/ray/common/id.h:430-440），长度必须先在这里拒掉。
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

bool *ray_wait(const ray_bytes_t *ids, size_t count, int num_objects, int timeout_ms) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return nullptr;

        std::vector<std::string> id_vec;
        id_vec.reserve(count);
        for (size_t i = 0; i < count; i++) {
            id_vec.emplace_back(ids[i].data, ids[i].len);
        }

        auto results = runtime->Wait(id_vec, num_objects, timeout_ms);

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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder{std::move(fn)};

        auto task_args = build_task_args_cpp(runtime.get(), args, arg_count, is_ref);

        // The SDK leaves bundle_index uninitialized in CallOptions; -1 means
        // "any bundle" and must be set before parse_call_options may leave it.
        ray::internal::CallOptions options;
        options.bundle_index = -1;
        parse_call_options(options_json, options);
        apply_default_task_cpu(options);
        std::string id = runtime->Call(holder, task_args, options);
        return dup_bytes(id);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

/// Call a Python remote function.
/// `module_name` and `function_name` are null-terminated C strings.
/// `args` are msgpack-serialized arguments (will be wrapped with xlang header).
ray_bytes_t ray_task_call_python(const char *module_name,
                                  const char *function_name,
                                  const ray_bytes_t *args,
                                  size_t arg_count,
                                  const bool *is_ref) {
    ray_clear_error();
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), std::string(function_name),
            "", ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count, is_ref);

        ray::internal::CallOptions options;
        apply_default_task_cpu(options);
        std::string id = runtime->Call(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

// ─── Actor ────────────────────────────────────────────────────

/// Create an actor with full ActorCreationOptions via options JSON.
ray_bytes_t ray_actor_create_with_options(const char *func_name,
                                            const ray_bytes_t *args,
                                            size_t arg_count,
                                            const char *options_json) {
    ray_clear_error();
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

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
        std::string id = runtime->CreateActor(holder, task_args, options);
        return dup_bytes(id);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

/// Create a Python actor with full ActorCreationOptions via options JSON.
ray_bytes_t ray_actor_create_python_with_options(const char *module_name,
                                                   const char *class_name,
                                                   const ray_bytes_t *args,
                                                   size_t arg_count,
                                                   const char *options_json) {
    ray_clear_error();
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

        // For CPP actor calls, RemoteFunctionHolder should use single-argument
        // constructor (only function_name_, class_name_ stays empty).
        // This matches ActorHandle::Task() behavior in the SDK.
        // The function_name_ must match the registered member function name
        // (e.g. "__rayrust_actor_factory_counter::increment").
        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder(fn);

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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
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
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) {
            std::string actor_id(actor_id_data, actor_id_len);
            runtime->KillActor(actor_id, no_restart);
        }
    } catch (const std::exception &e) {
        set_last_error_exception(e);
    }
}

ray_bytes_t ray_actor_handle_serialize(const char *actor_id_data, size_t actor_id_len) {
    ray_clear_error();
    if (!actor_id_data || actor_id_len != kActorIdLength) {
        std::string msg = "actor id must be " + std::to_string(kActorIdLength) +
                          " bytes, got " + std::to_string(actor_id_len) + " bytes";
        set_last_error(msg.c_str());
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }
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

ray_bytes_t ray_actor_handle_deserialize(const char *serialized_data,
                                         size_t serialized_len) {
    ray_clear_error();
    if (!serialized_data || serialized_len == 0) {
        set_last_error("the serialized actor handle is empty");
        return ray_bytes_t{nullptr, 0};
    }
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }
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
        // 畸形字节：上游丢弃 `ParseFromString` 的结果（actor_handle.cc:66、:135），
        // 空 actor id 变成 `ActorID::Nil()` 并登记成 nil actor。合法 actor id 是
        // `ActorID::Of(job_id, task_id, counter)` 算出来的散列，不会是全 0xff（
        // `BaseID()` 的填充）也不会是全 0x00。拒掉它，因为 nil handle 在调用方手里
        // 没有任何明确的失败：调用提交成功但永远不就绪（`get` 到期才报超时），
        // `serialize()` 把登记的那份 protobuf 再交出来（长度与登记时相同，protobuf
        // 重新序列化可能改写长度前缀；重新序列化结果为空时报错），坏 handle 因此会一路传播。
        bool all_ff = true;
        bool all_zero = true;
        for (char byte : actor_id) {
            if (byte != static_cast<char>(0xff)) all_ff = false;
            if (byte != '\0') all_zero = false;
        }
        if (all_ff || all_zero) {
            set_last_error(
                "the bytes are not a valid actor handle: they register as the nil actor, "
                "which never answers a call");
            return ray_bytes_t{nullptr, 0};
        }
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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

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

void ray_placement_group_remove(const char *group_id_data, size_t group_id_len) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) {
            std::string group_id(group_id_data, group_id_len);
            runtime->RemovePlacementGroup(group_id);
        }
    } catch (const std::exception &e) {
        set_last_error_exception(e);
    }
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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return -1;
        }
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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }
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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }
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
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }
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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) return runtime->WasCurrentActorRestarted();
    } catch (...) {}
    return false;
}

bool ray_is_local_mode(void) {
    auto runtime = ray::internal::GetRayRuntime();
    return runtime ? runtime->IsLocalMode() : false;
}

ray_bytes_t ray_get_namespace(void) {
    ray_clear_error();
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) return dup_bytes(runtime->GetNamespace());
        set_last_error("Ray runtime is not initialized");
    } catch (const std::exception &e) {
        set_last_error_exception(e);
    } catch (...) {
        set_last_error("unknown exception");
    }
    return ray_bytes_t{nullptr, 0};
}

ray_bytes_t ray_get_actor(const char *name, const char *ray_namespace) {
    ray_clear_error();
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return ray_bytes_t{nullptr, 0};
        }

        std::string ns_str = ray_namespace ? std::string(ray_namespace) : "";
        std::string actor_id = runtime->GetActorId(std::string(name), ns_str);
        if (actor_id.empty()) return ray_bytes_t{nullptr, 0};
        return dup_bytes(actor_id);
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return ray_bytes_t{nullptr, 0};
    }
}

int ray_cancel(const char *id_data, size_t id_len, bool force_kill, bool recursive) {
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

ray_bytes_t *ray_get_many(const ray_bytes_t *ids, size_t count, int timeout_ms) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) {
            set_last_error("Ray runtime is not initialized");
            return nullptr;
        }

        std::vector<std::string> id_vec;
        id_vec.reserve(count);
        for (size_t i = 0; i < count; i++) {
            id_vec.emplace_back(ids[i].data, ids[i].len);
        }

        auto results = runtime->Get(id_vec, timeout_ms);

        auto *out = static_cast<ray_bytes_t *>(std::malloc(count * sizeof(ray_bytes_t)));
        if (!out) {
            set_last_error("out of memory allocating the result array");
            return nullptr;
        }

        for (size_t i = 0; i < count && i < results.size(); i++) {
            if (results[i]) {
                char *data = static_cast<char *>(std::malloc(results[i]->size()));
                if (data) {
                    std::memcpy(data, results[i]->data(), results[i]->size());
                    out[i] = ray_bytes_t{data, results[i]->size()};
                } else {
                    out[i] = ray_bytes_t{nullptr, 0};
                }
            } else {
                out[i] = ray_bytes_t{nullptr, 0};
            }
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

const char *ray_last_error(void) {
    return g_last_error.empty() ? nullptr : g_last_error.c_str();
}

int ray_last_error_kind(void) {
    return g_last_error_kind;
}

void ray_clear_error(void) {
    g_last_error.clear();
    g_last_error_kind = RAY_ERROR_KIND_NONE;
}
