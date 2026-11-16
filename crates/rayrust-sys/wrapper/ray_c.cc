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

// ─── Thread-local error storage ─────────────────────────────────
// Instead of fprintf(stderr), we store the error message in a thread-local
// string so the Rust side can retrieve it via ray_last_error().
#include <cstdint>
#include <string>
#include <cstring>

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

/// Parse an options JSON string and fill an ActorCreationOptions struct.
/// `options_json` may be NULL or empty (returns default options).
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
    if (j.contains("placement_group_id")) {
        std::string pg_hex = j["placement_group_id"].get<std::string>();
        std::string pg_id = hex_decode(pg_hex);
        if (!pg_id.empty()) {
            auto runtime = ray::internal::GetRayRuntime();
            if (runtime) {
                options.group = runtime->GetPlacementGroupById(pg_id);
                if (j.contains("bundle_index")) {
                    options.bundle_index = j["bundle_index"].get<int>();
                }
            }
        }
    }
}

// ─── Lifecycle ────────────────────────────────────────────────

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
        return 0;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return -1;
    }
}

bool ray_is_initialized(void) {
    return ray::IsInitialized();
}

void ray_shutdown(void) {
    ray::Shutdown();
}

// ─── Object Store ─────────────────────────────────────────────

ray_bytes_t ray_put(const char *data, size_t len) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
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
/// vs value args (pass by serialized msgpack).
static std::vector<ray::internal::TaskArg>
build_task_args_cpp(const ray_bytes_t *args, size_t arg_count,
                    const bool *is_ref = nullptr) {
    std::vector<ray::internal::TaskArg> task_args;
    task_args.reserve(arg_count);
    for (size_t i = 0; i < arg_count; i++) {
        ray::internal::TaskArg arg;
        if (is_ref && is_ref[i]) {
            // ObjectRef argument: pass by reference (object ID)
            arg.id = std::string(args[i].data, args[i].len);
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

ray_bytes_t ray_task_call(const char *func_name,
                           const ray_bytes_t *args,
                           size_t arg_count,
                           const bool *is_ref) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder{std::move(fn)};

        auto task_args = build_task_args_cpp(args, arg_count, is_ref);

        ray::internal::CallOptions options;
        std::string id = runtime->Call(holder, task_args, options);
        return dup_bytes(id);
    } RAY_CATCH
    return ray_bytes_t{nullptr, 0};
}

/// Call a remote task with resource requirements.
/// `resources_json` is a JSON string like `{"CPU":1,"GPU":0.5}` or NULL to skip.
ray_bytes_t ray_task_call_with_resources(const char *func_name,
                                          const ray_bytes_t *args,
                                          size_t arg_count,
                                          const bool *is_ref,
                                          const char *resources_json) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder{std::move(fn)};

        auto task_args = build_task_args_cpp(args, arg_count, is_ref);

        ray::internal::CallOptions options;
        if (resources_json && resources_json[0] != '\0') {
            auto j = nlohmann::json::parse(resources_json);
            for (auto it = j.begin(); it != j.end(); ++it) {
                options.resources[it.key()] = it.value().get<double>();
            }
        }
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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), std::string(function_name),
            "", ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count, is_ref);

        ray::internal::CallOptions options;
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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder("", std::string(func_name),
                                                   std::string(func_name),
                                                   ray::internal::LangType::CPP);

        auto task_args = build_task_args_cpp(args, arg_count);

        ray::internal::ActorCreationOptions options;
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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), "__init__",
            std::string(class_name), ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count);

        ray::internal::ActorCreationOptions options;
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
                            size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        // For CPP actor calls, RemoteFunctionHolder should use single-argument
        // constructor (only function_name_, class_name_ stays empty).
        // This matches ActorHandle::Task() behavior in the SDK.
        // The function_name_ must match the registered member function name
        // (e.g. "__rayrust_actor_factory_counter::increment").
        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder(fn);

        auto task_args = build_task_args_cpp(args, arg_count);

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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

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

// ─── Placement Group ──────────────────────────────────────────

// nlohmann/json is shipped with the Ray C++ SDK headers.
#include <nlohmann/json.hpp>

ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
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

// ─── Misc ─────────────────────────────────────────────────────

bool ray_was_current_actor_restarted(void) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) return runtime->WasCurrentActorRestarted();
    } catch (...) {}
    return false;
}

ray_bytes_t ray_get_namespace(void) {
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
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

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

// ─── Async Get (polling thread + eventfd) ────────────────────
//
// Polling thread: Get(timeout=100ms) loop + eventfd notification.

#include <sys/eventfd.h>
#include <unistd.h>
#include <thread>

struct ray_async_get {
    int efd;
    bool ready;
    bool error;
};

ray_async_get_t *ray_async_get_start(const char *id_data, size_t id_len) {
    try {
        int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0) return nullptr;

        auto *handle = new ray_async_get{efd, false, false};

        // Copy the object ID for the polling thread
        std::string id_str(id_data, id_len);

        // Start a lightweight polling thread that uses Get(timeout=100ms)
        // to check if the object is available. This is NOT the same as
        // spawn_blocking(Get(-1)) — each poll blocks at most 100ms,
        // and the thread releases between polls.
        //
        // The thread signals via eventfd when the object is ready.
        // The Rust side uses AsyncFd to poll the eventfd — zero tokio
        // threads blocked.
        std::thread([handle, id_str]() {
            auto runtime = ray::internal::GetRayRuntime();
            if (!runtime) {
                handle->error = true;
                uint64_t val = 1;
                write(handle->efd, &val, sizeof(val));
                return;
            }

            while (!handle->ready && !handle->error) {
                try {
                    // timeout=100ms: returns immediately if object is local,
                    // blocks at most 100ms if not.
                    auto buf = runtime->Get(id_str, 100);
                    if (buf) {
                        handle->ready = true;
                        break;
                    }
                } catch (...) {
                    // Not ready yet — retry
                }
            }

            uint64_t val = 1;
            write(handle->efd, &val, sizeof(val));
        }).detach();

        return handle;
    } catch (const std::exception &e) {
        set_last_error_exception(e);
        return nullptr;
    }
}

int ray_async_get_fd(const ray_async_get_t *handle) {
    return handle ? handle->efd : -1;
}

int ray_async_get_is_ready(const ray_async_get_t *handle) {
    if (!handle) return -1;
    return handle->ready ? 1 : (handle->error ? -1 : 0);
}

ray_bytes_t ray_async_get_result(const ray_async_get_t *handle) {
    // Not used — Rust side calls get_raw() after eventfd fires.
    (void)handle;
    return ray_bytes_t{nullptr, 0};
}

void ray_async_get_destroy(ray_async_get_t *handle) {
    if (handle) {
        if (handle->efd >= 0) close(handle->efd);
        delete handle;
    }
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
