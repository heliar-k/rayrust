/// C ABI wrapper header — bridges Ray C++ SDK templates to plain C functions.
///
/// The Ray C++ SDK uses heavy C++ templates (Put<T>, Task<F>, etc.) that have no
/// stable ABI. This header exposes a minimal C interface that the Rust FFI layer
/// can call. The C++ source (ray_c.cc) wraps `ray::internal::GetRayRuntime()` directly,
/// bypassing the template API.
///
/// IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
/// All functions that return or accept IDs use `ray_bytes_t` (ptr + len)
/// instead of null-terminated `char*` to be binary-safe.

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Byte buffer: data pointer + length. Binary-safe.
typedef struct {
    const char *data;
    size_t len;
} ray_bytes_t;

/// 一个 actor handle 的语言与创建描述符类型，由 `ray_actor_handle_deserialize` 与
/// `ray_get_actor` 交给 Rust 侧的登记表。
///
/// `language` 是 `rpc.ActorHandle.actor_language`（`src/ray/protobuf/core_worker.proto:42`，
/// 字段 5）的取值：`PYTHON = 0`、`JAVA = 1`、`CPP = 2`
/// （`src/ray/protobuf/common.proto:26-30`）。
/// `creation_descriptor` 是字段 6 里那个 oneof 成员的字段号
/// （`src/ray/protobuf/common.proto:158-164`）：0 = 没有描述符，java 1、python 2、cpp 3。
/// 这个类型要一起记下来，因为 Python 调用路径会把 handle 的创建描述符按
/// `PythonFunctionDescriptor` 读（`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`），
/// 描述符缺席或类型不符时读越界。
typedef struct {
    int language;
    int creation_descriptor;
} ray_actor_handle_meta_t;

/// Rust 回调返回的哨兵长度（`data` 为空指针），配合 `ray_func_callback_t` 与
/// `ray_member_callback_t` 使用。真实结果不可能有这么长（`std::malloc` 申请不到
/// 2^64-1 字节），所以它们不会与合法结果相撞。
///
/// - `RAY_INTENTIONAL_EXIT_SENTINEL`：actor 方法里调用了 `exit_actor()`。C 侧在
///   Rust 帧返回之后抛 `ray::internal::RayIntentionalSystemExitException`，与
///   `AbstractRayRuntime::ExitActor` 抛的是同一个类；`GetExecuteResult` 把它变成
///   `Status::IntentionalSystemExit`（`cpp/src/ray/runtime/task/task_executor.cc:97-100`），
///   `ExecuteTask` 见到该状态直接返回、不写 meta buffer（同文件 209-211），actor
///   因此被关闭、不会被 `max_restarts` 重启。
/// - `RAY_EXIT_OUTSIDE_ACTOR_SENTINEL`：在任务或 actor 构造函数里调用了
///   `exit_actor()`。C 侧抛 `ray::internal::RayException`，`GetExecuteResult` 的
///   `catch (const std::exception &)` 把它变成该次调用的失败。
///
/// 本地模式（`ray::RayConfig::local_mode`）不会走到这两个哨兵：本地模式没有 core worker，
/// 退出语义没有承接方，Rust 侧的 `exit_actor()` 在那里是空操作（不置标志），见
/// `crates/rayrust/src/actor_exit.rs` 的模块文档。
///
/// 必须与 `crates/rayrust-sys/src/lib.rs` 的 `RAY_INTENTIONAL_EXIT_SENTINEL` 与
/// `RAY_EXIT_OUTSIDE_ACTOR_SENTINEL` 一致。
#define RAY_INTENTIONAL_EXIT_SENTINEL ((size_t)-1)
#define RAY_EXIT_OUTSIDE_ACTOR_SENTINEL ((size_t)-1 - 1)

// ─── Lifecycle ────────────────────────────────────────────────

/// Initialize Ray runtime.
/// `local_mode` alone decides the run mode: non-zero builds `LocalModeRayRuntime`
///   (cpp/src/ray/runtime/abstract_ray_runtime.cc:48-51), zero joins a cluster.
///   `address` never selects the mode.
/// `address` is the "host:port" of the head node (e.g. "192.168.42.141:6379").
/// In local mode it is ignored entirely: it is not handed to the SDK and not
///   validated, whatever the string is.
/// In cluster mode an empty or NULL address means no bootstrap address is set,
///   so the SDK looks at the `RAY_ADDRESS` environment variable and, if that is
///   unset too, starts a head node itself
///   (cpp/src/ray/config_internal.cc:207-215, cpp/src/ray/util/process_helper.cc:80-92).
/// In cluster mode a non-empty address must be `host:port`: a bracketed IPv6
///   host, a non-empty host, and a decimal port in 0..=65535. An address that does
///   not match, and a `RAY_ADDRESS` that does not match while `address` is empty
///   (an empty value counts as set, and is refused the same way), both return -1
///   with the reason in ray_last_error() before the SDK is entered.
/// What upstream did with each shape this entry refuses: the ones `ParseAddress`
///   cannot split (no `:`, an IPv6 host without brackets) and the ones whose port
///   is empty or does not start with decimal digits stopped the process in the
///   RAY_CHECK of `ConfigInternal::SetBootstrapAddress`
///   (cpp/src/ray/config_internal.cc:255, :261), which RAY_CATCH cannot catch; a
///   port above 0..=65535 and a port whose suffix `std::from_chars` drops went on
///   to the GCS and stopped it about 35 seconds later at
///   cpp/src/ray/util/process_helper.cc:110; a negative port stopped it at
///   cpp/src/ray/util/util.cc:42; and an empty host is not an empty address: it
///   made upstream take the bootstrap address as unset and start a head node with
///   the port it split off (cpp/src/ray/util/process_helper.cc:84-92), which is why
///   this entry refuses it.
/// `node_ip` is the IP address of this node as seen by the cluster.
///   Pass NULL to auto-detect.
/// `code_search_path` is a colon-separated list of directories or .so files
///   for the worker to search for remote functions. Pass NULL to skip.
/// `runtime_env_json` is a JSON string for runtime_env (e.g. {"pip": ["pkg"]}).
///   Pass NULL to skip.
/// `log_dir` is the directory for Ray logs. Pass NULL for default.
/// `actor_lifetime` is 0 = NON_DETACHED (default), 1 = DETACHED.
///   Detached actors outlive the job that created them.
/// `namespace` is the job-level namespace for named actors.
///   Pass NULL or empty string for default (no namespace).
/// Returns 0 on success, -1 on failure.
/// The address checks run before the shutdown check: when an address is refused,
/// ray_last_error() carries that message and not
/// `a ray_shutdown is still in progress`.
/// A local-mode shutdown that is still running reports -1 with
/// `a ray_shutdown is still in progress` in ray_last_error(), and
/// leaves the runtime and the bookkeeping of the running shutdown untouched.
/// A call while Ray is already initialized is a no-op that keeps the pending
/// local-mode task list: the runtime does not change, so the tasks still have to
/// be waited for on shutdown. The address checks still run first, so a malformed
/// address returns -1 there even though `ray::Init` itself does nothing at that
/// point (cpp/src/ray/api.cc:25-31).
int ray_init(const char *address, int local_mode, const char *node_ip,
             const char *code_search_path,
             const char *runtime_env_json,
             const char *log_dir,
             int actor_lifetime,
             const char *ns);

/// Returns true if ray::Init has been called.
bool ray_is_initialized(void);

/// Shutdown Ray runtime.
///
/// In local mode, waits for every task submitted through `ray_task_call_with_options`
/// and `ray_task_call_python` during the current runtime to become ready before
/// closing the reference-count gate and calling `ray::Shutdown()`. Once shutdown
/// begins, new local-mode task submissions are rejected with an error, and they
/// keep being rejected after the shutdown has finished until a new `ray_init`
/// establishes a runtime; `ray_init` itself reports -1 until the shutdown has
/// finished. If waiting fails, prints
/// the reason to `stderr` and aborts the process, because this `extern "C"` entry
/// has no error channel and letting the runtime be destroyed while tasks are
/// still running causes use-after-free on `object_store_`.
/// Concurrent calls perform the shutdown once: the first call takes the snapshot
/// and waits, every later call finds the shutdown in progress and returns
/// immediately without waiting or touching Ray. Cluster mode does not wait for
/// submitted tasks.
void ray_shutdown(void);

// ─── Object Store ─────────────────────────────────────────────

/// Put a serialized object into the object store.
/// `data` is msgpack-serialized bytes.
/// Returns a ray_bytes_t containing the binary object ID.
/// Caller must free with ray_free_bytes.
ray_bytes_t ray_put(const char *data, size_t len);

/// Get a single object from the object store.
/// `id_data`/`id_len` is the binary object ID and must be exactly 28 bytes (the
/// Ray 2.51.1 ObjectID length). Any other length, including 0, is rejected
/// without touching Ray: `ObjectID::FromBinary` RAY_CHECKs a non-empty length
/// mismatch and terminates the process (`src/ray/common/id.h:430-441`), while an
/// empty id becomes the nil id, which is never ready (`get` then reports
/// TimedOut).
/// `timeout_ms` is -1 for infinite wait.
/// Returns a ray_bytes_t containing the serialized object data.
/// On error, returns ray_bytes_t with data=NULL.
ray_bytes_t ray_get(const char *id_data, size_t id_len, int timeout_ms);

/// Add one local reference to an object ID.
/// `id_len` must be exactly 28 bytes (the Ray 2.51.1 ObjectID length); any
/// other length is rejected without touching Ray. `ObjectID::FromBinary`
/// terminates the process on a non-empty length mismatch, and returns the nil
/// ID for an empty string (src/ray/common/id.h:430-440).
/// Returns 0 on success, -1 on error with the message in ray_last_error().
/// Once `ray_shutdown` has started the call returns 0 without touching Ray:
/// the core worker is going away, so there is no count left to change.
int ray_add_local_reference(const char *id_data, size_t id_len);

/// Remove one local reference from an object ID.
/// Same length requirement and return value as ray_add_local_reference.
/// Removing a reference the core worker does not know about only logs a
/// warning; it does not fail.
int ray_remove_local_reference(const char *id_data, size_t id_len);

/// Wait for objects to be locally available.
/// `ids` is an array of ray_bytes_t (binary object IDs), each exactly 28 bytes
/// (the Ray 2.51.1 ObjectID length). Any other length, including 0, is rejected
/// without touching Ray for the same reasons as ray_get; an empty id would
/// become the nil id and wait as an object that never becomes ready.
/// `count` is the number of IDs.
/// `num_objects` is the minimum number to wait for. It must be between 1 and
/// `count`, and no larger than INT_MAX: Ray's `Wait` takes an `int`, so values
/// above INT_MAX could not be handed on. Anything else is rejected without
/// touching Ray.
/// The IDs must be pairwise distinct. Ray's own `CoreWorker::Wait` rejects a
/// count outside 1..=count and repeated ids whether or not the objects are ready
/// (src/ray/core_worker/core_worker.cc:1475-1483); the wrapper applies the same
/// rules in both modes, so local mode and cluster mode report the same text. In
/// local mode the ids are deduplicated while `num_objects` is passed on unchanged
/// (cpp/src/ray/runtime/object/local_mode_object_store.cc:82-97); a count above the
/// deduplicated set size ends the process in the RAY_CHECK of
/// src/ray/core_worker/store_provider/memory_store/memory_store.cc:89 only when at
/// least one deduplicated id is not in the local store yet. With every id already
/// there, the store returns Status::OK() before it builds a GetRequest, so the same
/// input answers Ok instead of ending the process.
/// Both rejections carry the sentence Ray's own `CoreWorker::Wait` produces
/// (src/ray/core_worker/core_worker.cc:1475-1483).
/// `timeout_ms` is -1 for infinite wait.
/// Returns a heap-allocated bool array (one per input ID).
/// Caller must free with ray_free_bools.
bool *ray_wait(const ray_bytes_t *ids, size_t count, size_t num_objects, int timeout_ms);

// ─── Task ─────────────────────────────────────────────────────

/// Call a remote task with full CallOptions.
/// `func_name` is a null-terminated C string (function names are ASCII).
/// `args` is an array of msgpack-serialized arguments.
/// `is_ref` is an array of booleans (same length as args). If is_ref[i] is true,
///   args[i] is treated as a binary ObjectRef ID (pass by reference).
///   Pass NULL to treat all args as values. Any other length is rejected by the
///   Rust side; this function itself does not know arg_count's relation to it.
///   A reference argument must be exactly 28 bytes (the Ray 2.51.1 ObjectID
///   length); any other length is rejected before the TaskArg is built, with the
///   argument index in ray_last_error(). A length mismatch would hit a RAY_CHECK
///   in `ObjectID::FromBinary` (`src/ray/common/id.h:430-441`), and in local mode
///   an empty id waits forever in the undeadlined `runtime->Get(id)` that
///   `build_task_args_cpp` performs.
///   An id passed here must be a reference the process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process.
/// `options_json` is a JSON string with any subset of fields:
///   {
///     "name": "my_task",                  // task name
///     "resources": {"CPU":1,"GPU":0.5},  // resource requirements
///     "placement_group_id": "a1b2c3...",  // hex-encoded binary placement group ID
///     "bundle_index": 0,                  // bundle index (default -1 = any bundle)
///     "runtime_env": "{\"env_vars\":{...}}"  // runtime env object JSON, as a
///                                             // JSON string value
///   }
/// Every field is validated; a bad value returns an error (data=NULL) instead of
/// falling back to the default scheduling:
/// - `placement_group_id` must decode to exactly 18 bytes: an empty string, an
///   odd number of hex characters, a non-hex character or any other length is
///   an error, and so is an id that names no existing group.
/// - `bundle_index` must be -1 or between 0 and 2147483647 (a smaller value
///   would hit a RAY_CHECK in the SDK and abort the process; anything outside
///   `int` is rejected instead of being truncated to its low 32 bits), and it
///   requires `placement_group_id`.
/// - `runtime_env` must be a JSON string holding a single JSON object; a
///   non-string, malformed JSON or a non-object is an error.
/// `resources` carries Ray's task default: when the map has no `CPU` key, 1.0 is
/// added (the Python `@ray.remote` default), so `--num-cpus` limits how many
/// tasks run at once. An explicit `"CPU":0` is the only way to ask for zero CPU;
/// a map with other resources and no `CPU` still gets 1.0 merged in.
/// Local mode rejects `placement_group_id` outright (the SDK has no placement
/// group lookup there) and drops the other three fields: LocalModeTaskSubmitter
/// calls `Submit(invocation, {})`, so name, resources and runtime_env have no
/// effect.
/// Pass NULL or empty string for no options (all defaults).
/// Returns a ray_bytes_t containing the binary object ID of the result.
/// On error, returns ray_bytes_t with data=NULL.
ray_bytes_t ray_task_call_with_options(const char *func_name,
                                         const ray_bytes_t *args,
                                         size_t arg_count,
                                         const bool *is_ref,
                                         const char *options_json);

/// Call a Python remote function.
/// This entry point takes no options JSON: a Python task submitted here cannot be
/// named, cannot request resources, cannot be bound to a placement group bundle
/// and cannot be given a runtime env.
/// The task requests one CPU: with no options there is no way to opt out of the
/// default 1.0 that `ray_task_call_with_options` applies.
/// `module_name` and `function_name` are null-terminated C strings.
/// `args` are msgpack-serialized arguments (will be wrapped with xlang header).
/// `is_ref[i] = true` means args[i] is an ObjectRef ID (pass by reference).
///   Such an id must be exactly 28 bytes, the same requirement and the same
///   rejection as ray_task_call_with_options.
///   Such an id must be a reference the calling process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process. It must also come
///   from a Python worker; objects put through this SDK carry no metadata and
///   Python rejects them as an argument.
/// Local mode returns an error instead of submitting when a `#[rayrust::remote]`
/// function is registered under `function_name`: `LocalModeTaskSubmitter::Submit`
/// drops the module and the language and resolves the function name in the C++
/// remote function map
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40`), so that Rust
/// function would run in place of the Python one. A local submission under any
/// other name — including a method of a `#[rayrust::actor]` type, which registers
/// in the member function map — is submitted as a C++ task and fails at the C++
/// function lookup.
ray_bytes_t ray_task_call_python(const char *module_name,
                                  const char *function_name,
                                  const ray_bytes_t *args,
                                  size_t arg_count,
                                  const bool *is_ref);

// ─── Actor ────────────────────────────────────────────────────

/// Create an actor with full ActorCreationOptions.
/// `options_json` is a JSON string with any subset of fields:
///   {
///     "name": "my_actor",              // named actor
///     "ray_namespace": "my_ns",        // namespace for named actor lookup
///     "resources": {"CPU":1,"GPU":1},  // resource requirements
///     "max_restarts": 3,               // restart on failure (default 0)
///     "max_concurrency": 10,           // max concurrent method calls (default 1)
///     "serialized_runtime_env_info": "...",  // runtime env info string
///     "placement_group_id": "a1b2c3...", // hex-encoded binary placement group ID
///     "bundle_index": 0                // bundle index within the placement group
///   }
/// Pass NULL or empty string for no options (all defaults).
/// The placement group fields are validated before they reach the SDK: a
/// `placement_group_id` that is empty, not hex, or not exactly 18 bytes, a
/// `bundle_index` outside -1 .. 2147483647 (including values that do not fit in
/// an `int`, which are rejected rather than truncated) or without a group, a
/// group id that names no existing group, and any group in local mode all return
/// an error with RAY_ERROR_KIND_GENERIC instead of letting the SDK's RAY_CHECK
/// terminate the process. A group without a `bundle_index` key means -1 (any
/// bundle).
/// Constructor arguments are always values: there is no `is_ref` here.
/// `func_name` must be a non-empty name. An empty name — and a name with an
/// interior NUL byte, which reaches this entry as an empty C string — is refused
/// here with RAY_ERROR_KIND_GENERIC before the runtime is looked up and before any
/// task is submitted: every name a `#[rayrust::actor]` type registers is non-empty,
/// and an empty one can never run. Local mode constructs only a driver
/// (`cpp/src/ray/runtime/local_mode_ray_runtime.cc:30-34`), runs the creation task
/// in the calling thread before `CreateActor` returns
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:115-121`, `:143-151`) and
/// throws `Task function name is empty` before any lookup
/// (`cpp/src/ray/runtime/task/task_executor.cc:32-34`); the error text goes into the
/// creation task's return value and no actor context is registered (same file,
/// `:336-339`, so the branch at `:327-331` is skipped), and the first call throws a
/// bare `_Map_base::at` from `actor_contexts_.at`
/// (`local_mode_task_submitter.cc:110-112`). Cluster mode returns as soon as the
/// creation task is submitted, and the worker fails it with `RayFunctionNotFound`
/// (`cpp/src/ray/util/function_helper.cc:163-168`, caught at
/// `task_executor.cc:101-111`): the actor worker stops at
/// `src/ray/core_worker/core_worker_shutdown_executor.cc:40` and the driver reads
/// only `Actor error ... unknown FFI error`. A whitespace-only name is not trimmed
/// and is submitted as it is, as `ray_get_actor` does; a non-empty
/// name nobody registered keeps Ray's asynchronous semantics, because the driver has
/// no way to learn which names are registered.
ray_bytes_t ray_actor_create_with_options(const char *func_name,
                                            const ray_bytes_t *args,
                                            size_t arg_count,
                                            const char *options_json);

/// Create a Python actor with full ActorCreationOptions.
/// `options_json` has the same format as ray_actor_create_with_options.
/// Constructor arguments are always values: there is no `is_ref` here.
/// `module_name` and `class_name` must be non-empty. A Python actor is named by
/// both, and the worker resolves them in Python, so either one empty hands out
/// an actor that never runs: the driver only submits the creation task and the
/// caller gets `Ok` with a 16-byte id whose first read answers only
/// `Actor error ... unknown FFI error`. An empty module name fails in
/// `python/ray/_private/function_manager.py:531` (`load_actor_class`) → `:589`
/// (`_load_actor_class_from_local`) → `:138`
/// (`load_function_or_class_from_local`), where
/// `importlib.import_module(module_name)` raises `ValueError: Empty module name`;
/// an empty class name fails at `python/ray/_raylet.pyx:2253`, which raises
/// `TypeError: module.__new__(X): X is not a type object (module)` before
/// `load_actor_class` is reached (the measured traceback holds only
/// `_raylet.pyx:2432` and `:2253`). In both cases the worker exits with
/// `SYSTEM_ERROR`. A name with an interior NUL byte reaches this entry as an
/// empty C string as well (`crates/rayrust-sys/src/lib.rs:253-255`, #27).
/// The two name checks run right after `ray_clear_error()` and before the runtime
/// lookup, so a refused call builds no runtime and submits nothing, and they run
/// before the local mode rejection below, so local mode with an empty name
/// reports the name error instead.
/// Local mode returns an error instead of creating the actor, and it does so
/// before the options JSON is parsed and before the arguments are built, so in
/// local mode this entry rejects every call with non-empty names whatever the
/// options and the arguments are. `ray_actor_create_with_options` (a C++ actor)
/// keeps its option diagnostics in local mode.
/// The reason is the local submitter itself: local mode starts no core worker
/// process and has no Python executor, and `LocalModeTaskSubmitter::Submit`
/// ignores the language and the module and builds every task as a C++ descriptor
/// from the function name alone (`local_mode_task_submitter.cc:39-40`, language
/// CPP at `:52-56`). The creation function name this entry puts in the holder is
/// `"__init__"`, which the C++ `FunctionManager` usually has no entry for, so
/// without the refusal `TaskExecutor` would store
/// `PackError("unknown function: __init__")` as the actor state
/// (`task_executor.cc:44-52`, `:313-331`) without throwing and without returning
/// an error, and the caller would see a successful creation. A same-named
/// function registered in the process would run in place of the Python
/// constructor, and a named creation would write the name into the local name
/// table (`local_mode_task_submitter.cc:143-152`), which `ray_get_actor` reads
/// back (`:159-168`) to hand out a handle nothing can call:
/// `ray_actor_call_python` rejects every Python actor call in local mode.
ray_bytes_t ray_actor_create_python_with_options(const char *module_name,
                                                   const char *class_name,
                                                   const ray_bytes_t *args,
                                                   size_t arg_count,
                                                   const char *options_json);

/// Call a method on an actor.
/// `actor_id_data`/`actor_id_len` is the binary actor ID and must be exactly 16
/// bytes (the Ray 2.51.1 ActorID length, `src/ray/common/id.h:124-142`); any
/// other length is rejected without touching Ray, because `ActorID::FromBinary`
/// RAY_CHECKs the length (`src/ray/common/id.h:430-441`).
/// Beyond the length there is no registration check here: the core worker's
/// `ActorManager::GetActorHandle` terminates the process on an id it holds no
/// handle for (`src/ray/core_worker/actor_manager.cc:52-59`), and the actor
/// manager is reachable only through the core worker. The `rayrust` crate keeps
/// its own registry of the handles it handed out and checks it before calling
/// this entry, so a driver through that crate never reaches the RAY_CHECK.
/// The method call this entry builds is a C++ one (hence the language note in
/// `rayrust`'s `Ray::actor_call`).
/// `is_ref` is an array of booleans (same length as args). If is_ref[i] is true,
///   args[i] is treated as a binary ObjectRef ID (pass by reference).
///   Pass NULL to treat all args as values. Any other length is rejected by the
///   Rust side. A reference argument must be exactly 28 bytes, the same
///   requirement and the same rejection as ray_task_call_with_options. The same
///   owner requirement applies as well.
/// Returns a ray_bytes_t containing the binary object ID of the result.
ray_bytes_t ray_actor_call(const char *actor_id_data, size_t actor_id_len,
                            const char *func_name,
                            const ray_bytes_t *args,
                            size_t arg_count,
                            const bool *is_ref);

/// Call a method on a Python actor.
/// `actor_id_data`/`actor_id_len` must be exactly 16 bytes, the same requirement
/// and the same rejection as ray_actor_call. The width check runs before the
/// local mode rejection below, so local mode with a short actor id reports the
/// width error, in the same order as ray_cancel.
/// As in ray_actor_call there is no registration check here: the core worker's
/// `ActorManager::GetActorHandle` terminates the process on an unknown id
/// (`src/ray/core_worker/actor_manager.cc:52-59`) and the actor manager is
/// reachable only through the core worker. `rayrust`'s `Ray::actor_call_python`
/// checks its own registry first. That registry also records each handle's
/// language (`rpc.ActorHandle.actor_language`), which this entry does not look
/// at either: an id of a C++ actor reaches `AbstractRayRuntime::CallActor`, whose
/// PYTHON branch reads the creation descriptor as a `PythonFunctionDescriptor`
/// (`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`), and the C++ worker
/// then ends at `RAY_CHECK(ray_function.GetLanguage() == ray::Language::CPP)`
/// (`cpp/src/ray/runtime/task/task_executor.cc:145`). `rayrust` refuses a
/// non-Python id before this entry.
/// `method_name` is the Python method name (without `self`).
/// `is_ref[i] = true` means args[i] is an ObjectRef ID (pass by reference).
///   Such an id must be exactly 28 bytes, the same requirement and the same
///   rejection as ray_task_call_with_options.
///   It must also be a reference the calling process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process. It must also come
///   from a Python worker; objects put through this SDK carry no metadata and
///   Python rejects them as an argument.
/// Local mode returns an error without reaching the core worker:
/// `AbstractRayRuntime::CallActor` reads the actor's module and class from the
/// core worker's actor handle (`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`),
/// and that lookup takes `CoreWorkerProcess::GetCoreWorker()`, whose
/// `EnsureInitialized` calls `QuickExit()` (`_Exit(1)`,
/// `src/ray/util/process.cc:845-848`) when no core worker exists
/// (`src/ray/core_worker/core_worker_process.cc:842-850`). The C ABI receives only
/// the actor id and the method name, and `LocalModeTaskSubmitter::Submit` builds
/// every task as a C++ descriptor (`local_mode_task_submitter.cc:39-55`), so local
/// mode has nothing to run a Python actor method with either.
ray_bytes_t ray_actor_call_python(const char *actor_id_data, size_t actor_id_len,
                                    const char *method_name,
                                    const ray_bytes_t *args,
                                    size_t arg_count,
                                    const bool *is_ref);

/// Kill an actor.
/// `actor_id_data`/`actor_id_len` is the binary actor ID and must be exactly 16
/// bytes, the same requirement and the same rejection as ray_actor_call. The
/// width check runs before the local mode rejection below, so local mode with a
/// short actor id reports the width error, in the same order as ray_cancel.
/// Returns nothing: an error is reported through ray_last_error().
/// Local mode returns an error: `AbstractRayRuntime::KillActor` starts with
/// `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc:845-848`) because local
/// mode never starts a core worker process. `LocalModeRayRuntime` does not
/// override `KillActor`, and a local actor lives in the calling process
/// (`LocalModeTaskSubmitter::actor_contexts_`), so there is nothing to kill.
void ray_actor_kill(const char *actor_id_data, size_t actor_id_len, bool no_restart);

/// Serialize a registered actor handle.
/// `actor_id_data`/`actor_id_len` must be exactly 16 bytes (the Ray 2.51.1
/// ActorID length, `src/ray/common/id.h:124-142`); any other length is rejected
/// without touching Ray, because `ActorID::FromBinary` RAY_CHECKs the length
/// (`src/ray/common/id.h:430-441`).
/// The id must belong to a handle this process registered. The SDK's
/// `ActorManager::GetActorHandle` RAY_CHECKs an unknown id and terminates the
/// process (`src/ray/core_worker/actor_manager.cc:52-59`), so the Rust side only
/// exposes this on handles obtained from creation, `ray_get_actor` or
/// `ray_actor_handle_deserialize`, and it checks its own registry before calling
/// this entry: an id the registry does not hold is refused there, and a handle
/// held across a shutdown and re-init of the runtime reports that refusal
/// instead of reaching the RAY_CHECK here.
/// Local mode returns an error: `AbstractRayRuntime::SerializeActorHandle` starts
/// with `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc`) because local mode never
/// starts a core worker process.
/// Returns the serialized handle bytes, or data=NULL with last_error set.
ray_bytes_t ray_actor_handle_serialize(const char *actor_id_data, size_t actor_id_len);

/// Register an actor handle from its serialized bytes and return its binary
/// actor id (16 bytes).
/// Empty input is rejected without touching Ray.
///
/// The bytes must be a well-formed `rpc::ActorHandle` protobuf
/// (`src/ray/protobuf/core_worker.proto:27-79`) whose `actor_id` (field 1) is
/// exactly 16 bytes and whose `creation_job_id` (field 4) is exactly 4 bytes.
/// The wheel ships no `core_worker.pb.h`, so the payload is walked here over the
/// protobuf wire format against the Ray 2.51.1 schema: nested messages
/// (`owner_address`, `actor_creation_task_function_descriptor`, the `labels`
/// entries) are validated recursively and proto3 `string` fields by their UTF-8
/// content, while unknown fields — and fields whose wire type does not match the
/// schema — are skipped by their wire type, the way upstream treats them. A
/// field that appears more than once counts by its last occurrence, as protobuf
/// does.
/// The nested and string validation is what keeps the two lengths reliable:
/// upstream's `ParseFromString` stops at the first damaged nested message or
/// invalid `string`, `CreateInnerActorHandleFromString` discards that result
/// (`src/ray/core_worker/actor_handle.cc:66`), and the fields written before the
/// stop are what gets registered. A three-byte `actor_id` followed by a damaged
/// `owner_address` and then a 16-byte `actor_id` therefore registers the
/// three-byte one and ends the process at `id.h:435`; a `creation_job_id` behind
/// damaged content is never written and the handle registers with a nil job id.
/// Both are rejected here. The recursion follows the schema tree, which is three
/// levels deep (`rpc.ActorHandle` → `rpc.FunctionDescriptor` →
/// `rpc.PythonFunctionDescriptor`); a walk that goes past eight levels is rejected
/// as malformed, so no crafted payload can drive it deeper.
/// Field keys and length prefixes must be minimally encoded and must fit in 32
/// bits. Upstream reads tags as `uint32` and length prefixes as `int32`: a
/// five-byte key above 32 bits is truncated into a different field
/// (`0x10000000a` becomes `actor_id`, and a short payload there ends the
/// process), a key longer than five bytes or a length at or above 2^31 makes its
/// `ParseFromString` fail, and a length varint longer than five bytes fails as
/// well. Non-minimal encodings within five bytes are accepted upstream and
/// rejected here. Protobuf's own serialization produces none of these, so
/// rejecting them cannot refuse a handle `serialize()` handed out.
///
/// The table above mirrors Ray 2.51.1. A release that adds fields needs nothing
/// (both sides treat them as unknown), while one that *changes* an existing
/// field's type would need the table updated, or damaged content of the new type
/// would be skipped again.
/// The two lengths are checked here because upstream reads them as fixed-size
/// ids and RAY_CHECKs the size, which `RAY_CATCH` cannot catch: `actor_id` at
/// registration (`ActorManager::RegisterActorHandle`,
/// `src/ray/core_worker/actor_manager.cc:33` → `ActorID::FromBinary`,
/// `src/ray/common/id.h:430-441`) and `creation_job_id` on the first call
/// (`CoreWorker::SubmitActorTask`, `src/ray/core_worker/core_worker.cc:2374` →
/// `JobID::FromBinary`, `JobID::kLength` at `src/ray/common/id.h:105`). A
/// payload without `creation_job_id` is accepted by upstream, and its nil job id
/// ends the actor's worker at `WorkerContext::SetCurrentTask`
/// (`src/ray/core_worker/context.cc:295`) on the first call.
///
/// Bit for bit, a payload that upstream would register with a broken handle is
/// rejected here: `ParseFromString` discards its result, so valid fields followed
/// by damaged bytes used to register fine, while this walk reports them as
/// malformed. The walk is stricter than upstream in exactly these encodings: wire
/// types 3/4/6/7, and field keys or length prefixes that are not minimally
/// encoded or exceed 32 bits. Protobuf's own serialization produces none of them,
/// so rejecting them cannot refuse a handle `serialize()` handed out.
///
/// The payload check runs before the runtime and local-mode checks: a payload
/// error is reported in both modes, and only a well-formed payload reaches the
/// local-mode error below.
///
/// The payload check only guarantees the lengths. Upstream reads `actor_id` with
/// `ActorID::FromBinary`, which rejects an empty or wrong-sized input and keeps
/// 16 bytes as they are: 16 bytes of 0xff are the nil id, and 16 bytes of 0x00
/// belong to no actor (a real id is a hash). Both are rejected here after
/// registration, because such a handle has no clear failure in the caller's
/// hands: its calls are submitted but never become ready (`get` waits out its
/// deadline), and `serialize()` hands the registered protobuf back — the same
/// length as the bytes that were registered, with protobuf re-serialization free
/// to rewrite a length prefix — or an error when re-serializing produces nothing,
/// so a bad handle would keep spreading.
///
/// Local mode returns an error: `DeserializeAndRegisterActorHandle` reads
/// `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc`) because local mode never
/// starts a core worker process.
///
/// `out_meta` must be a non-null pointer: a null one is rejected without touching
/// Ray. Once the payload checks out and the handle is registered, it receives the
/// handle's language and creation descriptor type. The language comes from
/// `actor_language` (field 5, `src/ray/protobuf/core_worker.proto:42`):
/// `PYTHON = 0`, `JAVA = 1`, `CPP = 2` (`src/ray/protobuf/common.proto:26-30`). A
/// payload without field 5 yields `PYTHON` (0), because proto3 omits fields at
/// their default value and `PYTHON` is 0 — the same language upstream parses out
/// of those bytes. The creation descriptor type is the field number of the oneof
/// member in `actor_creation_task_function_descriptor` (field 6,
/// `src/ray/protobuf/common.proto:158-164`): 0 when field 6 is absent, 1 java,
/// 2 python, 3 cpp. Both are handed out because the registry the Rust side keeps
/// needs them: the language decides which call entry accepts the handle, and the
/// creation descriptor type decides whether the Python entry may run at all —
/// that path reads the descriptor as a `PythonFunctionDescriptor`
/// (`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`).
ray_bytes_t ray_actor_handle_deserialize(const char *serialized_data,
                                         size_t serialized_len,
                                         ray_actor_handle_meta_t *out_meta);

// ─── Placement Group ──────────────────────────────────────────

/// Create a placement group.
/// `name` is a null-terminated string (can be NULL).
/// `bundles_json` is a JSON array of bundle objects.
/// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
/// Returns a ray_bytes_t containing the group ID.
ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy);

/// Remove a placement group by ID.
/// `group_id_data`/`group_id_len` must be exactly 18 bytes
/// (`PlacementGroupID::kLength`); any other length, a null pointer and an empty
/// length are rejected without touching Ray, because
/// `PlacementGroupID::FromBinary` RAY_CHECKs the length (`src/ray/common/id.h:435`)
/// and that check ends the process instead of throwing.
/// Returns 0 on success, -1 on error with the message in ray_last_error().
/// An id that names no group, and one that was already removed, both return 0:
/// upstream treats them as "already removed" and does nothing
/// (`src/ray/gcs/gcs_placement_group_manager.cc:404-408` answers `Status::OK()`
/// from the GCS side, `cpp/src/ray/runtime/task/local_mode_task_submitter.cc:182-184`
/// erases from a map that does not hold the id). Measured in both modes.
/// A -1 comes from the SDK only when the call itself fails:
/// `NativeTaskSubmitter::RemovePlacementGroup` turns a non-OK status of
/// `CoreWorker::RemovePlacementGroup` into `RayException`
/// (`cpp/src/ray/runtime/task/native_task_submitter.cc:219-226`), and that status is
/// the GCS RPC result, whose timeout is rewritten into a message carrying the id
/// (`src/ray/core_worker/core_worker.cc:2293-2306`).
/// Local mode is not rejected: `LocalModeTaskSubmitter::CreatePlacementGroup` inserts
/// the group into its own map with `PlacementGroupID::Of(job_id)`
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:170-180`) and removes it by
/// erasing from that map (same file, `:182-184`). Nothing in local mode reads that map
/// — the three queries throw, `wait_ready` falls back to the interface default, and
/// tasks and actors drop the group binding — so the erase has no observable effect and
/// `Ok(())` is the whole result. `wait_ready` is rejected instead because its default
/// answers `ready` for any id, which is a false answer a caller can see.
/// The crate wraps this entry in `Ray::placement_group_remove`, which returns
/// `Result<(), RayError>`: it checks the width a second time before the call and
/// turns a -1 into an error carrying this message.
int ray_placement_group_remove(const char *group_id_data, size_t group_id_len);

/// Wait for a placement group to be ready.
/// `group_id_data`/`group_id_len` must be exactly 18 bytes
/// (`PlacementGroupID::kLength`); any other length is rejected without touching
/// Ray, because `PlacementGroupID::FromBinary` RAY_CHECKs the length
/// (`src/ray/common/id.h:430-441`). Empty is rejected for the same reason the
/// other entries reject it: the nil id names no group.
/// `timeout_seconds` is passed to GCS as-is and becomes the RPC deadline: a
/// positive value waits that long, `0` sets the deadline to now, a negative value
/// puts it in the past. Measured on an already-ready group (three runs, same
/// result): `0` still returns 1 because the reply arrives within the millisecond,
/// while a **negative value returns 0 without looking at the group**, even when
/// the group is ready. An unready group returns 0 when the deadline passes.
/// Returns 1 = ready, 0 = not ready, -1 = error with the message in
/// ray_last_error().
/// Local mode returns -1: `LocalModeTaskSubmitter` does not override the
/// interface default `return true` (`cpp/src/ray/runtime/task/task_submitter.h:49-52`),
/// so the SDK would answer "ready" without waiting for anything.
int ray_placement_group_wait_ready(const char *group_id_data, size_t group_id_len,
                                   int timeout_seconds);

/// Query placement groups. Each returns a msgpack array of groups, where a group
/// is a 4-element array [id (bin), name (str), state (int), strategy (int)].
/// - `ray_placement_group_get_all`: every row in the GCS table, including the
///   ones whose state is REMOVED. GCS rewrites the row to REMOVED instead of
///   deleting it (`src/ray/gcs/gcs_placement_group_manager.cc:458-469`), so "all"
///   is not "all live" and the caller has to read `state`.
/// - `ray_placement_group_get_by_id`: a removed group still comes back with
///   `state == 3`; an empty array means the id never named a group.
/// - `ray_placement_group_get_by_name`: looked up in the current job's namespace
///   only (`AbstractRayRuntime::GetPlacementGroup` passes
///   `GetJobConfig().ray_namespace()`), so a group created in another namespace
///   reads as "no such group". The name mapping is erased when the group is
///   removed (`gcs_placement_group_manager.cc:414-427`), so this one starts
///   returning an empty array before `_by_id` does.
///
/// `state` and `strategy` keep the SDK's integer encoding
/// (`cpp/include/ray/api/task_options.h:44-58`): PlacementGroupState PENDING 0,
/// PREPARED 1, CREATED 2, REMOVED 3, RESCHEDULING 4, UNRECOGNIZED -1;
/// PlacementStrategy PACK 0, SPREAD 1, STRICT_PACK 2, STRICT_SPREAD 3.
/// Bundles are not returned: `AbstractRayRuntime::GeneratePlacementGroup` builds
/// an empty bundle vector and copies from it
/// (`cpp/src/ray/runtime/abstract_ray_runtime.cc:326-340`), so `GetBundles()` is
/// always empty.
///
/// `_by_id` requires exactly 18 bytes, like ray_placement_group_wait_ready.
/// All three return data=NULL with last_error set on error, and all three fail in
/// local mode: `global_state_accessor_` is null there and these three read it
/// directly (`cpp/src/ray/runtime/abstract_ray_runtime.cc:342-370`).
ray_bytes_t ray_placement_group_get_all(void);
ray_bytes_t ray_placement_group_get_by_id(const char *group_id_data, size_t group_id_len);
ray_bytes_t ray_placement_group_get_by_name(const char *name);

// ─── Misc ─────────────────────────────────────────────────────

/// Returns true if the current actor was restarted.
///
/// The return type has no error channel, so a failure reads as `false`; the
/// reason is only visible to C callers through `ray_last_error()`. Failures are
/// a missing runtime (`"Ray runtime is not initialized"`), an SDK exception
/// (its `what()`), and a non-`std::exception` throw (`"unknown exception"`).
/// In local mode this returns `false` without touching the core worker
/// (`AbstractRayRuntime::WasCurrentActorRestarted` takes its SINGLE_PROCESS
/// branch, `cpp/src/ray/runtime/abstract_ray_runtime.cc:288-291`).
bool ray_was_current_actor_restarted(void);

/// Returns true if this process is in local mode.
/// Returns false before `ray_init`: there is no runtime to ask.
bool ray_is_local_mode(void);

/// Get the namespace of this job.
/// Returns a ray_bytes_t containing the namespace string.
///
/// Cluster mode asks the runtime, so the answer is the job config's namespace
/// (Ray generates a random one when the job configured none). Local mode never
/// starts a core worker and the runtime's `GetNamespace` calls
/// `CoreWorkerProcess::GetCoreWorker()`, which ends the process there
/// (`src/ray/core_worker/core_worker_process.cc:842-850`); local mode therefore
/// returns the namespace passed to `ray_init`, which is the empty string when
/// none was passed. An empty string comes back as a non-null buffer of length 0,
/// not as an error.
ray_bytes_t ray_get_namespace(void);

/// Get a named actor by name.
/// `name` is the actor name: a null pointer and an empty string are both rejected
/// in both modes, and the check runs before the mode decision, so local mode
/// reports the error where it used to answer `Ok(None)`. The empty name reaches
/// `CoreWorker::GetNamedActorHandle`, whose first statement is
/// `RAY_CHECK(!name.empty())` (`src/ray/core_worker/core_worker.cc:2594`), and
/// that RAY_CHECK terminates the process. A name with an interior NUL byte reads
/// as an empty C string on the Rust side, so it is rejected the same way (#27).
/// A name of only whitespace characters is legal and is not trimmed.
/// `namespace` can be NULL or an empty string for the current namespace: with an
/// empty namespace `NativeTaskSubmitter::GetActor` falls back to the job config's
/// namespace (`cpp/src/ray/runtime/task/native_task_submitter.cc:181-186`), and
/// only the name is RAY_CHECKed.
/// `out_meta` must be a non-null pointer: a null one is rejected without
/// touching Ray. A found actor writes its language and creation descriptor type
/// there, and a name that resolves to nothing leaves them alone.
/// Cluster mode reads both out of the handle's serialized bytes:
/// `runtime->SerializeActorHandle(actor_id)` returns the `rpc.ActorHandle` of the
/// handle `GetNamedActorHandle` just registered
/// (`src/ray/core_worker/actor_manager.cc:68-83`), and `actor_language` (field 5)
/// carries `PYTHON = 0`, `JAVA = 1`, `CPP = 2`
/// (`src/ray/protobuf/common.proto:26-30`) while the oneof member inside
/// `actor_creation_task_function_descriptor` (field 6) carries 1 java, 2 python,
/// 3 cpp (`src/ray/protobuf/common.proto:158-164`). `GetActorId` itself returns
/// only the 16-byte id (`cpp/src/ray/runtime/abstract_ray_runtime.cc:255-263`), so
/// the serialized bytes are the only place those two are available.
/// Local mode writes `CPP` (2) with the cpp creation descriptor (3) instead: only
/// a C++ actor can be created there (`ray_actor_create_python_with_options`
/// refuses local mode) and the local submitter runs the registered Rust functions
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40`), building every
/// task as a C++ descriptor and writing `CPP` as the task language
/// (same file, `:52-56`).
/// Returns a ray_bytes_t containing the binary actor ID, or data=NULL if not found.
ray_bytes_t ray_get_actor(const char *name, const char *ray_namespace,
                          ray_actor_handle_meta_t *out_meta);

/// Cancel a remote task by object ID.
/// `id_data` must be exactly 28 bytes (the Ray 2.51.1 ObjectID length); any
/// other length is rejected without touching Ray.
/// `force_kill` kills the worker if true. `recursive` cancels dependent tasks.
/// Returns 0 on success, -1 on error.
/// Local mode returns -1 without reaching the core worker: `CancelTask` exists
/// only on the core worker, so the cluster path goes through
/// `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc:845-848`) because local
/// mode never starts a core worker process. Local mode has no cancellation path
/// either: `LocalModeTaskSubmitter` keeps no handle to withdraw a task once it
/// is submitted.
int ray_cancel(const char *id_data, size_t id_len, bool force_kill, bool recursive);

/// Get multiple objects from the object store.
/// `ids` is an array of ray_bytes_t, each exactly 28 bytes (the Ray 2.51.1
/// ObjectID length). Any other length, including 0, is rejected without touching
/// Ray for the same reasons as ray_get. `count` is the number of IDs.
/// `timeout_ms` is -1 for infinite wait.
/// Returns a heap-allocated array of ray_bytes_t (one per input ID).
/// On error for a specific ID, that element will have data=NULL.
/// Caller must free each element with ray_free_bytes, then free the array.
ray_bytes_t *ray_get_many(const ray_bytes_t *ids, size_t count, int timeout_ms);

/// Free an array returned by ray_get_many.
void ray_free_bytes_array(ray_bytes_t *array, size_t count);

// ─── Function Registration ────────────────────────────────────

/// Callback type for a Rust remote function.
/// Receives an array of msgpack-serialized argument buffers.
/// Returns a heap-allocated ray_bytes_t containing the msgpack-serialized result.
/// The caller (C wrapper) will free the returned data with free().
typedef ray_bytes_t (*ray_func_callback_t)(const ray_bytes_t *args, size_t arg_count);

/// Register a Rust function as a Ray remote task.
/// Must be called before ray_init (or at least before the first task call).
/// `func_name` is the name used with ray_task_call_with_options.
/// `callback` is the C callback that will be invoked when the task executes.
void ray_register_function(const char *func_name, ray_func_callback_t callback);

// ─── Actor Member Function Registration ───────────────────────

/// Callback type for a Rust actor member function.
/// `actor_ptr` is the raw pointer (uint64_t) stored by the factory.
/// `args` / `arg_count` are the msgpack-serialized arguments (without `self`).
/// Returns a heap-allocated ray_bytes_t containing the msgpack-serialized result.
typedef ray_bytes_t (*ray_member_callback_t)(uint64_t actor_ptr,
                                                const ray_bytes_t *args,
                                                size_t arg_count);

/// Register a Rust actor member function.
/// `func_name` is the name used with ray_actor_call.
/// `callback` is the C callback invoked when the actor method executes.
void ray_register_member_function(const char *func_name, ray_member_callback_t callback);

// ─── Memory Management ────────────────────────────────────────

/// Free a ray_bytes_t returned by any ray_* function.
void ray_free_bytes(ray_bytes_t *ptr);

/// Free a bool array returned by ray_wait.
void ray_free_bools(bool *ptr);

// ─── Error Handling ───────────────────────────────────────────

/// Class of the last error, mirroring Ray's C++ exception hierarchy
/// (`ray/api/ray_exception.h`).
///
/// `RayFunctionNotFound` and `RayRuntimeEnvException` have no constant here:
/// neither class ever reaches this side. A missing function is thrown only on
/// the worker, where `TaskExecutor`'s catch turns it into `Status::Invalid`
/// (`cpp/src/ray/runtime/task/task_executor.cc:101-111`, the catch at `:101` and the
/// `Status::Invalid` it returns at `:111`), so the driver sees
/// a `RayTaskException`. A malformed runtime_env is deserialized before
/// `ray_init`'s `try`, so it aborts the process instead of being recorded.
typedef enum {
    /// No error recorded on this thread.
    RAY_ERROR_KIND_NONE = 0,
    /// A plain `std::exception`, or a message set directly.
    RAY_ERROR_KIND_GENERIC = 1,
    /// `ray::internal::RayException`, the base of the five below.
    RAY_ERROR_KIND_RAY = 2,
    RAY_ERROR_KIND_ACTOR = 3,
    RAY_ERROR_KIND_TASK = 4,
    RAY_ERROR_KIND_WORKER = 5,
    RAY_ERROR_KIND_UNRECONSTRUCTABLE = 6,
    RAY_ERROR_KIND_TIMEOUT = 7,
} ray_error_kind_t;

/// Get the last error message (thread-local).
/// Returns NULL if no error. The returned pointer is valid until the next
/// ray_* call on the same thread.
const char *ray_last_error(void);

/// Get the class of the last error (thread-local), one of ray_error_kind_t.
/// Returns RAY_ERROR_KIND_NONE when ray_last_error() returns NULL.
int ray_last_error_kind(void);

/// Clear the last error message and its kind.
void ray_clear_error(void);

#ifdef __cplusplus
}
#endif
