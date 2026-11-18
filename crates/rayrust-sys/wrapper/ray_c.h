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
/// `address` is "ip:port" of the head node (e.g. "192.168.42.141:6379").
/// Pass NULL or empty string for local mode.
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
int ray_init(const char *address, int local_mode, const char *node_ip,
             const char *code_search_path,
             const char *runtime_env_json,
             const char *log_dir,
             int actor_lifetime,
             const char *ns);

/// Returns true if ray::Init has been called.
bool ray_is_initialized(void);

/// Shutdown Ray runtime.
void ray_shutdown(void);

// ─── Object Store ─────────────────────────────────────────────

/// Put a serialized object into the object store.
/// `data` is msgpack-serialized bytes.
/// Returns a ray_bytes_t containing the binary object ID.
/// Caller must free with ray_free_bytes.
ray_bytes_t ray_put(const char *data, size_t len);

/// Get a single object from the object store.
/// `id_data`/`id_len` is the binary object ID.
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
/// `ids` is an array of ray_bytes_t (binary object IDs).
/// `count` is the number of IDs.
/// `num_objects` is the minimum number to wait for.
/// `timeout_ms` is -1 for infinite wait.
/// Returns a heap-allocated bool array (one per input ID).
/// Caller must free with ray_free_bools.
bool *ray_wait(const ray_bytes_t *ids, size_t count, int num_objects, int timeout_ms);

// ─── Task ─────────────────────────────────────────────────────

/// Call a remote task with full CallOptions.
/// `func_name` is a null-terminated C string (function names are ASCII).
/// `args` is an array of msgpack-serialized arguments.
/// `is_ref` is an array of booleans (same length as args). If is_ref[i] is true,
///   args[i] is treated as a binary ObjectRef ID (pass by reference).
///   Pass NULL to treat all args as values. Any other length is rejected by the
///   Rust side; this function itself does not know arg_count's relation to it.
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
///   Such an id must be a reference the calling process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process. It must also come
///   from a Python worker; objects put through this SDK carry no metadata and
///   Python rejects them as an argument.
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
ray_bytes_t ray_actor_create_with_options(const char *func_name,
                                            const ray_bytes_t *args,
                                            size_t arg_count,
                                            const char *options_json);

/// Create a Python actor with full ActorCreationOptions.
/// `options_json` has the same format as ray_actor_create_with_options.
ray_bytes_t ray_actor_create_python_with_options(const char *module_name,
                                                   const char *class_name,
                                                   const ray_bytes_t *args,
                                                   size_t arg_count,
                                                   const char *options_json);

/// Call a method on an actor.
/// `actor_id_data`/`actor_id_len` is the binary actor ID.
/// `is_ref` is an array of booleans (same length as args). If is_ref[i] is true,
///   args[i] is treated as a binary ObjectRef ID (pass by reference).
///   Pass NULL to treat all args as values. Any other length is rejected by the
///   Rust side. The same owner requirement as ray_task_call_with_options applies.
/// Returns a ray_bytes_t containing the binary object ID of the result.
ray_bytes_t ray_actor_call(const char *actor_id_data, size_t actor_id_len,
                            const char *func_name,
                            const ray_bytes_t *args,
                            size_t arg_count,
                            const bool *is_ref);

/// Call a method on a Python actor.
/// `method_name` is the Python method name (without `self`).
/// `is_ref[i] = true` means args[i] is an ObjectRef ID (pass by reference).
///   Such an id must be a reference the calling process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process. It must also come
///   from a Python worker; objects put through this SDK carry no metadata and
///   Python rejects them as an argument.
ray_bytes_t ray_actor_call_python(const char *actor_id_data, size_t actor_id_len,
                                    const char *method_name,
                                    const ray_bytes_t *args,
                                    size_t arg_count,
                                    const bool *is_ref);

/// Kill an actor.
/// `actor_id_data`/`actor_id_len` is the binary actor ID.
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
/// `ray_actor_handle_deserialize`. Measured: a handle held across a shutdown and
/// re-init of the runtime aborts the process here, the same way `ray_actor_call`
/// does for the same stale id.
/// Local mode returns an error: `AbstractRayRuntime::SerializeActorHandle` starts
/// with `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc`) because local mode never
/// starts a core worker process.
/// Returns the serialized handle bytes, or data=NULL with last_error set.
ray_bytes_t ray_actor_handle_serialize(const char *actor_id_data, size_t actor_id_len);

/// Register an actor handle from its serialized bytes and return its binary
/// actor id (16 bytes).
/// Empty input is rejected without touching Ray. Upstream
/// `ActorHandle(serialized)` discards the `ParseFromString` result
/// (`src/ray/core_worker/actor_handle.cc:66`, `:135`), so bytes that are not a
/// valid protobuf register as the nil actor and come back as 16 bytes of 0xff.
/// Such bytes are rejected here as well, and so are 16 bytes of 0x00: a real
/// actor id is a hash and never either pattern. What a nil handle does instead is
/// why: its calls are submitted but never become ready (`get` waits out its
/// deadline), and `serialize()` hands the registered protobuf back — the same
/// length as the bytes that were registered, with protobuf re-serialization free
/// to rewrite a length prefix — or an error when re-serializing produces nothing,
/// so a bad handle would keep spreading.
/// Local mode returns an error: `DeserializeAndRegisterActorHandle` reads
/// `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc`) because local mode never
/// starts a core worker process.
ray_bytes_t ray_actor_handle_deserialize(const char *serialized_data, size_t serialized_len);

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
/// `group_id_data`/`group_id_len` is the binary group ID.
void ray_placement_group_remove(const char *group_id_data, size_t group_id_len);

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
/// `name` is the actor name. `namespace` can be NULL for current namespace.
/// Returns a ray_bytes_t containing the binary actor ID, or data=NULL if not found.
ray_bytes_t ray_get_actor(const char *name, const char *ray_namespace);

/// Cancel a remote task by object ID.
/// `id_data` must be exactly 28 bytes (the Ray 2.51.1 ObjectID length); any
/// other length is rejected without touching Ray.
/// `force_kill` kills the worker if true. `recursive` cancels dependent tasks.
/// Returns 0 on success, -1 on error.
int ray_cancel(const char *id_data, size_t id_len, bool force_kill, bool recursive);

/// Get multiple objects from the object store.
/// `ids` is an array of ray_bytes_t. `count` is the number of IDs.
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
