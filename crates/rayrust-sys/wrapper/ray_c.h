/// C ABI wrapper header — bridges Ray C++ SDK templates to plain C functions.
///
/// The Ray C++ SDK uses heavy C++ templates (Put<T>, Task<F>, etc.) that have no
/// stable ABI. This header exposes a minimal C interface that the Rust FFI layer
/// can call. The C++ source (ray_c.cc) wraps `RayRuntime` directly via
/// `process_runtime()` (`GetRayRuntime()` in the driver, falling back to the
/// exported `AbstractRayRuntime::GetInstance()` inside a cdylib worker),
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

/// Rust 回调返回状态与结果结构体，配合 `ray_func_callback_t` 与
/// `ray_member_callback_t` 使用。所有非空 `data` 缓冲区必须由回调方通过
/// C 标准库 `malloc`（Rust 侧为 `libc::malloc`）分配，由 C 侧调用 `std::free` 释放。
///
/// 四个状态在集群模式与本地模式下的传播路径：
/// - `RAY_CALLBACK_OK`（0）：正常返回，`data`/`len` 为 msgpack 序列化的返回值，C 侧
///   拷入 `msgpack::sbuffer` 返回。
/// - `RAY_CALLBACK_ERROR`（1）：回调失败（参数个数不符、参数反序列化失败、空指针
///   参数或空 actor 指针、用户代码 `panic!`、actor 互斥锁中毒、返回值序列化失败），
///   `data`/`len` 为不含内部 NUL 字节的 UTF-8 错误文本；C 侧抛出
///   `ray::internal::RayException`。
///   - 集群模式下 `GetExecuteResult`（`cpp/src/ray/runtime/task/task_executor.cc:80-118`）
///     在 `:101-111` 捕获 `const std::exception &` 返回 `Status::Invalid`：普通任务与
///     actor 方法在 `ExecuteTask`（同文件 `:120-291`，`:207-237`）写入
///     `TASK_EXECUTION_EXCEPTION` 元数据，驱动在 `NativeObjectStore::GetRaw`
///     （`cpp/src/ray/runtime/object/native_object_store.cc:90-134`）经 `CheckException`
///     （同文件 `:70-88`）收到 `RayTaskException`；actor 创建任务在 `:191-193`、`:285-288`
///     使 `current_actor_` 为空，并返回空消息 `Status::CreationTaskError("")` 且未填充
///     `creation_task_exception_pb_bytes`（同文件 `:132, 286-288`），因此驱动在首次方法
///     调用的 `GetRaw` 处收到 `ACTOR_DIED` 抛出的 `RayActorException` 报告工人退出类型为
///     `USER_ERROR`（初始化异常），内层异常文本记录在工人日志（同文件 `:110`、`:216`）。
///   - 本地模式下 `TaskExecutor::Invoke`（`cpp/src/ray/runtime/task/task_executor.cc:293-341`）
///     在 `:336-340` 捕获 `std::exception &` 并调用 `PackError(e.what())`
///     （`cpp/src/ray/runtime/abstract_ray_runtime.cc:31-39`），将 msgpack `nil` 与
///     `(TASK_EXECUTION_EXCEPTION, err_msg)` 二元组写入 `LocalModeObjectStore`
///     （`cpp/src/ray/runtime/object/local_mode_object_store.cc:40-46`）；actor 创建任务
///     抛出异常时跳过 `:327-331` 的 `actor_contexts.emplace`，后续 `actor_call` 在
///     `cpp/src/ray/runtime/task/local_mode_task_submitter.cc:109-113` 的
///     `actor_contexts_.at` 抛出 `std::out_of_range`。
/// - `RAY_CALLBACK_INTENTIONAL_EXIT`（2）：在 actor 方法里调用了 `exit_actor()`
///   （`data = NULL, len = 0`）。C 侧在 Rust 帧返回之后抛出
///   `ray::internal::RayIntentionalSystemExitException`，与 `AbstractRayRuntime::ExitActor`
///   同属一类；`GetExecuteResult`（`task_executor.cc:97-100`）把它变成
///   `Status::IntentionalSystemExit`，`ExecuteTask`（`:209-211`）见到该状态直接返回、
///   不写 meta buffer，actor 因此关闭且不按 `max_restarts` 重启。
/// - `RAY_CALLBACK_EXIT_OUTSIDE_ACTOR`（3）：在远程任务或 actor 构造函数里调用了
///   `exit_actor()`（`data = NULL, len = 0`）。C 侧抛出 `ray::internal::RayException`，
///   `GetExecuteResult` 的 `catch (const std::exception &)` 把它变成该次调用的失败。
///
/// 本地模式（`ray::RayConfig::local_mode`）没有 core worker，Rust 侧的
/// `exit_actor()` 在那里为空操作（不置标志），见 `crates/rayrust/src/actor_exit.rs`
/// 的模块文档。
///
/// 必须与 `crates/rayrust-sys/src/lib.rs` 的 `RAY_CALLBACK_*` 常量及
/// `RayCallbackResult` 布局一致。
typedef enum {
    /// Normal return: `data`/`len` hold the msgpack-serialized return value.
    RAY_CALLBACK_OK = 0,
    /// Callback failure (argument count mismatch, argument deserialization failure,
    /// null argument/actor pointer, panic in user code, poisoned actor mutex, or
    /// return-value serialization failure): `data`/`len` hold the UTF-8 error text
    /// without interior NUL bytes.
    RAY_CALLBACK_ERROR = 1,
    /// `exit_actor()` called inside an actor method (`data = NULL, len = 0`).
    RAY_CALLBACK_INTENTIONAL_EXIT = 2,
    /// `exit_actor()` called inside a remote task or actor constructor (`data = NULL, len = 0`).
    RAY_CALLBACK_EXIT_OUTSIDE_ACTOR = 3,
} ray_callback_status_t;

typedef struct {
    int status;
    const char *data;
    size_t len;
} ray_callback_result_t;

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
///   with the reason in ray_last_error_bytes() before the SDK is entered.
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
/// `runtime_env_json` is a JSON object string for runtime_env
///   (e.g. {"pip": ["pkg"]}). Pass NULL or empty string to skip. A malformed
///   JSON text or a valid JSON value that is not an object returns -1 before
///   the SDK is entered, recording `RAY_ERROR_KIND_GENERIC` with a message
///   starting with `runtime_env JSON is invalid: ` without terminating the
///   process. A non-object value reports only the JSON type name
///   (`array`, `number`, `string`, `boolean`, `null`), so the message does not
///   grow with the input.
/// `log_dir` is the directory for Ray logs. Pass NULL for default.
/// `actor_lifetime` is 0 = NON_DETACHED (default), 1 = DETACHED.
///   Detached actors outlive the job that created them.
/// `namespace` is the job-level namespace for named actors.
///   Pass NULL or empty string for default (no namespace).
/// Returns 0 on success, -1 on failure.
/// Any `std::exception` thrown while building `RayConfig` or `argv` is caught
/// inside `ray_init`, recorded as `RAY_ERROR_KIND_GENERIC` (or the matching
/// `ray_error_kind_t` for a `RayException`), and reported as -1 without
/// terminating the process. The namespace that local mode reports through
/// `ray_get_namespace()` is updated only after a new runtime is initialized
/// successfully: a rejected call, a call while Ray is already initialized, and
/// a `ray::Init` that throws all leave it unchanged.
/// The address checks run before the shutdown check: when an address is refused,
/// ray_last_error_bytes() carries that message and not
/// `a ray_shutdown is still in progress`.
/// A local-mode shutdown that is still running reports -1 with
/// `a ray_shutdown is still in progress` in ray_last_error_bytes(), and
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

/// 停止 Ray 运行时。
///
/// 执行次序：一开始先置位关闭标志（拒绝所有取运行时的入口与新的引用计数调用）并
/// 等待已经进入的引用计数调用退出；本地模式下接着等待当前运行时期间经三个任务入口
/// （`ray_task_call_with_options`、`ray_task_call_python`、
/// `ray_task_call_python_with_options`）提交的所有任务就绪；最后调用
/// `ray::Shutdown()`。集群模式不等待已提交的任务。
///
/// 关闭一旦开始，新的调用（包括本地任务提交）即被拒绝，并在关闭结束后继续被拒绝，
/// 直到下一次 `ray_init` 成功建立新运行时为止；`ray_init` 本身在本地关闭尚未完成
/// 期间返回 -1（消息为 `a ray_shutdown is still in progress`）。若本地任务排空等待
/// 失败，向 `stderr` 打印原因并调用 `std::abort()` 终止进程：本 `extern "C"` 入口
/// 没有错误通道，在任务仍在运行时销毁运行时会导致对 `object_store_` 的悬垂访问。
/// 并发调用只执行一次关闭：第一个进入的调用置位标志、取快照并等待，后续并发调用
/// 见到关闭正在进行就立即返回，不等待也不访问 Ray。
void ray_shutdown(void);

/// Closing gate
///
/// 上游 `ray::Shutdown()`（`cpp/src/ray/api.cc:42-46`）调用
/// `AbstractRayRuntime::DoShutdown()`（`cpp/src/ray/runtime/abstract_ray_runtime.cc:72-77`）：
/// 集群模式下 `CoreWorkerProcess::Shutdown()`
/// （`src/ray/core_worker/core_worker_process.cc:101-108`）先经 `ShutdownDriver()`
/// （同文件 `:945-957`）清空 `core_worker_`，再执行 `core_worker_process.reset()`，
/// 但 `RayRuntimeHolder::runtime_`（`cpp/include/ray/api/ray_runtime_holder.h:22-45`）
/// 不会被清空，`GetRayRuntime()` 在关闭开始与结束之后仍返回非空指针。关闭开始后访问
/// core worker 会在 `CoreWorkerProcessImpl::GetCoreWorker()`（`core_worker_process.cc:964-984`）
/// 或 `CoreWorkerProcess::EnsureInitialized`（同文件 `:842-856`）里调用 `QuickExit()`
/// （`_Exit(1)`，`src/ray/util/process.cc:845-848`）终止进程，`RAY_CATCH` 无法捕获。
///
/// - 关闭期间被拒绝的入口（共 29 个）：`ray_put`、`ray_put_xlang`、`ray_get`、
///   `ray_wait`、`ray_get_many`、`ray_task_call_with_options`、`ray_task_call_python`、
///   `ray_task_call_python_with_options`、`ray_actor_create_with_options`、
///   `ray_actor_create_python_with_options`、`ray_actor_call`、
///   `ray_actor_call_python`、`ray_actor_kill`、`ray_actor_handle_serialize`、
///   `ray_actor_handle_deserialize`、`ray_placement_group_create`、
///   `ray_placement_group_remove`、`ray_placement_group_wait_ready`、
///   `ray_placement_group_get_all`、`ray_placement_group_get_by_id`、
///   `ray_placement_group_get_by_name`、`ray_was_current_actor_restarted`、
///   `ray_get_namespace`、`ray_get_actor`、`ray_cancel`、`ray_gauge_set`、
///   `ray_counter_inc`、`ray_histogram_observe`、`ray_metric_record`。被拒绝时返回
///   各自的空值或错误码，错误类别为 `RAY_ERROR_KIND_RUNTIME`，`ray_last_error_bytes()`
///   消息原文为：
///   `the Ray runtime is shutting down or has shut down: calls are refused until a new runtime is initialized`。
/// - 判定次序：名字、id 宽度、引用参数 id 以及指标录制入口的句柄、指标种类、数值与
///   标签键值在取运行时之前检查，所以它们与关闭状态同时不合法时报的是参数错误；选项
///   JSON 以及其中 `placement_group_id` 的查表在取运行时之后（查表需要运行时），所以
///   畸形 options JSON 与正在关闭同时出现时报关闭错误。
///   本地模式判定在取运行时与关闭检查之后，所以关闭期间的本地模式调用报的是关闭错误。
///   从未初始化过运行时时报 `Ray runtime is not initialized`
///   （`RAY_ERROR_KIND_GENERIC`）。
/// - 生效范围：关闭标志从 `ray_shutdown` 一开始置位（涵盖本地模式排空已提交任务的那段
///   时间），关闭结束后保持置位，直到下一次 `ray_init` 成功之后清除。
/// - 残留窗口：已经进入 Ray SDK 的调用不由 `ray_shutdown` 等待，因为阻塞调用可能没有
///   期限（`Get` 的期限可以是 -1）。另一个线程 drop 最后一个 `Ray` 句柄时，在途的
///   `Get` 会与关闭竞争：`CoreWorkerProcessImpl::ShutdownDriver()`
///   （`src/ray/core_worker/core_worker_process.cc:945-957`）在清空 `core_worker_` 之前
///   先调用 `CoreWorker::Disconnect()`（`src/ray/core_worker/core_worker.cc:563-596`），
///   后者关掉与 raylet 的 IPC 连接（同文件 `:588-589`）；在途调用随后析构
///   `CoreWorkerPlasmaStoreProvider::Get()` 里持有的 `ipc::ScopedResponse`
///   （`src/ray/core_worker/store_provider/plasma_store_provider.cc:254-355`；持有它的是
///   同文件 `:258` 的 `get_request_cleanup_handlers`，在 `:278-281` 填入），其
///   `HandleCleanup()`（`src/ray/raylet_ipc_client/raylet_ipc_client_interface.h:67-72`）
///   在 `:70` 对 `Broken pipe` 执行 `RAY_CHECK(s.ok())`，进程以退出码 1 终止。
///   调用也可能以别的方式返回错误（实测到过 `Get object error: IOError: Broken pipe`）。
///   这个风险覆盖 `get`、`get_timeout` 与 `get_async`（`get_async` 在 `spawn_blocking`
///   里调同一个 `runtime::get_raw_timeout`）。通过关闭检查之后、还没进入
///   `CoreWorkerProcessImpl::GetCoreWorker()`（`core_worker_process.cc:964-984`）或
///   `CoreWorkerProcess::EnsureInitialized()`（同文件 `:842-856`）的那一段仍然可能撞上
///   `QuickExit()`。关闭开始之后的新调用由代次（`check_epoch`）与关闭闸门挡成
///   `RayError::Runtime`。Ray 自己的 C++ 前端同样不会等待在途的 `Get`。
/// - 不被拒绝的入口：`ray_is_local_mode` 不被拒绝（无错误通道，`IsLocalMode()` 是对
///   运行时对象的 C++ virtual 调用，关闭之后照常可答）；`ray_register_function` 与
///   `ray_register_member_function` 只写 `FunctionManager`，不访问运行时；
///   `ray_gauge_create`、`ray_counter_create`、`ray_histogram_create`、
///   `ray_sum_create`、`ray_metric_get_name` 与 `ray_metric_destroy` 只构造、读取或
///   释放 `ray::Metric` 对象，不访问 `RayRuntime`；`ray_add_local_reference` 与
///   `ray_remove_local_reference` 在关闭开始后直接返回 0 且不记录错误。

// ─── Object Store ─────────────────────────────────────────────

/// Put a serialized object into the object store.
/// `data` is msgpack-serialized bytes; `len` must be at least 1: a zero-length
/// value is refused with `RAY_ERROR_KIND_GENERIC` before the runtime is looked
/// up, because `msgpack::sbuffer(0)` hands the SDK a null data pointer and
/// `LocalMemoryBuffer` RAY_CHECKs it (`src/ray/common/buffer.h:77`), terminating
/// the process in both modes. Every value this crate serializes is at least one
/// byte, so the refusal never rejects a value the crate itself produces.
/// Returns a ray_bytes_t containing the binary object ID.
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
/// Caller must free with ray_free_bytes.
ray_bytes_t ray_put(const char *data, size_t len);

/// Put a cross-language serialized object into the object store with `"XLANG"`
/// metadata (`ray::internal::METADATA_STR_XLANG`).
/// - `data` must hold a cross-language payload prefixed with the 9-byte XLANG
///   header: `len > 9`, `data != NULL`, and the first msgpack value in the
///   9-byte header must be a non-negative integer equal to `len - 9`. Any other
///   input (zero length, null pointer, `len <= 9`, a payload without an XLANG
///   header, or a header whose declared length differs from `len - 9`) returns
///   `{NULL, 0}` with `RAY_ERROR_KIND_GENERIC` before the runtime is looked up.
/// - Once `ray_shutdown` has started or finished, the closing gate refuses the
///   call and returns `{NULL, 0}` with `RAY_ERROR_KIND_RUNTIME`.
/// - Local mode starts no `CoreWorkerProcess` or Python executor, and
///   `LocalModeObjectStore::PutRaw`
///   (`cpp/src/ray/runtime/object/local_mode_object_store.cc:34-38`, `:40-46`)
///   can only store empty metadata. When `runtime->IsLocalMode()` is true or
///   `CoreWorkerProcess::IsInitialized()`
///   (`src/ray/core_worker/core_worker_process.cc:110`) is false, this entry
///   returns `{NULL, 0}` with `RAY_ERROR_KIND_GENERIC` without writing an
///   object lacking metadata.
/// - In cluster mode this entry writes `data` together with `"XLANG"` metadata
///   through `CoreWorker::Put` (`src/ray/core_worker/core_worker.cc:956-975`),
///   returning a `malloc`-allocated 28-byte binary `ObjectID` (freed via
///   `ray_free_bytes`) whose initial local reference count is 1
///   (`core_worker.cc:962-969`).
ray_bytes_t ray_put_xlang(const char *data, size_t len);

/// Get a single object from the object store.
/// `id_data`/`id_len` is the binary object ID and must be exactly 28 bytes (the
/// Ray 2.51.1 ObjectID length). Any other length, including 0, is rejected
/// without touching Ray: `ObjectID::FromBinary` RAY_CHECKs a non-empty length
/// mismatch and terminates the process (`src/ray/common/id.h:430-441`), while an
/// empty id becomes the nil id, which is never ready (`get` then reports
/// TimedOut).
/// `timeout_ms` is -1 for infinite wait; any value below -1 is rejected before
/// Ray is entered, because `src/ray/core_worker/store_provider/memory_store/memory_store.cc:97`
/// RAY_CHECKs `timeout_ms >= 0 || timeout_ms == -1` and ends the process.
/// Returns a ray_bytes_t containing the serialized object data. The underlying
/// buffer from the unique `msgpack::sbuffer` returned by `RayRuntime::Get`
/// (allocated with global C standard library `::malloc` / `::realloc` in
/// `msgpack/v1/sbuffer.hpp:24-141`) is transferred directly via
/// `sbuffer::release()` (`:93-100`); caller frees it with `ray_free_bytes`
/// (`std::free`).
/// On error, returns ray_bytes_t with data=NULL.
/// A zero-length buffer from `Get` means Ray failed the object with an error
/// type outside the seven the C++ SDK translates and recorded no error text:
/// `ray_get` returns `data=NULL` and `len=0` with `RAY_ERROR_KIND_UNCLASSIFIED`
/// and a message that explains the condition and names `TASK_CANCELLED`
/// (untranslated types that record a `RayErrorInfo` payload carry a `9 + N`
/// protobuf frame instead, which Rust recognises before decoding and reports as
/// `RayError::Unclassified`). Every value this crate writes is msgpack-encoded
/// and at least one byte long, and `ray_put` refuses `len == 0`, so zero length
/// never comes from a normal value.
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_get(const char *id_data, size_t id_len, int timeout_ms);

/// Add one local reference to an object ID.
/// `id_len` must be exactly 28 bytes (the Ray 2.51.1 ObjectID length); any
/// other length is rejected without touching Ray. `ObjectID::FromBinary`
/// terminates the process on a non-empty length mismatch, and returns the nil
/// ID for an empty string (src/ray/common/id.h:430-440).
/// Returns 0 on success, -1 on error with the message in ray_last_error_bytes().
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
/// `timeout_ms` is -1 for infinite wait; any value below -1 is rejected before
/// Ray is entered (`src/ray/core_worker/store_provider/memory_store/memory_store.cc:97`
/// RAY_CHECKs it and ends the process).
/// Returns a heap-allocated bool array (one per input ID).
/// 关闭期间同样拒绝（返回 NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
/// Caller must free with ray_free_bools.
bool *ray_wait(const ray_bytes_t *ids, size_t count, size_t num_objects, int timeout_ms);

// ─── Task ─────────────────────────────────────────────────────

/// Call a remote task with full CallOptions.
/// `func_name` is a null-terminated C string (function names are ASCII). A null
///   pointer is rejected with RAY_ERROR_KIND_GENERIC and `func_name is null`. The
///   empty string is refused too: the single-argument `RemoteFunctionHolder`
///   constructor throws `Function not found. Please use RAY_REMOTE to register
///   this function.` (`cpp/include/ray/api/ray_runtime.h:41-47`), which the entry's
///   catch records as RAY_ERROR_KIND_RAY. Both checks run before the runtime
///   lookup and before the closing check, so a null or empty name without a
///   runtime reports the name error.
/// `args` is an array of msgpack-serialized arguments.
/// `is_ref` is an array of booleans (same length as args). If is_ref[i] is true,
///   args[i] is treated as a binary ObjectRef ID (pass by reference).
///   Pass NULL to treat all args as values. Any other length is rejected by the
///   Rust side; this function itself does not know arg_count's relation to it.
///   A reference argument must be exactly 28 bytes (the Ray 2.51.1 ObjectID
///   length); any other length is rejected before the TaskArg is built, with the
///   argument index in ray_last_error_bytes(). A length mismatch would hit a RAY_CHECK
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
/// group lookup there) and drops the other three fields:
/// `LocalModeTaskSubmitter::SubmitTask` calls `Submit(invocation, {})`
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:138-140`), so name,
/// resources and runtime_env have no effect.
/// Pass NULL or empty string for no options (all defaults).
/// Returns a ray_bytes_t containing the binary object ID of the result.
/// On error, returns ray_bytes_t with data=NULL.
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_task_call_with_options(const char *func_name,
                                         const ray_bytes_t *args,
                                         size_t arg_count,
                                         const bool *is_ref,
                                         const char *options_json);

/// Call a Python remote function.
/// This entry point takes no options JSON: the submitted task keeps Ray's default
/// of one CPU and cannot be named, cannot request resources, cannot be bound to a
/// placement group bundle and cannot be given a runtime env. Use
/// `ray_task_call_python_with_options` for any of those.
/// `module_name` and `function_name` are null-terminated C strings.
/// `args` are msgpack-serialized arguments (will be wrapped with xlang header).
/// `is_ref[i] = true` means args[i] is an ObjectRef ID (pass by reference).
///   Such an id must be exactly 28 bytes, the same requirement and the same
///   rejection as ray_task_call_with_options.
///   Such an id must be a reference the calling process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process. It must also carry
///   cross-language or Python metadata (written by `ray_put_xlang` or produced
///   by a Python worker); objects written by `ray_put` carry no metadata and
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
/// `module_name` must be non-empty. An empty one — and a name with an interior NUL
/// byte, which reaches this entry as an empty C string — is refused here with
/// RAY_ERROR_KIND_GENERIC before the runtime is looked up and before any task is
/// submitted. That check also comes before the runtime-not-initialized check and
/// before the local mode refusal above, so an empty module name reports the name
/// error in both modes and without a runtime.
/// An empty module name can never run. The worker imports the module by name
/// (`python/ray/_private/function_manager.py:138`, `importlib.import_module`) and
/// importlib raises `ValueError: Empty module name` for an empty string — it
/// raises before any lookup, so a module planted in `sys.modules` under the empty
/// name does not change that. The exception escapes `task_execution_handler`, the
/// worker exits with `SYSTEM_ERROR`, the caller's first read fails with
/// `RAY_ERROR_KIND_WORKER` (whose `RayErrorInfo` message Rust's `last_error()`
/// extracts from the recorded payload, falling back to UTF-8 lossy bytes), and
/// the worker pool starts a replacement worker afterwards.
/// `function_name` is not checked. The loader splits it on dots and throws the
/// empty pieces away (`function_manager.py:139`), so an empty name resolves to the
/// module object itself, which `_load_function_from_local` registers as the task's
/// function (`:391-395`). An ordinary module is not callable, so the task then
/// fails with `TypeError: 'module' object is not callable`: an ordinary task
/// failure (`RAY_ERROR_KIND_TASK`), where the worker stays alive. A module object can be callable, though — a `types.ModuleType` subclass
/// with `__call__` assigned to the module's `__class__`, or any callable placed in
/// `sys.modules` under that name — and then the task runs and returns that
/// object's result (measured on 2.51.1: such a module returned 3 for a call with
/// an empty function name). Refusing the name would reject that call, so the entry
/// submits it. An interior NUL byte makes it an empty function name as well, and
/// the submission goes through the same way.
/// A non-empty name nobody registered keeps Ray's asynchronous semantics: the
/// driver holds no function table and cannot tell a valid name from an invalid
/// one, so the submission returns an `ObjectRef` and the first read waits until it
/// times out. A whitespace-only name such as `" "` is not trimmed and is submitted
/// as it is; names such as `"rayrust_test"` and `"add"` reach the submission
/// unchanged.
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_task_call_python(const char *module_name,
                                  const char *function_name,
                                  const ray_bytes_t *args,
                                  size_t arg_count,
                                  const bool *is_ref);

/// Call a Python remote function with full CallOptions.
/// `options_json` is a JSON string with any subset of the fields
/// `ray_task_call_with_options` takes:
///   {
///     "name": "my_task",                  // task name
///     "resources": {"CPU":1,"GPU":0.5},  // resource requirements
///     "placement_group_id": "a1b2c3...",  // hex-encoded binary placement group ID
///     "bundle_index": 0,                  // bundle index (default -1 = any bundle)
///     "runtime_env": "{\"env_vars\":{...}}"  // runtime env object JSON, as a
///                                             // JSON string value
///   }
/// The field set, the validation and the error kinds are the ones
/// `ray_task_call_with_options` documents above: a bad field value returns an
/// error (data=NULL) instead of letting the task run with default scheduling.
/// - `placement_group_id` must decode to exactly 18 bytes and name an existing
///   group.
/// - `bundle_index` must be -1 or between 0 and 2147483647, and it requires
///   `placement_group_id`.
/// - `runtime_env` must be a JSON string holding a single JSON object.
/// `resources` carries the same task default: when the map has no `CPU` key, 1.0 is
/// added (the Python `@ray.remote` default), so `--num-cpus` limits how many
/// tasks run at once. An explicit `"CPU":0` is the only way to ask for zero CPU,
/// which is what `ray_task_call_python` cannot do; a map with other resources and
/// no `CPU` still gets 1.0 merged in.
/// Local mode rejects `placement_group_id` outright and drops the other three
/// fields, the same way `ray_task_call_with_options` does
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:138-140`).
/// The name checks and the reference-argument requirements are the ones
/// `ray_task_call_python` documents above, because both entries share the same
/// submission path: `module_name` must be non-empty (an empty one, or one with an
/// interior NUL byte, is refused with RAY_ERROR_KIND_GENERIC before the runtime is
/// looked up and before any task is submitted), and `function_name` is not checked
/// (an empty one resolves to the module object, which can be callable). A
/// reference argument must be a 28-byte id this process holds, whose owner Ray
/// knows, and it must carry cross-language or Python metadata (written by
/// `ray_put_xlang` or produced by a Python worker).
/// Local mode returns an error instead of submitting when a `#[rayrust::remote]`
/// function is registered under `function_name`, for the reason
/// `ray_task_call_python` documents.
/// Pass NULL or empty string for no options (all defaults).
/// Returns a ray_bytes_t containing the binary object ID of the result.
/// On error, returns ray_bytes_t with data=NULL.
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_task_call_python_with_options(const char *module_name,
                                              const char *function_name,
                                              const ray_bytes_t *args,
                                              size_t arg_count,
                                              const bool *is_ref,
                                              const char *options_json);

// ─── Actor ────────────────────────────────────────────────────

/// Create an actor with full ActorCreationOptions.
/// `options_json` is a JSON string with any subset of fields:
///   {
///     "name": "my_actor",              // named actor
///     "ray_namespace": "my_ns",        // namespace for named actor lookup
///     "resources": {"CPU":1,"GPU":1},  // resource requirements
///     "max_restarts": 3,               // restart on failure (default 0)
///     "max_concurrency": 10,           // max concurrent method calls (default 1)
///     "runtime_env": "{\"env_vars\":{\"FOO\":\"bar\"}}",  // runtime env document
///     "placement_group_id": "a1b2c3...", // hex-encoded binary placement group ID
///     "bundle_index": 0                // bundle index within the placement group
///   }
/// Pass NULL or empty string for no options (all defaults).
/// `resources` gets an actor default the C++ SDK does not apply: when it names any
/// resource other than `memory` and `object_store_memory` and does not name `CPU`,
/// 1.0 CPU is added for the actor's lifetime. That is the branch Python's
/// `@ray.remote` takes when
/// `set(resources.keys()).difference({"memory", "object_store_memory"})` is
/// non-empty (`ray/actor.py:1660-1672`), and `DEFAULT_ACTOR_CREATION_CPU_SPECIFIED`
/// is 1 (`ray/_private/ray_constants.py:179`). An existing `CPU` key is never
/// overwritten, so `"CPU":0` opts out and `"CPU":2` stays 2. Naming only the
/// memory keys, or naming nothing, adds no CPU, and that actor holds no CPU for its
/// lifetime (`DEFAULT_ACTOR_CREATION_CPU_SIMPLE` is 0, same file `:175`); Python
/// pairs that branch with a 1 CPU requirement per method
/// (`DEFAULT_ACTOR_METHOD_CPU_SIMPLE`), and the C++ SDK's `ActorCreationOptions`
/// has no method-CPU field (`cpp/include/ray/api/task_options.h:104-112`), so here
/// such an actor holds no CPU at all.
/// The memory keys reach the same table from the `memory=` and
/// `object_store_memory=` options and never from `resources=`, which rejects them
/// (`ray/_common/utils.py:136-140`, `:157-160`); an actor with a `resources` table
/// that holds nothing but them is the only-memory case above.
/// Local mode builds the actor's task spec with an empty resource table
/// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:42-43`, `:66-67`), so this
/// default has no effect there; the name and the two actor limits are still read
/// (`:146-149`, `:81`, `:84`).
/// `runtime_env` is the user-shaped document Python's `runtime_env=` takes, the
/// same one `ray_task_call_with_options` accepts: `{"env_vars":{...}}`,
/// `{"pip":[...]}`. Its value is a JSON string holding that document, so the
/// document cannot splice keys into the options JSON. The document must be a JSON
/// object: a non-string value, text that is not JSON, a JSON array or scalar
/// returns an error with RAY_ERROR_KIND_GENERIC before the actor is created.
/// Converting the document into the info string the core worker wants is
/// `ray::RuntimeEnv::Deserialize(...).SerializeToRuntimeEnvInfo()`
/// (`cpp/src/ray/runtime/runtime_env.cc:57-76`); the core worker parses the
/// string it receives with `RAY_CHECK(JsonStringToMessage(...).ok())`
/// (`src/ray/core_worker/core_worker.cc:1792-1796`), which terminates the driver.
/// An absent key means no runtime env, and so does a `NULL` or empty
/// `options_json`.
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
/// `src/ray/core_worker/core_worker_shutdown_executor.cc:40` and the driver's
/// first read fails with `RAY_ERROR_KIND_ACTOR` (whose error message Rust's
/// `last_error()` extracts from the recorded payload, falling back to UTF-8
/// lossy bytes). A whitespace-only name is not trimmed
/// and is submitted as it is, as `ray_get_actor` does; a non-empty
/// name nobody registered keeps Ray's asynchronous semantics, because the driver has
/// no way to learn which names are registered.
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// caller gets `Ok` with a 16-byte id whose first read fails with
/// `RAY_ERROR_KIND_ACTOR` (whose error message Rust's `last_error()` extracts
/// from the recorded payload, falling back to UTF-8 lossy bytes). An empty module name fails in
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
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// `func_name` is the member function name. A null pointer is rejected with
///   RAY_ERROR_KIND_GENERIC and `func_name is null`. The empty string is refused
///   too: the single-argument `RemoteFunctionHolder` constructor throws
///   `RayException` (`cpp/include/ray/api/ray_runtime.h:41-47`), recorded as
///   RAY_ERROR_KIND_RAY. Both checks run before the runtime lookup and before the
///   closing check.
/// Returns a ray_bytes_t containing the binary object ID of the result.
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// `method_name` is the Python method name (without `self`). It must be non-empty.
/// An empty name is not always fatal: a Python instance can carry an attribute named
/// with the empty string (`setattr(Class, "", method)` puts one into the class
/// namespace), and a cross-language call with that name reaches it (measured on Ray
/// 2.51.1: the call read 42 and the actor kept answering). The driver cannot tell such
/// an actor apart from one without that attribute, and there the call ends the actor's
/// worker: it exits with `SYSTEM_ERROR` (`Worker exits unexpectedly`) and a traceback
/// holding `python/ray/_raylet.pyx:2432` (`task_execution_handler`), `:2291` (which
/// calls `execute_task`) and `:1873`, where `execute_task` runs
/// `function_name = execution_info.function_name` and raises
/// `AttributeError: 'dict' object has no attribute 'function_name'`; the caller's
/// first read then fails with `RAY_ERROR_KIND_ACTOR` (whose error message Rust's
/// `last_error()` extracts from the recorded payload, falling back to UTF-8
/// lossy bytes) and that actor cannot be called again. This entry refuses the empty name in both cases, including
/// the one where the method exists. The C++ entry `ray_actor_call` refuses an
/// empty name in the driver as well: its single-argument `RemoteFunctionHolder`
/// constructor throws `Function not found. Please use RAY_REMOTE to register this
/// function.` (`cpp/include/ray/api/ray_runtime.h:41-47`) before any task is
/// submitted, so its actor keeps running. A name with an interior NUL byte reaches
/// this entry as an empty C string as well (`crates/rayrust-sys/src/lib.rs:253-255`,
/// #27). The null and empty checks run after the actor id width check and before the
/// runtime lookup, so local mode with an empty method name reports `the method name is
/// empty` while local mode with a non-empty one still reports the local mode error.
/// A non-empty method name the actor does not have is not rejected here: the driver
/// holds no method table and cannot tell a valid name from an invalid one. Measured
/// on 2.51.1 such a name ends the actor's worker with the same traceback as an empty
/// one, so the caller must pass a method name that really exists.
/// `is_ref[i] = true` means args[i] is an ObjectRef ID (pass by reference).
///   Such an id must be exactly 28 bytes, the same requirement and the same
///   rejection as ray_task_call_with_options.
///   It must also be a reference the calling process holds and whose owner
///   Ray knows: a 28-byte id of unknown origin makes `TransformArgs` call
///   `GetOwnerAddressOrDie`, which terminates the process. It must also carry
///   cross-language or Python metadata (written by `ray_put_xlang` or produced
///   by a Python worker); objects written by `ray_put` carry no metadata and
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
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// Returns nothing: an error is reported through ray_last_error_bytes().
/// Local mode returns an error: `AbstractRayRuntime::KillActor` starts with
/// `CoreWorkerProcess::GetCoreWorker()`, whose `EnsureInitialized` calls
/// `QuickExit()` (`_Exit(1)`, `src/ray/util/process.cc:845-848`) because local
/// mode never starts a core worker process. `LocalModeRayRuntime` does not
/// override `KillActor`, and a local actor lives in the calling process
/// (`LocalModeTaskSubmitter::actor_contexts_`), so there is nothing to kill.
/// 关闭期间同样拒绝，类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_actor_handle_deserialize(const char *serialized_data,
                                         size_t serialized_len,
                                         ray_actor_handle_meta_t *out_meta);

// ─── Placement Group ──────────────────────────────────────────

/// Create a placement group.
/// `name` is a null-terminated string (can be NULL).
/// `bundles_json` is a JSON array of bundle objects.
/// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
/// Returns a ray_bytes_t containing the group ID.
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy);

/// Remove a placement group by ID.
/// `group_id_data`/`group_id_len` must be exactly 18 bytes
/// (`PlacementGroupID::kLength`); any other length, a null pointer and an empty
/// length are rejected without touching Ray, because
/// `PlacementGroupID::FromBinary` RAY_CHECKs the length (`src/ray/common/id.h:435`)
/// and that check ends the process instead of throwing.
/// Returns 0 on success, -1 on error with the message in ray_last_error_bytes().
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
/// 关闭期间同样拒绝（返回 -1），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// ray_last_error_bytes().
/// Local mode returns -1: `LocalModeTaskSubmitter` does not override the
/// interface default `return true` (`cpp/src/ray/runtime/task/task_submitter.h:49-52`),
/// so the SDK would answer "ready" without waiting for anything.
/// 关闭期间同样拒绝（返回 -1），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// 三个查询入口在关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
ray_bytes_t ray_placement_group_get_all(void);
ray_bytes_t ray_placement_group_get_by_id(const char *group_id_data, size_t group_id_len);
ray_bytes_t ray_placement_group_get_by_name(const char *name);

// ─── Misc ─────────────────────────────────────────────────────

/// Returns true if the current actor was restarted.
///
/// The return type has no error channel, so a failure reads as `false`; the
/// reason is only visible to C callers through `ray_last_error_bytes()`. Failures are
/// a missing runtime (`"Ray runtime is not initialized"`), an SDK exception
/// (its `RayException::msg_` payload, or `what()` for other exceptions), and a
/// non-`std::exception` throw (`"unknown exception"`).
/// In local mode this returns `false` without touching the core worker
/// (`AbstractRayRuntime::WasCurrentActorRestarted` takes its SINGLE_PROCESS
/// branch, `cpp/src/ray/runtime/abstract_ray_runtime.cc:288-291`).
/// 关闭期间同样拒绝（返回 false 并写入错误），类别是 `RAY_ERROR_KIND_RUNTIME`。
bool ray_was_current_actor_restarted(void);

/// Returns true if this process is in local mode.
/// Returns false before `ray_init`: there is no runtime to ask.
bool ray_is_local_mode(void);

/// 运行时是不是由别的 DSO 建的。
///
/// 返回 1 的条件是：本 .so 内联的 `RayRuntimeHolder::Instance()::instance`（`ray_c.cc`
/// 通过 `GetRayRuntime()` 读到的那份）为空，而 `libray_api.so` 里进程唯一的那份
/// （`AbstractRayRuntime::GetInstance()`）有运行时。
/// 工人进程（`ray/cpp/default_worker` dlopen 本 .so）满足这条：运行时由
/// `default_worker` 里的 `ray::Init` 建在 `libray_api.so` 的那份上。
/// 驱动不满足，包括直接调本入口 `ray_init` 的原始 C 驱动：`ray::Init` 填的就是本
/// .so 的 holder。
/// 只给 Rust 侧的代次检查（`object_ref::check_epoch`）用：它决定代次 0 的重建引用
/// 能不能进入包装层。没有错误通道，也不需要：每个分支都是一个可读的布尔结果。
int ray_runtime_is_borrowed(void);

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
/// 关闭期间同样拒绝（data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// 关闭期间同样拒绝（返回 data=NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
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
/// 关闭期间同样拒绝（返回 -1），类别是 `RAY_ERROR_KIND_RUNTIME`。
int ray_cancel(const char *id_data, size_t id_len, bool force_kill, bool recursive);

/// Get multiple objects from the object store.
/// `ids` is an array of ray_bytes_t, each exactly 28 bytes (the Ray 2.51.1
/// ObjectID length). Any other length, including 0, is rejected without touching
/// Ray for the same reasons as ray_get. `count` is the number of IDs; a value
/// above INT_MAX is rejected before the IDs are walked, because Ray's `Wait`
/// takes an `int`.
/// `timeout_ms` is -1 for infinite wait; any value below -1 is rejected before
/// Ray is entered (`src/ray/core_worker/store_provider/memory_store/memory_store.cc:97`
/// RAY_CHECKs it and ends the process).
/// Returns a heap-allocated array of ray_bytes_t (one per input ID), or NULL
/// when argument validation, runtime lookup, `Wait`, or array allocation fails.
/// 关闭期间同样拒绝（返回 NULL），类别是 `RAY_ERROR_KIND_RUNTIME`。
///
/// Implementation and cost: every non-empty batch deduplicates IDs (because
/// `CoreWorker::Wait` rejects duplicates at
/// `src/ray/core_worker/core_worker.cc:1475-1484`), calls `Wait` once with
/// `timeout_ms` to establish the deadline across all unique IDs, and then performs
/// one single-object `Get` per ready element — at most `count` calls, and a
/// single-element batch costs one `Wait` plus one `Get`. Upstream's batch `Get`
/// cannot be used because mixed batches leave null `RayObject` entries when any
/// element is an exception — both for unready elements
/// (`src/ray/core_worker/store_provider/memory_store/memory_store.cc:276-419`,
/// early return at `:308-333`) and for plasma-promoted elements skipped when
/// `got_exception` is set (`src/ray/core_worker/core_worker.cc:1337-1426` at
/// `:1375-1384` and `:1404-1423`) — and `NativeObjectStore::GetRaw` dereferences
/// `results[i]` without a null check
/// (`cpp/src/ray/runtime/object/native_object_store.cc:90-134` at `:104-105`),
/// crashing the process with SIGSEGV.
///
/// The `Wait` does not return early when the batch already holds an exception
/// object (`CoreWorkerMemoryStore::GetImpl` runs with
/// `abort_if_any_object_is_exception = false`), so a multi-element batch waits for
/// every element or for the deadline; `timeout_ms = -1` therefore blocks until all
/// elements are ready.
///
/// Elements are processed in input order:
/// - An element still unready after `Wait` has `data=NULL, len=0` with
///   `RAY_ERROR_KIND_TIMEOUT` (`"Get timed out: some object(s) not ready."`).
/// - An element whose single-object `Get` throws a C++ exception has
///   `data=NULL, len=0` with the exception's mapped `ray_error_kind_t` and full
///   message.
/// - A zero-length buffer from `Get` means Ray failed that object with an error
///   type outside the seven the C++ SDK translates and recorded no error text
///   (every value this crate writes is msgpack-encoded and at least one byte long,
///   and `ray_put` refuses `len == 0`, so zero length never comes from a normal
///   value; untranslated types that record a `RayErrorInfo` payload carry a
///   `9 + N` protobuf frame instead, which Rust recognises before decoding and
///   reports as `RayError::Unclassified`), and sets that element to
///   `data=NULL, len=0` with `RAY_ERROR_KIND_UNCLASSIFIED` and a message naming
///   `TASK_CANCELLED`.
/// - Each ready element transfers the buffer of the unique `msgpack::sbuffer`
///   returned by `RayRuntime::Get` directly via `sbuffer::release()`
///   (`msgpack/v1/sbuffer.hpp:93-100`).
/// - When the object store returns no buffer for an element or buffer transfer
///   falls back to a failed allocation, that element has `data=NULL, len=0`
///   with a `RAY_ERROR_KIND_GENERIC` message.
/// The first failing element's kind and message are recorded for
/// `ray_last_error_bytes()` and `ray_last_error_kind()`; later failing elements
/// leave the thread-local error channel untouched.
/// Caller must free the returned array and element buffers with
/// `ray_free_bytes_array(out, count)`.
ray_bytes_t *ray_get_many(const ray_bytes_t *ids, size_t count, int timeout_ms);

/// Free an array returned by ray_get_many.
void ray_free_bytes_array(ray_bytes_t *array, size_t count);

// ─── Metrics ──────────────────────────────────────────────────

/// Opaque handle owning a C++ `ray::Metric` subclass instance (`ray::Gauge`,
/// `ray::Counter`, `ray::Histogram`, or `ray::Sum` from `<ray/api/metric.h>`).
typedef struct ray_metric_s ray_metric_t;

/// A single `(key, value)` tag pair passed to metric recording entries.
typedef struct {
    ray_bytes_t key;
    ray_bytes_t value;
} ray_metric_tag_t;

/// Create a `ray::Gauge`, `ray::Counter`, `ray::Histogram`, or `ray::Sum` metric.
///
/// - `name` must be non-empty, free of interior NUL bytes, and match
///   `^[a-zA-Z_:][a-zA-Z0-9_:]*$`, the exact pattern enforced by
///   `ray::stats::Metric::Metric` (`src/ray/stats/metric.cc:86-109`). Checking it
///   before entering C++ prevents `RAY_CHECK_WITH_DISPLAY` (`:95-100`) from
///   calling `std::abort()`.
/// - `description` and `unit` may be empty; when `len > 0`, `data` must be
///   non-NULL and free of interior NUL bytes.
/// - `tag_keys` (`tag_key_count` entries; `tag_keys` may be NULL when
///   `tag_key_count == 0`): each key must be non-empty, free of interior NUL
///   bytes, and pairwise distinct.
/// - For `ray_histogram_create`, `boundaries` (`boundary_count` entries) must be
///   non-empty (`boundary_count >= 1` and `boundaries != NULL`), finite, and
///   strictly monotonically increasing (`boundaries[i - 1] < boundaries[i]`).
///
/// Construction does not access `RayRuntime` and is unaffected by the closing
/// gate, allowing static initialization before `ray_init`.
/// Returns an owned `ray_metric_t *` freed via `ray_metric_destroy`, or NULL on
/// error with `RAY_ERROR_KIND_GENERIC` recorded in `ray_last_error_bytes()`.
ray_metric_t *ray_gauge_create(ray_bytes_t name,
                               ray_bytes_t description,
                               ray_bytes_t unit,
                               const ray_bytes_t *tag_keys,
                               size_t tag_key_count);
ray_metric_t *ray_counter_create(ray_bytes_t name,
                                 ray_bytes_t description,
                                 ray_bytes_t unit,
                                 const ray_bytes_t *tag_keys,
                                 size_t tag_key_count);
ray_metric_t *ray_histogram_create(ray_bytes_t name,
                                   ray_bytes_t description,
                                   ray_bytes_t unit,
                                   const double *boundaries,
                                   size_t boundary_count,
                                   const ray_bytes_t *tag_keys,
                                   size_t tag_key_count);
ray_metric_t *ray_sum_create(ray_bytes_t name,
                             ray_bytes_t description,
                             ray_bytes_t unit,
                             const ray_bytes_t *tag_keys,
                             size_t tag_key_count);

/// Return the metric name from `ray::Metric::GetName()`.
/// Unaffected by the closing gate. Caller frees the returned buffer via
/// `ray_free_bytes`. Returns `{NULL, 0}` when `metric` is NULL or allocation fails.
ray_bytes_t ray_metric_get_name(const ray_metric_t *metric);

/// Destroy a metric handle created by `ray_*_create`. Passing NULL is a no-op.
/// Unaffected by the closing gate.
void ray_metric_destroy(ray_metric_t *metric);

/// Record a value on a metric handle.
///
/// Validation runs before `runtime_for_call()`:
/// 1. `metric` must be non-NULL, and type-specific entries require the matching
///    metric kind (`ray_gauge_set` -> `Gauge`, `ray_counter_inc` -> `Counter`,
///    `ray_histogram_observe` -> `Histogram`; `ray_metric_record` accepts all four).
/// 2. `value` must be finite (`std::isfinite(value)`), and for `Counter` handles
///    (both `ray_counter_inc` and `ray_metric_record`) `value >= 0.0`.
/// 3. `tags` (`tag_count` entries; `tags` may be NULL when `tag_count == 0`): each
///    `key` must be non-empty and free of interior NUL bytes; each `value` must be
///    free of interior NUL bytes (empty `value` is allowed). Repeated keys within
///    one slice keep the last value (`insert_or_assign`).
/// 4. `runtime_for_call()` requires an initialized runtime and rejects calls while
///    `ray_shutdown` is running or has completed (`RAY_ERROR_KIND_RUNTIME`, because
///    `ray::stats::Shutdown` at `src/ray/stats/stats.h:133-150` stops the exporter
///    threads without resetting `StatsConfig::is_stats_disabled_`).
///
/// Returns 0 on success, -1 on error.
int ray_gauge_set(ray_metric_t *metric,
                  double value,
                  const ray_metric_tag_t *tags,
                  size_t tag_count);
int ray_counter_inc(ray_metric_t *metric,
                    double value,
                    const ray_metric_tag_t *tags,
                    size_t tag_count);
int ray_histogram_observe(ray_metric_t *metric,
                          double value,
                          const ray_metric_tag_t *tags,
                          size_t tag_count);
int ray_metric_record(ray_metric_t *metric,
                      double value,
                      const ray_metric_tag_t *tags,
                      size_t tag_count);

// ─── Function Registration ────────────────────────────────────

/// Callback type for a Rust remote function.
/// Receives an array of msgpack-serialized argument buffers.
/// Returns a `ray_callback_result_t` whose non-NULL `data` buffer is allocated by
/// C standard library `malloc` (`libc::malloc` on the Rust side), independent of
/// Rust's `#[global_allocator]`, and freed by the C wrapper with `std::free`.
typedef ray_callback_result_t (*ray_func_callback_t)(const ray_bytes_t *args, size_t arg_count);

/// Register a Rust function as a Ray remote task.
/// Must be called before ray_init (or at least before the first task call).
/// `func_name` is the name used with ray_task_call_with_options.
/// `callback` is the C callback that will be invoked when the task executes.
void ray_register_function(const char *func_name, ray_func_callback_t callback);

// ─── Actor Member Function Registration ───────────────────────

/// Callback type for a Rust actor member function.
/// `actor_ptr` is the raw pointer (uint64_t) stored by the factory (`Box<Mutex<T>>`).
/// `args` / `arg_count` are the msgpack-serialized arguments (without `self`).
/// Returns a `ray_callback_result_t` whose non-NULL `data` buffer is allocated by
/// C standard library `malloc` (`libc::malloc` on the Rust side), independent of
/// Rust's `#[global_allocator]`, and freed by the C wrapper with `std::free`.
typedef ray_callback_result_t (*ray_member_callback_t)(uint64_t actor_ptr,
                                                        const ray_bytes_t *args,
                                                        size_t arg_count);

/// Register a Rust actor member function.
/// `func_name` is the name used with ray_actor_call.
/// `callback` is the C callback invoked when the actor method executes.
void ray_register_member_function(const char *func_name, ray_member_callback_t callback);

// ─── Memory Management ────────────────────────────────────────
//
// Cross-boundary heap ownership uses a single allocator per direction:
//
// 1. C -> Rust: Every heap buffer returned by `ray_*` (`ray_bytes_t.data`
//    including `ray_metric_get_name`, the `ray_bytes_t *` array and each
//    element's `data` from `ray_get_many`, and the `bool *` array from `ray_wait`)
//    comes from the global C standard library `malloc` / `realloc` (`std::malloc`
//    in `ray_c.cc`, or `msgpack::sbuffer` buffers allocated with `::malloc` /
//    `::realloc` in `msgpack/v1/sbuffer.hpp:24-141` and handed off via
//    `sbuffer::release()` in `ray_get` and `ray_get_many`). Rust frees them
//    exclusively via `ray_free_bytes`, `ray_free_bytes_array`, and `ray_free_bools`
//    (all calling `std::free`). Opaque `ray_metric_t *` handles are allocated on
//    the C++ heap by `ray_gauge_create`, `ray_counter_create`,
//    `ray_histogram_create`, and `ray_sum_create`, and freed exclusively via
//    `ray_metric_destroy`.
//
// 2. Rust -> C: In `ray_callback_result_t` returned by `ray_func_callback_t` and
//    `ray_member_callback_t`, every non-NULL `data` buffer must be allocated with
//    C standard library `malloc` (`libc::malloc` in Rust), independent of Rust's
//    `#[global_allocator]`. `unwrap_callback_result` releases it via
//    `CallbackBufferGuard` calling `std::free`. Zero-length payloads pass
//    `data = NULL, len = 0`.
//
// 3. Borrowed pointers and opaque handles: Input pointers passed to `ray_*` or
//    into Rust callbacks (`const char *`, `const ray_bytes_t *`) are borrowed for
//    the duration of the call only; `*data` written by `ray_last_error_bytes`
//    borrows the thread-local error buffer until the next error write or
//    `ray_clear_error` on the same thread; and `actor_ptr` (`uint64_t`) is an
//    opaque Rust `Box<Mutex<T>>` address serialized as a msgpack integer and
//    forwarded back to `ray_member_callback_t`. None of these are freed by the
//    receiver or C wrapper.

/// Free a ray_bytes_t returned by any ray_* function.
void ray_free_bytes(ray_bytes_t *ptr);

/// Free a bool array returned by ray_wait.
void ray_free_bools(bool *ptr);

// ─── Error Handling ───────────────────────────────────────────

/// Class of the last error, mirroring Ray's C++ exception hierarchy
/// (`ray/api/ray_exception.h`).
///
/// `RayFunctionNotFound` and `RayRuntimeEnvException` have no constant here:
/// a missing function is thrown only on the worker, where `TaskExecutor`'s
/// catch turns it into `Status::Invalid`
/// (`cpp/src/ray/runtime/task/task_executor.cc:101-111`, the catch at `:101`
/// and the `Status::Invalid` it returns at `:111`), so the driver sees a
/// `RayTaskException`; `RayRuntimeEnvException` from `ray_init` is caught by
/// its inner `runtime_env` block and recorded as `RAY_ERROR_KIND_GENERIC` with
/// a `runtime_env JSON is invalid: ` prefix, while `apply_runtime_env` on task
/// and actor options parses the JSON upfront (`nlohmann::json::parse` throws
/// `json::parse_error`, also `RAY_ERROR_KIND_GENERIC`) before calling
/// `RuntimeEnv::Deserialize`.
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
    /// 运行时正在关闭：仅由关闭闸门给出，与 Ray 的异常层次无关。
    RAY_ERROR_KIND_RUNTIME = 8,
    /// Ray 判定该对象失败，且错误类型在 C++ SDK 翻译的七种之外：仅由 `ray_get` 与
    /// `ray_get_many` 的零长度结果给出，与 Ray 的异常层次无关。
    RAY_ERROR_KIND_UNCLASSIFIED = 9,
} ray_error_kind_t;

/// Get the last error message bytes (thread-local) and their byte length.
/// Points directly into the thread-local `g_last_error` without copying; the
/// pointer remains valid until the next error write or `ray_clear_error` on
/// the same thread. Writes `*data = NULL` and `*len = 0` when no error is
/// recorded on this thread.
void ray_last_error_bytes(const char **data, size_t *len);

/// Get the class of the last error (thread-local), one of ray_error_kind_t
/// （0..=9，含关闭闸门写入的 `RAY_ERROR_KIND_RUNTIME` 与零长度结果写入的
/// `RAY_ERROR_KIND_UNCLASSIFIED`）。
/// Returns RAY_ERROR_KIND_NONE when no error is recorded on this thread
/// (`ray_last_error_bytes` writes `*data = NULL, *len = 0`).
int ray_last_error_kind(void);

/// Clear the last error message and its kind.
void ray_clear_error(void);

#ifdef __cplusplus
}
#endif
