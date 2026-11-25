# rayrust

[English](README.md) | [中文](README_CN.md)

A Rust SDK for [Ray](https://ray.io) — the distributed computing framework for scaling AI and Python applications.

rayrust wraps the Ray C++ SDK (`libray_api.so`) via a C ABI layer, providing idiomatic Rust APIs for Ray's core distributed primitives: object store, remote tasks, actors, placement groups, and cross-language calls.

## Features

| Feature | Local Mode | Cluster Mode |
|---|:---:|:---:|
| `Ray::connect` / `drop` (RAII lifecycle) | ✅ | ✅ |
| `put` / `get` / `wait` | ✅ | ✅ |
| `get_many` / `get_many_timeout` (batch get, optional deadline; one `Wait` + per-element `Get`, see #41) | ✅ | ✅ |
| `get_many_async` / `get_many_timeout_async` / `wait_async` (batch get and `Wait` on the tokio blocking pool) | ✅ | ✅ |
| `#[remote]` sync task | ✅ | ✅ |
| `#[remote]` async task (persistent runtime) | ✅ | ✅ |
| `#[actor]` macro (auto-generate factory + methods) | ✅ | ✅ |
| `actor_call_async` (non-blocking) | ✅ | ✅ |
| `get_async` (bounded `Get` on the tokio blocking pool) | ✅ | ✅ |
| Python task (cross-language, complex types) | ✅ | ✅ |
| Python actor (cross-language; local mode refuses creation #68 and calls #59) | ❌ | ✅ |
| Resource scheduling (`TaskOptions` / `ActorOptions` builder) | ✅ | ✅ |
| `ActorOptions`: name, namespace, max_restarts, max_concurrency, runtime_env, placement_group | ✅ | ✅ |
| `ActorLifetime::Detached` (detached actors) | ✅ | ✅ |
| `RayConfig.namespace` (job-level namespace) | ✅ | ✅ |
| PlacementGroup | ✅ | ✅ |
| `get_actor` (named actor, cross-namespace) | ✅ | ✅ |
| `cancel` (task cancellation; local mode has no cancellation path and returns an error, see #58) | ❌ | ✅ |
| `kill_actor` (local mode returns an error and the actor ends with the process, see #53) | ❌ | ✅ |
| XLANG header auto-detection (Ray 2.56.1+ compatible) | ✅ | ✅ |
| `rmpv::Value` dynamic deserialization | ✅ | ✅ |
| `put_xlang` (writes the 9-byte XLANG header and `XLANG` metadata via `CoreWorker::Put` in cluster mode; local mode has no `CoreWorker` or Python executor and returns an error, see #21) | ❌ | ✅ |
| `ray_last_error_bytes()` (thread-local error bytes + length; Rust extracts formatted exception text with UTF-8 lossy fallback) | ✅ | ✅ |

## Quick Start

### Prerequisites

Provide the Ray C++ SDK through an installed `ray[cpp]` package, `RAY_CPP_DIR`, or an opt-in download from PyPI (`RAYRUST_DOWNLOAD=1`):

```bash
git clone https://github.com/NolanHo/rayrust.git
cd rayrust
RAYRUST_DOWNLOAD=1 cargo build --release -p rayrust-example-worker
```

With `RAYRUST_DOWNLOAD=1`, the first build downloads the `ray-cpp` wheel from PyPI, verifies its SHA-256 digest, and extracts the C++ SDK to `~/.cache/rayrust/`. Subsequent builds reuse the cache without setting `RAYRUST_DOWNLOAD=1`.

To use a pre-installed Ray C++ SDK (e.g. from `pip install ray[cpp]`):

```bash
export RAY_CPP_DIR=/path/to/site-packages/ray/cpp
```

### Run in local mode (no cluster needed)

```bash
cargo run --example hello_ray
```

### Run against a Ray cluster

```bash
export RAY_ADDRESS='<head-node-ip>:6379'
export RAY_NODE_IP='<this-node-ip>'
export RAY_WORKER_SO="$(pwd)/target/release/librayrust_worker.so"

cargo run --example full_test
```

On Linux, the build embeds the SDK's `lib` directory into the RPATH of binaries, tests and the worker `.so` built from this repository, so `cargo test` and `cargo run --example ...` work directly; `LD_LIBRARY_PATH` is only needed if the SDK is moved after building. A crate of your own that depends on `rayrust` does not inherit those entries: add the link arguments to its own build configuration, or set `LD_LIBRARY_PATH` when running it.

## Usage

### Remote Tasks (`#[remote]`)

Supports sync and `async fn`. Async functions use a persistent global tokio runtime (created once, reused across calls).

```rust
use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[rayrust::remote]
async fn fetch(url: String) -> Vec<u8> { /* ... */ }

#[tokio::main]
async fn main() -> Result<(), RayError> {
    let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;

    // Async get: one bounded Get runs on the tokio blocking pool, so no tokio thread is held
    let r = add_remote(&ray, 1, 2)?;
    let v: i32 = r.get_async().await?;
    println!("add(1, 2) = {}", v);

    // Async submission (concurrent)
    let r = add_remote_async(&ray, 10, 20).await?;
    let v: i32 = r.get_async().await?;

    drop(ray);
    Ok(())
}
```

### Passing an `ObjectRef` as an argument

A reference can stand in for a value parameter: pass `ObjectRef<T>` (the handle moves into the call) or `&ObjectRef<T>` (the handle stays with you; one reference is cloned for the duration of the submission), and the driver does not have to `get` the value first. In cluster mode the core worker delivers the object's value to the worker; in local mode the C wrapper reads it before submission. The generated caller holds its own reference until the submission call returns, so dropping your handle before an async future is polled is safe.

```rust
let a = ray.put(&40i32)?;
let sum: ObjectRef<i32> = add_remote(&ray, 2, &a)?;  // same as add_remote(&ray, 2, 40)
let v: i32 = sum.get_async().await?;                 // 42
```

Every macro-generated caller returns `Result`, so argument serialization and submission errors both reach the caller instead of panicking. Actor methods take references the same way (`counter_increment(&ray, &handle, &a)`).

Two limits come with this feature. Rust tasks and actors take non-xlang references such as those from `Ray::put` (`TaskArg` rejects `Ray::put_xlang` handles with `RayError::Serialization` because C++ workers do not strip the 9-byte XLANG header); to pass a driver-written object by reference to a Python task or actor in cluster mode, write it with `Ray::put_xlang` so it carries `XLANG` metadata (`Ray::put` writes empty metadata, which Python rejects). And a Rust task cannot hand back a usable reference: the id it returns carries no nested references, and the driver has no owner path for it, so reading that id gives `ObjectUnknownOwner` even while the object is alive. Both limits are documented on `rayrust::TaskArg`.

### Actors (`#[actor]` macro)

The `#[rayrust::actor]` macro auto-generates the factory callback, member function callbacks, `#[ctor]` registration, and convenience callers:

```rust
use rayrust::prelude::*;

struct Counter { value: i64 }

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn get(&self) -> i64 {
        self.value
    }
}

#[tokio::main]
async fn main() -> Result<(), RayError> {
    let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;

    // Create actor via macro-generated factory
    let arg = serialize(&100i64)?;
    let handle = ray.actor_create(
        "__rayrust_actor_factory_counter", &[&arg], &ActorOptions::new()
    )?;

    // Call methods asynchronously
    let arg = serialize(&5i64)?;
    let r = ray.actor_call_async(
        handle.id(),
        "__rayrust_actor_factory_counter::increment",
        vec![arg],
        vec![],
    ).await?.cast::<i64>();
    let v = r.get_async().await?; // 105

    ray.kill_actor(&handle, true)?;
    drop(ray);
    Ok(())
}
```

### Cross-language (Python)

Rust ↔ Python with automatic XLANG header handling and complex type support:

```rust
// Python → Rust: list, dict, nested, None, mixed types
let obj = ray.task_call_python("my_module", "return_list", &[], &[])?;
let val: Vec<i64> = obj.cast().get_async().await?; // [1, 2, 3, 4, 5]

// Dynamic type (when return type is unknown)
let obj = ray.task_call_python("my_module", "complex_func", &[], &[])?;
let val = obj.get_value_async().await?; // rmpv::Value

// Rust → Python: send complex args
let arg = serialize(&vec![1i64, 2, 3])?;
let obj = ray.task_call_python("my_module", "echo_list", &[&arg], &[])?;
```

### Resource Scheduling

Request CPU/GPU resources for tasks and actors:

```rust
// Task with GPU
let obj = ray.task_call(
    "train_model", &args, &[], &TaskOptions::new().resource("GPU", 1.0).resource("CPU", 4.0)
)?;

// Actor with resources
let handle = ray.actor_create(
    "gpu_actor_factory", &args, &ActorOptions::new().resource("GPU", 1.0)
)?;
```

In cluster mode every task requests one CPU by default, the same default
Python's `@ray.remote` applies, so `--num-cpus` limits how many tasks run at
once. To ask for zero CPU (a task that only waits or only does I/O and should not
hold a CPU slot), say `.resource("CPU", 0.0)` explicitly. Giving another
resource without a `CPU` key still leaves the task at one CPU: the default 1.0 is
merged in. This covers both Rust task entries: `Ray::task_call` and
`Ray::task_call_python_with_options` take a `TaskOptions`, so either can ask for
zero CPU here. `Ray::task_call_python` is the entry without options; it keeps the
one-CPU default. Local mode drops resource requirements entirely, so none of this
applies there.

An actor follows Ray's two presets as far as the C++ SDK allows. Without
`resources` it reserves no CPU for its lifetime, like Python's no-options actor,
whose methods each cost one CPU; `ActorCreationOptions` has no method-CPU field, so
method calls here request no CPU. Naming any resource other than `memory` and
`object_store_memory` reserves one CPU for the actor's lifetime as well, which is
Python's rule for an actor that names resources; `.resource("CPU", 0.0)` opts out,
and an actor that names only the memory keys stays at zero.

### Object Store

```rust
let obj = ray.put(&42i32)?;
let val: i32 = ray.get(&obj)?;

// Async put/get
let obj = ray.put_async(42i32).await?;
let val: i32 = obj.get_async().await?;

// Batch get
let vals = ray.get_many(&[obj1, obj2, obj3])?;

// Wait for readiness
let (ready, unready) = ray.wait(&[obj1, obj2], 2, 5000)?;
```

`get_many*` deduplicates IDs for a single `Wait` and then runs one single-object
`Get` per ready element (at most one per input slot), because upstream's batch
`Get` leaves null `RayObject` entries on mixed batches containing an exception
object and dereferences them in `NativeObjectStore::GetRaw` (#41). That `Wait`
does not return early when the batch already holds a failed object, so a batch
with several elements waits for all of them or for the deadline: undeadlined
`get_many` blocks until every element is ready, and the deadline-carrying entries
report the first failing element only after the deadline expires.

In cluster mode (the core worker is initialized) the `ObjectRef` returned by
`put` and by a task or actor call holds one Ray local reference for its id:
`Clone` adds one and `Drop` removes one, so the object lives while at least one
such reference exists and may be freed once the last one is dropped. Local mode
and Ray worker processes do not take part in reference counting, so dropping a
reference there frees nothing. A reference rebuilt from id bytes
(`ObjectRef::from(id_bytes)`) holds no reference at all: whether the object is
still alive is up to whoever else holds it, so read it with `get_timeout`.

## Architecture

```
Rust application code
    |
    v
rayrust (safe Rust API)
    Ray (RAII context: connect / drop)
    ObjectRef<T>, ActorHandle, ActorOptions, TaskOptions
    TaskArg / TaskArgs (value or &ObjectRef<T> per argument)
    #[remote]/#[actor] macros → &Ray callers
    get_async (bounded `Get` on the tokio blocking pool), persistent tokio runtime
    |
    v
rayrust-sys (FFI bindings)
    extern "C" declarations, build.rs (SDK lookup; opt-in PyPI download via RAYRUST_DOWNLOAD=1)
    |
    v
ray_c.h / ray_c.cc (C ABI wrapper)
    Type-erased C interface, binary-safe IDs
    Thread-local error messages, resource scheduling
    |
    v
libray_api.so (Ray C++ SDK)  ->  Ray Core (raylet / GCS / object store)
```

## Build System

`rayrust` requires **`Ray >= 2.56.1`**; any `RAY_VERSION` or `RAY_CPP_DIR` SDK earlier than `2.56.1` is rejected at build time. Build scripts share `rayrust-build` to acquire the Ray C++ SDK:

1. **`RAY_CPP_DIR` env var** — use a specified SDK directory (probed for minimum supported version `>= 2.56.1`; need not match `RAY_VERSION`)
2. **`~/.cache/rayrust/ray-cpp-{version}-{platform}/`** (or `$RAYRUST_CACHE_DIR/ray-cpp-{version}-{platform}/`) — shared cache (debug + release)
3. **`python3` (`ray[cpp]`)** — probe `sys.path` for an installed `ray/cpp/` whose `ray` version matches `RAY_VERSION`
4. **Opt-in download from PyPI (`RAYRUST_DOWNLOAD=1`)** — only runs when `RAYRUST_DOWNLOAD` is enabled (`1`/`true`/`yes`; `0`/`false`/`no` or unset disables it): fetches the PyPI JSON index for that version's `ray-cpp` wheel, verifies its SHA-256 digest against `digests.sha256` (writing the digest to `ray-cpp-{version}-{platform}.sha256` alongside the cache directory), and extracts `ray/cpp/`. When `RAYRUST_DOWNLOAD` is unset, the build makes zero network requests and on failure lists the three ways to acquire the SDK (`RAY_CPP_DIR`, `pip install ray[cpp]`, or `RAYRUST_DOWNLOAD=1`).

`RAY_VERSION` (default `2.56.1`) names the cache directory in step 2, the version matched in step 3, and the wheel downloaded in step 4; step 1 uses `RAY_CPP_DIR` directly and verifies that its Ray version is `>= 2.56.1`. The `.sha256` pin file alongside the cache directory records the identity of the wheel verified at download time; cache hits validate and read that pin file without re-hashing the extracted SDK directory on every build.

Relative values in `RAY_CPP_DIR` and `RAYRUST_CACHE_DIR` are resolved against the directory `cargo` runs in (`PWD`, or the workspace root when `PWD` is unset); `~` is not expanded, so write `$HOME` instead.

Build warnings print the resolved SDK directory, source, and Ray version:

- `cargo:warning=Using Ray C++ SDK at <dir> (from RAY_CPP_DIR, version not checked)`
- `cargo:warning=Using Ray C++ SDK at <dir> (from cache, Ray <ver>)` (or `(from cache, Ray <ver>, sha256 <64-hex>)` when the `.sha256` pin file is present)
- `cargo:warning=Using Ray C++ SDK at <dir> (from python3, Ray <ver>)`
- `cargo:warning=Using Ray C++ SDK at <dir> (from download, Ray <ver>, sha256 <64-hex>)`

At build time, `rayrust-sys/build.rs` extracts `_ZL11kRayVersion` from `libray_api.so` ELF64 `SHT_SYMTAB` (or reads `../_version.py` when symbol tables are absent), verifies `>= 2.56.1`, records it as `RAYRUST_BUILD_RAY_VERSION`, probes `ray::RuntimeEnv::Deserialize` for `_GLIBCXX_USE_CXX11_ABI`, and checks the 136-byte `ray::RayObject` layout. At runtime, `Ray::connect` and worker callback entry points verify `kRayVersion` in the loaded `libray_api.so` against `RAYRUST_BUILD_RAY_VERSION`; in cluster mode, `Ray::connect` also queries GCS internal KV `("cluster", "CLUSTER_METADATA")` via `dlsym`-resolved `ray::gcs::GlobalStateAccessor::GetInternalKV` to verify that the cluster `ray_version` matches the build version (reporting `rayrust requires Ray >= 2.56.1` alongside the mismatch when the loaded library or cluster version is earlier than `2.56.1`).

## Key Design Decisions

| Decision | Reason |
|---|---|
| Wrap C++ SDK, not native rewrite | `libray_api.so` is 38MB of Bazel-built code |
| C ABI wrapper | C++ templates have no stable ABI |
| Opt-in `build.rs` download (`RAYRUST_DOWNLOAD=1`) | Offline by default; optional SHA-256-verified wheel fetch without requiring Python |
| Binary-safe IDs (`ptr + len`) | Ray ObjectIDs may contain null bytes |
| XLANG header auto-detection | Ray 2.56.1+ header format (non-zero padding) |
| `rmpv::Value` for dynamic types | Python returns unknown types at compile time |
| Persistent global tokio runtime | Avoid per-call runtime creation overhead |
| `#[actor]` macro | Reduces 40 lines of boilerplate to 10 |
| Thread-local `ray_last_error_bytes()` | Length-delimited error propagation from C++ to Rust; `last_error()` extracts formatted exception text with UTF-8 lossy fallback |
| `clear_error()` before FFI call | Prevents stale errors from prior operations |

## Design Paradigm

rayrust follows idiomatic Rust patterns, not C-in-Rust:

### RAII Context — `Ray`

All operations are methods on a `Ray` context object. All handles in a process share one runtime, and `shutdown()` runs when the last handle is dropped — impossible to forget cleanup, even on panic. A second `connect` with the same config returns the runtime that is already live instead of initializing it again; a different config is refused with `RayError::Runtime`. `Ray` is `!Clone` (pass `&Ray` to share).

```rust
let ray = Ray::connect(&config)?;  // init
let obj = ray.put(&42i32)?;         // method call
// drop(ray) → automatic shutdown
```

### Builder Pattern — `RayConfig`, `ActorOptions`, `TaskOptions`

All configuration uses chainable builder methods returning `Self`:

```rust
let config = RayConfig::new("127.0.0.1:6379")
    .node_ip("192.168.1.5")
    .namespace("production")
    .detached_actors();

let opts = ActorOptions::new()
    .name("counter")
    .max_restarts(3)
    .max_concurrency(10)
    .resource("GPU", 1.0);
```

### `'static` Futures — async methods don't borrow `&Ray`

`task_call_async` and `actor_call_async` return `impl Future + Send + 'static`. The `&Ray` reference is only needed to submit the task, not to poll the result. This allows spawning futures on `JoinSet` without lifetime issues:

```rust
// These futures can be spawned on JoinSet — no &ray borrow needed
let futs = (0..10).map(|i| add_remote_async(&ray, i, 1));
```

### Error Handling — `Result` everywhere, no hidden panics

`put`, `kill_actor`, `get_actor`, and every macro-generated caller return `Result`: argument serialization and task/actor submission errors both reach the caller instead of panicking. `Ray::connect` validates `RayConfig::address` and `RayConfig::runtime_env` before entering the SDK, so malformed `runtime_env` JSON (or a non-object JSON value) returns `Err` instead of terminating the process. The C ABI's thread-local `last_error()` is checked after FFI calls and `clear_error()` is called before to prevent stale errors. Failed tasks and actors carry the formatted exception text (such as a Python traceback) extracted from the error payload, falling back to UTF-8 lossy rendering of the raw bytes. Error types the C++ SDK does not translate return `RayError::Unclassified`: objects that record no error text (such as a cancelled task) carry a message explaining the condition and naming `TASK_CANCELLED`, while objects that carry an error frame (such as a failed `runtime_env` setup) are recognised before decoding, so `get`, `get_value`, `Vec<u8>` and the batch and async forms all surface the text Ray recorded instead of decoding the frame into a value. A payload counts as such a frame only when it matches the frame rules exactly (total length `9 + N`, a msgpack `bin` body, known protobuf field numbers and wire types, non-empty UTF-8 text), so a normal value is never reclassified; a payload that happens to match those rules is reported as `RayError::Unclassified` and is not decoded. Worker callbacks generated by `#[remote]` and `#[actor]` catch argument decoding errors and function-body panics and return them through the callback error channel as task errors, without unwinding across `extern "C"` to abort the worker process.

## Benchmark

Rust vs Python on Ray cluster (500 tasks):

![Benchmark](docs/benchmark.svg)

| Metric | Rust | Python | Speedup |
|---|---|---|---|
| Async throughput | 4744 tasks/sec | 1918 tasks/sec | **2.5x** |
| Latency (median) | 617µs | 950µs | **1.5x** |
| Compute (sum 0..1M) | 2.8ms | 652ms | **234x** |
| Async runtime (100×50ms) | 521ms (9.6x parallel) | — | — |

## Cluster Setup

### Multi-node C++ Actor Support

Rust actors and `#[remote]` tasks use the Ray C++ SDK's worker process (`default_worker`). Every node that may run them **must** have `ray[cpp]` installed, and the Rust worker library (`.so`) must exist at the same `code_search_path` path on that node:

```bash
# On ALL worker nodes (not just the driver node):
pip install "ray[cpp]==2.56.1"
```

If a node only has `pip install ray` (Python only, no `[cpp]` extra), it lacks:
- `ray/cpp/default_worker` — the C++ worker binary that Ray's raylet launches
- `ray/cpp/lib/libray_api.so` — the C++ SDK shared library

C++ actors scheduled to such nodes will crash immediately (`never_started: true`, `NODE_DIED`). Without `default_worker`, `ray start` passes an empty `--cpp_worker_command=` to the raylet, which then holds no C++ worker state, so the first C++ request aborts that node's raylet with `Check failed: state != states_by_lang_.end() Required Language isn't supported: CPP` (`src/ray/raylet/worker_pool.cc:1623`):

- **Driver's node**: `Ray::connect` registers the driver as a C++ worker, the local raylet aborts in `WorkerPool::RegisterDriver`, and the driver exits with code 1 after `Failed to register worker to Raylet: IOError: Failed to read data from the socket: End of file`.
- **Worker node**: the `#[remote]` task is submitted and its lease reaches that node, whose raylet aborts in `WorkerPool::PrestartWorkers`; GCS marks the node `UNEXPECTED_TERMINATION` (`health check failed due to missing too many heartbeats`), the driver logs `Retrying attempt to schedule lease ... Disconnected: GRPC client is shut down`, and `get()` waits for a node that has CPUs and the C++ worker.

**Symptom**: `actor_create()` succeeds (returns an actor ID), but `actor_call()` hangs until timeout. Driver log shows `NODE_DIED` / `health check failed due to missing too many heartbeats`.

**Quick check** — verify C++ SDK on each node:
```bash
ssh <worker-node> 'test -f $(python3 -c "import ray,os;print(os.path.join(os.path.dirname(ray.__file__),\"cpp\",\"default_worker\"))") && echo "C++ SDK OK" || echo "MISSING: install ray[cpp]"'
```

**Workaround**: If only the driver node has the C++ SDK, use local mode or single-node cluster:
```bash
ray start --head --port=6380  # on the node with ray[cpp] installed
```

### Remote Tasks vs Actors

| Feature | Needs `ray[cpp]` on worker nodes? | Why |
|---|---|---|
| `put` / `get` / `wait` | No | Runs in driver process |
| `#[remote]` task | **Yes** | The raylet starts `default_worker` on the node that runs the task; the Rust worker library must also be reachable at the `code_search_path` path there |
| `#[actor]` (cluster mode) | **Yes** | Ray launches `default_worker` on a worker node |
| `#[actor]` (local mode) | No | Everything runs in-process |
| Python task/actor (xlang) | No | Uses Python workers (available everywhere) |

### Worker `.so` RPATH (dlopen compatibility)

When Ray's `default_worker` dlopens your `librayrust_worker.so`, the dynamic linker searches for `libray_api.so` (a NEEDED dependency) using the `.so`'s own `DT_RPATH` — **not** `LD_LIBRARY_PATH` from the calling process. Without RPATH, the worker `.so` fails to load with `libray_api.so: cannot open shared object file`.

The `rayrust-example-worker` build script sets this automatically:

```bash
# Verify: should show DT_RPATH (not DT_RUNPATH)
readelf -d target/release/librayrust_worker.so | grep RPATH
# 0x000000000000000f (RPATH) Library rpath: [$ORIGIN:/path/to/ray/cpp/lib]
```

**If you build your own worker crate** (not using `rayrust-example-worker`), add this to your `build.rs`:

```rust
fn main() {
    // ... locate ray_cpp_dir (RAY_CPP_DIR or pip-installed) ...

    let lib_dir = std::path::PathBuf::from(&ray_cpp_dir).join("lib");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    // DT_RPATH: searched by dlopen, inherited by transitive deps.
    //   1. $ORIGIN — copy libray_api.so next to your worker .so (deployment)
    //   2. absolute path — same machine builds and runs (dev)
    println!("cargo:rustc-link-arg-cdylib=-Wl,--disable-new-dtags");
    println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,$ORIGIN");
    println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,{}", lib_dir.display());

    // Force libray_api.so into NEEDED (linker may drop unreferenced libs).
    println!("cargo:rustc-link-arg-cdylib=-Wl,--no-as-needed");
    println!("cargo:rustc-link-arg-cdylib=-lray_api");
    println!("cargo:rustc-link-arg-cdylib=-Wl,--as-needed");
}
```

| Deployment method | How it works |
|---|---|
| **Dev (same machine)** | RPATH entry #2 points to `ray/cpp/lib` — automatic |
| **Cluster (copy .so)** | Copy `libray_api.so` to the same directory as your worker `.so` — RPATH entry #1 (`$ORIGIN`) finds it |
| **Cluster (shared path)** | Set `RAY_CPP_DIR` at build time so RPATH points to the worker node's `ray/cpp/lib` |

## Examples

| Example | Description |
|---|---|
| `hello_ray` | Basic put/get/init |
| `full_test` | All features (tasks, actors, xlang, placement groups) |
| `async_demo` | Concurrent async tasks with tokio |
| `xlang_complex` | Cross-language complex types (11 tests) |
| `actor_e2e` | Rust actor e2e with `#[actor]` macro |
| `raybench` | Performance benchmark (Rust vs Python) |

## Continuous Integration

GitHub Actions (`.github/workflows/ci.yml`) runs on `ubuntu-24.04` (`linux/x86_64` only; macOS, Windows, and `linux/aarch64` are not tested in CI) using `actions/setup-python@v7` (Python 3.12):

- **`build` job** — downloads and extracts the `ray-cpp` wheel for each Ray version in the matrix (`2.56.1`, `2.58.2`, `2.60.1`), sets `RAY_CPP_DIR` to the extracted `ray/cpp`, and runs `cargo build --release --all-targets`, `cargo test`, `cargo test --release`, `cargo test --doc`, and `cargo doc --no-deps`. On the `2.60.1` matrix entry it also unsets `RAY_CPP_DIR` with a fresh `RAYRUST_CACHE_DIR` and `RAYRUST_DOWNLOAD=1` to verify the opt-in PyPI download branch on a cold cache (the warning names the wheel digest: `(from download, Ray 2.60.1, sha256 <digest>)`) followed by cache reuse (`(from cache, Ray 2.60.1, sha256 <digest>)`).
- **`cluster` job** — installs `ray[cpp]==2.56.1` via `actions/setup-python@v7` (Python 3.12) without setting `RAY_CPP_DIR` (exercising `python3` SDK discovery), sets `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` so an object store larger than `/dev/shm` falls back to Ray's own temporary directory, starts a single-node Ray cluster (`ray start --head --num-cpus=2`), runs the `full_test`, `actor_e2e`, `async_demo`, `error_kinds_test`, `mixed_batch_test`, `worker_runtime_test`, `actor_soundness_test`, `xlang_decode_test`, `object_ref_args_test`, and `shutdown_gate_test` examples, and stops the cluster with `ray stop --force`.

`cargo fmt --check` and `cargo clippy` are omitted because the repository carries pre-existing `rustfmt` diffs and `clippy` warnings on `main`.

## License

Apache-2.0 (same as Ray)
