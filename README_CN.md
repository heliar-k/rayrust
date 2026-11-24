# rayrust

[English](README.md) | [中文](README_CN.md)

Ray 的 Rust SDK —— 通过 FFI 封装 Ray C++ SDK，提供地道的 Rust API 来使用 Ray 的核心分布式原语：对象存储、远程任务、Actor、Placement Group 和跨语言调用。

## 特性

| 特性 | 本地模式 | 集群模式 |
|---|:---:|:---:|
| `Ray::connect` / `drop`（RAII 生命周期管理） | ✅ | ✅ |
| `put` / `get` / `wait` | ✅ | ✅ |
| `get_many` / `get_many_timeout`（批量获取，可带期限；一次 `Wait` 加逐元素 `Get`，见 #41） | ✅ | ✅ |
| `get_many_async` / `get_many_timeout_async` / `wait_async`（批量获取与 `Wait` 跑在 tokio blocking pool 上） | ✅ | ✅ |
| `#[remote]` 同步任务 | ✅ | ✅ |
| `#[remote]` 异步任务（持久化 runtime） | ✅ | ✅ |
| `#[actor]` 宏（自动生成 factory + 方法） | ✅ | ✅ |
| `actor_call_async`（非阻塞） | ✅ | ✅ |
| `get_async`（带期限的 `Get` 跑在 tokio blocking pool 上） | ✅ | ✅ |
| Python 任务（跨语言，复杂类型） | ✅ | ✅ |
| Python Actor（跨语言；本地模式拒绝创建（#68）与调用（#59）） | ❌ | ✅ |
| 资源调度（`TaskOptions` / `ActorOptions` builder） | ✅ | ✅ |
| `ActorOptions`：name、namespace、max_restarts、max_concurrency、runtime_env、placement_group | ✅ | ✅ |
| `ActorLifetime::Detached`（detached actor） | ✅ | ✅ |
| `RayConfig.namespace`（job 级命名空间） | ✅ | ✅ |
| PlacementGroup | ✅ | ✅ |
| `get_actor`（命名 actor，跨 namespace） | ✅ | ✅ |
| `cancel`（任务取消；本地模式没有取消路径，返回错误，见 #58） | ❌ | ✅ |
| `kill_actor`（本地模式返回错误，actor 随进程结束，见 #53） | ❌ | ✅ |
| XLANG header 自动检测（兼容 Ray 2.51.1+） | ✅ | ✅ |
| `rmpv::Value` 动态反序列化 | ✅ | ✅ |
| `put_xlang`（把 xlang 头写进存入的数据；Python 侧仍因缺 metadata 拒收，见 #21） | ✅ | ✅ |
| `ray_last_error_bytes()`（带长度的线程局部错误字节；Rust 提取格式化异常文本，失败时按 UTF-8 lossy 回退） | ✅ | ✅ |

## 快速开始

### 前置条件

通过预先安装的 `ray[cpp]`、`RAY_CPP_DIR` 环境变量或显式开启 PyPI 下载（`RAYRUST_DOWNLOAD=1`）提供 Ray C++ SDK：

```bash
git clone https://github.com/NolanHo/rayrust.git
cd rayrust
RAYRUST_DOWNLOAD=1 cargo build --release -p rayrust-example-worker
```

设置 `RAYRUST_DOWNLOAD=1` 时，首次构建从 PyPI 下载 `ray-cpp` wheel、校验 SHA-256 摘要并解压 C++ SDK 到 `~/.cache/rayrust/`；后续构建直接复用缓存，无需继续设置 `RAYRUST_DOWNLOAD=1`。

使用预先安装的 Ray C++ SDK（如 `pip install ray[cpp]`）：

```bash
export RAY_CPP_DIR=/path/to/site-packages/ray/cpp
```

### 本地模式运行（无需集群）

```bash
cargo run --example hello_ray
```

### 集群模式运行

```bash
export RAY_ADDRESS='<head节点IP>:6379'
export RAY_NODE_IP='<本机IP>'
export RAY_WORKER_SO="$(pwd)/target/release/librayrust_worker.so"

cargo run --example full_test
```

在 Linux 上，构建过程会把 SDK 的 `lib` 目录写进本仓库构建出的二进制、测试与 worker `.so` 的 RPATH，`cargo test` 与 `cargo run --example ...` 可直接使用；`LD_LIBRARY_PATH` 仅在构建完成后移动了 SDK 目录时才需要。把自己的 crate 依赖 `rayrust` 时不会继承这些条目：需要在自身构建配置里加链接参数，或在运行它的时候设置 `LD_LIBRARY_PATH`。

## 使用方式

### 远程任务（`#[remote]`）

支持同步和 `async fn`。异步函数使用持久化全局 tokio runtime（创建一次，复用调用）。

```rust
use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[rayrust::remote]
async fn fetch(url: String) -> Vec<u8> { /* ... */ }

#[tokio::main]
async fn main() -> Result<(), RayError> {
    let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;

    // 异步获取：一次带期限的 Get 跑在 tokio blocking pool 上，tokio 线程不被占用
    let r = add_remote(&ray, 1, 2)?;
    let v: i32 = r.get_async().await?;
    println!("add(1, 2) = {}", v);

    // 异步提交（并发）
    let r = add_remote_async(&ray, 10, 20).await?;
    let v: i32 = r.get_async().await?;

    drop(ray);
    Ok(())
}
```

### 引用参数

引用可以顶替一个值参数：传 `ObjectRef<T>`（句柄转移进调用）或 `&ObjectRef<T>`（调用方保留句柄，调用器克隆一份），驱动不必先 `get`。集群模式下 core worker 把对象的值投递给工人，本地模式下 C 包装层在提交前取成值。生成的调用器自己持有一份引用直到提交的 FFI 调用返回，所以异步 future 轮询之前 drop 掉自己的句柄是安全的。

```rust
let a = ray.put(&40i32)?;
let sum: ObjectRef<i32> = add_remote(&ray, 2, &a)?;  // 等价于 add_remote(&ray, 2, 40)
let v: i32 = sum.get_async().await?;                 // 42
```

宏生成的调用器都返回 `Result`：参数序列化错误与提交错误都回到调用方，不变成 panic。actor 方法的引用参数写法相同（`counter_increment(&ray, &handle, &a)`）。

这条能力有两条限制。驱动自己 put 出来的引用只能交给 Rust 任务与 actor：Python 拒绝没有 metadata 的对象，而 C++ SDK 的 `put` 不写 metadata。Rust 任务也不能把可用的引用交回来：它交回的 id 不附带嵌套引用，驱动侧也没有这个 id 的 owner 路径，所以读这个 id 得到 `ObjectUnknownOwner`，对象活着也一样。两条限制都写在 `rayrust::TaskArg` 的文档里。

### Actor（`#[actor]` 宏）

`#[rayrust::actor]` 宏自动生成 factory callback、成员函数 callback、`#[ctor]` 注册和便捷调用函数：

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

    // 通过宏生成的 factory 创建 Actor
    let arg = serialize(&100i64)?;
    let handle = ray.actor_create(
        "__rayrust_actor_factory_counter", &[&arg], &ActorOptions::new()
    )?;

    // 异步调用方法
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

### 跨语言调用（Python）

Rust ↔ Python，自动处理 XLANG header，支持复杂类型：

```rust
// Python → Rust: list、dict、nested、None、混合类型
let obj = ray.task_call_python("my_module", "return_list", &[], &[])?;
let val: Vec<i64> = obj.cast().get_async().await?; // [1, 2, 3, 4, 5]

// 动态类型（返回类型未知时）
let obj = ray.task_call_python("my_module", "complex_func", &[], &[])?;
let val = obj.get_value_async().await?; // rmpv::Value

// Rust → Python: 发送复杂参数
let arg = serialize(&vec![1i64, 2, 3])?;
let obj = ray.task_call_python("my_module", "echo_list", &[&arg], &[])?;
```

### 资源调度

为 task 和 actor 请求 CPU/GPU 资源：

```rust
// 带 GPU 的 task
let obj = ray.task_call(
    "train_model", &args, &[], &TaskOptions::new().resource("GPU", 1.0).resource("CPU", 4.0)
)?;

// 带资源的 actor
let handle = ray.actor_create(
    "gpu_actor_factory", &args, &ActorOptions::new().resource("GPU", 1.0)
)?;
```

### 对象存储

```rust
let obj = ray.put(&42i32)?;
let val: i32 = ray.get(&obj)?;

// 异步 put/get
let obj = ray.put_async(42i32).await?;
let val: i32 = obj.get_async().await?;

// 批量获取
let vals = ray.get_many(&[obj1, obj2, obj3])?;

// 等待就绪
let (ready, unready) = ray.wait(&[obj1, obj2], 2, 5000)?;
```

`get_many*` 先按去重后的 ID 调用一次 `Wait`，再对每个就绪元素执行一次单对象 `Get`
（最多每个输入槽位一次）：上游 C++ SDK 的批量 `Get` 在含有异常对象的混合批次中会
留下空 `RayObject` 指针并在 `NativeObjectStore::GetRaw` 中直接解引用触发段错误
（见 #41）。这次 `Wait` 不会因为批次里已有失败对象而提前返回，所以含多个元素的批次
会等到全部就绪或期限到期：无期限的 `get_many` 会阻塞到每个元素就绪，带期限的入口
在期限到期后才报告首个失败元素。

集群模式（core worker 已初始化）下，`put` 与任务、actor 调用返回的 `ObjectRef` 为
它的 id 持有一个 Ray 本地引用：`Clone` 加一个、`Drop` 减一个，只要还有这样的引用
活着对象就活着，最后一个被 drop 之后对象可能被释放。本地模式与 Ray 工人进程不参与
引用计数，在那里 drop 引用不会释放任何东西。从 id 字节重建的引用
（`ObjectRef::from(id_bytes)`）不持有引用：对象能不能活到你读它的时候取决于别处
是否还有引用，读取时用 `get_timeout`。

## 架构

```
Rust 应用代码
    |
    v
rayrust（安全 Rust API）
    Ray (RAII context: connect / drop)
    ObjectRef<T>, ActorHandle, ActorOptions, TaskOptions
    TaskArg / TaskArgs（每个参数是值或 &ObjectRef<T>）
    #[remote]/#[actor] 宏 → &Ray 调用器
    get_async (带期限的 `Get` 跑在 tokio blocking pool 上), 持久化 tokio runtime
    |
    v
rayrust-sys（FFI 绑定）
    extern "C" 声明, build.rs (SDK 查找；通过 RAYRUST_DOWNLOAD=1 显式开启 PyPI 下载)
    |
    v
ray_c.h / ray_c.cc（C ABI 封装层）
    类型擦除的 C 接口, 二进制安全的 ID
    线程局部错误消息, 资源调度
    |
    v
libray_api.so（Ray C++ SDK）  ->  Ray Core (raylet / GCS / object store)
```

## 构建系统

各包的 `build.rs` 共用 `rayrust-build` 获取 Ray C++ SDK：

1. **`RAY_CPP_DIR` 环境变量** — 使用指定的 SDK 目录，按给定值使用，不做版本比对
2. **`~/.cache/rayrust/ray-cpp-{version}-{platform}/`**（或 `$RAYRUST_CACHE_DIR/ray-cpp-{version}-{platform}/`）— 共享缓存（debug + release）
3. **`python3`（`ray[cpp]`）** — 在 `sys.path` 中查找已安装的 `ray/cpp/`，要求其 `ray` 版本与 `RAY_VERSION` 一致
4. **显式开启从 PyPI 下载（`RAYRUST_DOWNLOAD=1`）** — 仅在启用 `RAYRUST_DOWNLOAD`（`1`/`true`/`yes`；`0`/`false`/`no` 或未设置时关闭）时执行：从 PyPI JSON 索引获取对应版本的 `ray-cpp` wheel，按 `digests.sha256` 流式校验 SHA-256（摘要写进缓存旁边的 `ray-cpp-{version}-{platform}.sha256`），解开 `ray/cpp/`。不设置该变量时构建不发出任何网络请求，失败信息列出取得 SDK 的三种办法（设置 `RAY_CPP_DIR`、`pip install ray[cpp]` 或设置 `RAYRUST_DOWNLOAD=1`）。

`RAY_VERSION`（默认 `2.51.1`）决定第 2 步的缓存目录名、第 3 步的版本匹配与第 4 步下载的 wheel；第 1 步直接使用 `RAY_CPP_DIR` 给出的目录，不比对版本。缓存目录旁的 `.sha256` pin 文件记录下载时的产物身份；缓存命中时读取并校验该 pin 文件格式，每次构建不重新计算 SDK 目录内容的哈希。

`RAY_CPP_DIR` 与 `RAYRUST_CACHE_DIR` 中的相对路径按执行 `cargo` 的目录（`PWD`；`PWD` 未设时按工作区根）解释；`~` 不做展开，需要家用目录时写 `$HOME`。

构建警告会打印解析到的 SDK 路径、来源与 Ray 版本：

- `cargo:warning=Using Ray C++ SDK at <dir> (from RAY_CPP_DIR, version not checked)`
- `cargo:warning=Using Ray C++ SDK at <dir> (from cache, Ray <ver>)`（存在 `.sha256` pin 文件时为 `(from cache, Ray <ver>, sha256 <64 个十六进制字符的摘要>)`）
- `cargo:warning=Using Ray C++ SDK at <dir> (from python3, Ray <ver>)`
- `cargo:warning=Using Ray C++ SDK at <dir> (from download, Ray <ver>, sha256 <64 个十六进制字符的摘要>)`

wheel 附带的公开头文件（`include/ray/api.h`、`include/ray/api/*.h`）没有版本查询，也没有 GCS 访问入口；动态库 `libray_api.so` 导出了 `ray::gcs::GlobalStateAccessor`（`Connect`、`GetInternalKV`、`GetAllNodeInfo` 等）与 `GcsClient` 相关的 C++ 符号（可通过 `nm -D --defined-only libray_api.so | grep -E 'GlobalStateAccessor|GcsClient' | c++filt` 查看），这些符号都带有 `@@VERSION_1.0` 后缀且属于私有符号，SDK 未附带对应声明，调用它们需要手写私有声明并承接 `std::string` ABI（#19 记录了这类依赖）；导出的全部符号均不携带 Ray 版本号（`nm -D --defined-only libray_api.so | grep -i version | grep -Ev 'VERSION_1.0|FLAGS_'`）。上游在内部路径保存集群版本：GCS 在 `GcsNodeManager::HandleCheckAlive`（`src/ray/gcs/gcs_node_manager.cc:154-166`，`:158` 赋值）中写入 `CheckAliveReply.ray_version` 且仅有 raylet 调用 `CheckAlive`（`src/ray/raylet/node_manager.cc:442`），Python 侧则通过 `get_cluster_metadata` 读取 GCS 内部 KV 的 cluster metadata（`ray/_common/usage/usage_lib.py:841-862`）并在 `_check_ray_version`（`ray/scripts/scripts.py:69-79`）中拿它与 `ray.__version__` 比对。运行时的集群版本自动比对因此推迟到 #19；构建警告会输出解析到的版本，请与集群侧 `ray.__version__` 手动对照。

## 关键设计决策

| 决策 | 原因 |
|---|---|
| 封装 C++ SDK，非原生重写 | `libray_api.so` 是 38MB 的 Bazel 产物 |
| C ABI 封装层 | C++ 模板无稳定 ABI |
| `build.rs` 显式下载开关（`RAYRUST_DOWNLOAD=1`） | 默认不联网；开启后经 SHA-256 校验获取 wheel，构建无需依赖 Python 环境 |
| 二进制安全 ID（`ptr + len`） | Ray ObjectID 可能包含 null 字节 |
| XLANG header 自动检测 | Ray 2.51.1+ 改变了 header 格式（非零 padding） |
| `rmpv::Value` 动态类型 | Python 返回类型编译时未知 |
| 持久化全局 tokio runtime | 避免每次调用创建 runtime 的开销 |
| `#[actor]` 宏 | 将 40 行样板代码减少到 10 行 |
| 线程局部 `ray_last_error_bytes()` | 带长度的错误字节从 C++ 传到 Rust，`last_error()` 提取格式化异常文本并在无法提取时按 UTF-8 lossy 呈现原始字节 |
| FFI 调用前 `clear_error()` | 防止上次操作的残留错误被误报 |

## 编程范式

rayrust 遵循地道的 Rust 设计模式，而非 C 风格的全局函数：

### RAII Context — `Ray`

所有操作都是 `Ray` 上下文对象的方法。同一进程里的所有句柄共享一个运行时，`shutdown()` 在最后一个句柄 drop 时执行 —— 不可能忘记清理，即使 panic 也能正确关闭。同配置的第二次 `connect` 返回已经在运行的运行时，不会重新初始化；配置不同则返回 `RayError::Runtime`。`Ray` 是 `!Clone`（传 `&Ray` 共享）。

```rust
let ray = Ray::connect(&config)?;  // 初始化
let obj = ray.put(&42i32)?;         // 方法调用
// drop(ray) → 自动 shutdown
```

### Builder 模式 — `RayConfig`、`ActorOptions`、`TaskOptions`

所有配置类使用链式 builder 方法，返回 `Self`：

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

### `'static` Future — async 方法不借用 `&Ray`

`task_call_async` 和 `actor_call_async` 返回 `impl Future + Send + 'static`。`&Ray` 引用仅在提交任务时需要，不跨 `.await` 存活。因此可以在 `JoinSet` 上 spawn，无生命周期问题：

```rust
// 这些 future 可以 spawn 到 JoinSet —— 不需要借用 &ray
let futs = (0..10).map(|i| add_remote_async(&ray, i, 1));
```

### 错误处理 —— 全部 `Result`，无隐藏 panic

`put`、`kill_actor`、`get_actor` 与全部宏生成的调用器都返回 `Result`：参数序列化错误与任务、actor 提交错误都回到调用方，不变成 panic。C ABI 的线程局部 `last_error()` 在 FFI 调用后检查，调用前 `clear_error()` 防止残留错误。失败的任务与 actor 从错误载荷中提取格式化异常文本（如 Python traceback），无法提取时按 UTF-8 lossy 呈现原始字节。C++ SDK 未翻译的错误类型以 `RayError::Unclassified` 返回：不带错误文本的对象（如已取消的任务）由消息说明该情况并点名 `TASK_CANCELLED`；携带错误帧的对象（如 `runtime_env` 安装失败）在解码之前就被识别，因此 `get`、`get_value`、`Vec<u8>` 以及批量与异步入口都返回 Ray 记录的错误文本，不把帧字节当成值解出来。判定要求载荷严格满足帧规则（总长为 `9 + N`、正文是 msgpack `bin`、protobuf 顶层字段号与 wire type 都在允许集合内、文本非空且是合法 UTF-8），正常值不会被改判；恰好满足这些规则的载荷会以 `RayError::Unclassified` 返回，该载荷因此不再参与解码。

Rust vs Python 在 Ray 集群上（500 个任务）：

![Benchmark](docs/benchmark.svg)

| 指标 | Rust | Python | 加速比 |
|---|---|---|---|
| 异步吞吐 | 4744 tasks/sec | 1918 tasks/sec | **2.5x** |
| 延迟（中位数） | 617µs | 950µs | **1.5x** |
| 计算（sum 0..1M） | 2.8ms | 652ms | **234x** |
| 异步 runtime（100×50ms） | 521ms（9.6x 并行） | — | — |

## 集群配置

### 多节点 C++ Actor 支持

Rust Actor 与 `#[remote]` 任务都使用 Ray C++ SDK 的 worker 进程（`default_worker`）。每个可能运行它们的节点**必须**安装 `ray[cpp]`，并且该节点的相同 `code_search_path` 路径下必须存在对应的 Rust worker 动态库（`.so`）：

```bash
# 在所有 worker 节点上（不仅仅是 driver 节点）：
pip install "ray[cpp]==2.51.1"
```

如果节点只安装了 `pip install ray`（仅 Python，没有 `[cpp]`），则缺少：
- `ray/cpp/default_worker` — Ray raylet 启动的 C++ worker 二进制
- `ray/cpp/lib/libray_api.so` — C++ SDK 共享库

C++ Actor 被调度到此类节点会立即崩溃（`never_started: true`，`NODE_DIED`）。缺少 `default_worker` 时，`ray start` 传给 raylet 的 `--cpp_worker_command=` 为空，该 raylet 因此没有任何 C++ worker 状态，第一个 C++ 请求就会以 `Check failed: state != states_by_lang_.end() Required Language isn't supported: CPP`（`src/ray/raylet/worker_pool.cc:1623`）终止该节点的 raylet 进程：

- **driver 所在的节点**：`Ray::connect` 把 driver 作为 C++ worker 注册，本机 raylet 在 `WorkerPool::RegisterDriver` 中终止，driver 在打印 `Failed to register worker to Raylet: IOError: Failed to read data from the socket: End of file` 后以退出码 1 结束。
- **执行任务的 worker 节点**：`#[remote]` 任务提交成功，租约到达该节点后其 raylet 在 `WorkerPool::PrestartWorkers` 中终止；GCS 因心跳超时把该节点记为 `UNEXPECTED_TERMINATION`（`health check failed due to missing too many heartbeats`），driver 日志出现 `Retrying attempt to schedule lease ... Disconnected: GRPC client is shut down`，`get()` 持续等待具备 CPU 与 C++ worker 的节点。

**症状**：`actor_create()` 成功（返回 actor ID），但 `actor_call()` 超时挂起。Driver 日志显示 `NODE_DIED`。

**快速检查** — 验证各节点的 C++ SDK：
```bash
ssh <worker-node> 'test -f $(python3 -c "import ray,os;print(os.path.join(os.path.dirname(ray.__file__),\"cpp\",\"default_worker\"))") && echo "C++ SDK OK" || echo "缺失: 请安装 ray[cpp]"'
```

**变通方案**：如果只有 driver 节点有 C++ SDK，使用 local mode 或单节点集群：
```bash
ray start --head --port=6380  # 在安装了 ray[cpp] 的节点上
```

### 远程任务 vs Actor

| 功能 | 需要 worker 节点安装 `ray[cpp]`？ | 原因 |
|---|---|---|
| `put` / `get` / `wait` | 否 | 在 driver 进程内执行 |
| `#[remote]` task | **是** | 执行任务的节点由 raylet 启动 `default_worker`，且该节点的相同 `code_search_path` 路径下必须能访问 Rust worker 动态库 |
| `#[actor]`（集群模式） | **是** | Ray 在 worker 节点启动 `default_worker` |
| `#[actor]`（本地模式） | 否 | 全部在进程内 |
| Python task/actor（xlang） | 否 | 使用 Python worker（所有节点可用） |

### Worker `.so` RPATH（dlopen 兼容性）

Ray 的 `default_worker` 通过 dlopen 加载 `librayrust_worker.so` 时，动态链接器使用 `.so` 自身的 `DT_RPATH` 搜索 `libray_api.so`（NEEDED 依赖）——**不使用**调用进程的 `LD_LIBRARY_PATH`。如果没有 RPATH，worker `.so` 加载失败，报 `libray_api.so: cannot open shared object file`。

`rayrust-example-worker` 的 build script 已自动设置：

```bash
# 验证：应显示 DT_RPATH（不是 DT_RUNPATH）
readelf -d target/release/librayrust_worker.so | grep RPATH
# 0x000000000000000f (RPATH) Library rpath: [$ORIGIN:/path/to/ray/cpp/lib]
```

**如果你构建自己的 worker crate**（不使用 `rayrust-example-worker`），在 `build.rs` 中添加：

```rust
fn main() {
    // ... 定位 ray_cpp_dir（RAY_CPP_DIR 或 pip 安装路径）...

    let lib_dir = std::path::PathBuf::from(&ray_cpp_dir).join("lib");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    // DT_RPATH：dlopen 搜索，传递依赖继承。
    //   1. $ORIGIN — 把 libray_api.so 复制到 worker .so 旁边（部署）
    //   2. 绝对路径 — 同一台机器编译和运行（开发）
    println!("cargo:rustc-link-arg-cdylib=-Wl,--disable-new-dtags");
    println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,$ORIGIN");
    println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,{}", lib_dir.display());

    // 强制 libray_api.so 进入 NEEDED（链接器可能丢弃未被引用的库）。
    println!("cargo:rustc-link-arg-cdylib=-Wl,--no-as-needed");
    println!("cargo:rustc-link-arg-cdylib=-lray_api");
    println!("cargo:rustc-link-arg-cdylib=-Wl,--as-needed");
}
```

| 部署方式 | 原理 |
|---|---|
| **开发（同一台机器）** | RPATH #2 指向 `ray/cpp/lib` — 自动生效 |
| **集群（复制 .so）** | 把 `libray_api.so` 复制到 worker `.so` 同目录 — RPATH #1（`$ORIGIN`）命中 |
| **集群（共享路径）** | 构建时设 `RAY_CPP_DIR`，使 RPATH 指向 worker 节点的 `ray/cpp/lib` |

## 示例

| 示例 | 描述 |
|---|---|
| `hello_ray` | 基本的 put/get/init |
| `full_test` | 全功能测试（task、actor、xlang、placement group） |
| `async_demo` | tokio 并发异步任务 |
| `xlang_complex` | 跨语言复杂类型（11 项测试） |
| `actor_e2e` | Rust actor 端到端测试（`#[actor]` 宏） |
| `raybench` | 性能基准（Rust vs Python） |

## 持续集成

GitHub Actions（`.github/workflows/ci.yml`）在 `ubuntu-24.04` 上通过 `actions/setup-python@v7`（Python 3.12）运行，仅验证 `linux/x86_64` 平台（CI 未测试 macOS、Windows 与 `linux/aarch64`）：

- **`build` 任务** — 按版本矩阵（`2.49.0`、`2.51.1`、`2.55.0`、`2.60.0`）逐一下载并解压 `ray-cpp` wheel，将 `RAY_CPP_DIR` 指向解压出的 `ray/cpp` 目录，依次执行 `cargo build --release --all-targets`、`cargo test`、`cargo test --release`、`cargo test --doc` 与 `cargo doc --no-deps`；并在 `2.60.0` 矩阵项上清除 `RAY_CPP_DIR`、设置 `RAYRUST_DOWNLOAD=1` 与全新 `RAYRUST_CACHE_DIR` 验证显式开启的 PyPI 下载分支（首次空缓存下载的警告带有 wheel 摘要：`(from download, Ray 2.60.0, sha256 <digest>)`，再次构建复用缓存断言 `(from cache, Ray 2.60.0, sha256 <digest>)`）。
- **`cluster` 任务** — 通过 `actions/setup-python@v7`（Python 3.12）安装 `ray[cpp]==2.51.1` 且不设置 `RAY_CPP_DIR`（验证通过 `python3` 查找已安装 SDK 的路径），设置 `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` 使对象存储大小超过 `/dev/shm` 时回退到 Ray 自己的临时目录，随后启动单节点集群（`ray start --head --num-cpus=2`），依次运行 `full_test`、`actor_e2e`、`async_demo` 与 `error_kinds_test` 四个示例，最后执行 `ray stop --force` 停止集群。

CI 未包含 `cargo fmt --check` 与 `cargo clippy`：仓库在 `main` 分支上已存在既有的 `rustfmt` 格式差异与 `clippy` 警告，`cargo fmt --check` 在 `main` 上无法通过。

## License

Apache-2.0（与 Ray 一致）
