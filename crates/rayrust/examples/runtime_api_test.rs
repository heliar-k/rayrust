//! Example: #8 补齐的 `RayRuntime` 接口——`is_local_mode`、placement group 的等待与
//! 查询、actor handle 的序列化、`exit_actor`。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/runtime_api_test cluster
//! ./target/release/examples/runtime_api_test local
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! # 检查项
//!
//! 1. `is_local_mode`：集群 `false`、本地 `true`。同一项还断言 `namespace()`：集群返回
//!    job config 的 namespace，本地返回 init 时配置的那一个（两边都配了固定名字）。
//! 2. placement group 全流程（集群）：建带名字的单 bundle 组（`CPU 0.5`、
//!    `STRICT_PACK`）→ `wait_ready(10)` 为 `true` → `by_id` 的 id 与建组返回一致、
//!    name 一致、`state == 2`（CREATED）、`strategy` 与建组时传的一致 → `by_name`
//!    同上 → `placement_groups()` 里按 id 找到它 → 移除 → 有界重试直到
//!    `by_id` 的 `state == 3`（REMOVED）、`by_name` 报错、`placement_groups()` 里
//!    那一项也是 REMOVED。
//! 3. 非法与未知（集群）：17 / 19 / 空字节 id → `Ffi` 错误，随后合法调用照常完成；
//!    未知 18 字节 id → 错误且消息里带 id；未知名字 → 错误且消息里带名字。
//! 4. `exit_actor`：`max_restarts=2` 的 actor，方法里调用 `exit_actor()`。
//!    集群模式：`exit()` 的对象用 `get_timeout` 取（退出路径不写结果，无界 `get` 会永远等），
//!    随后同一 handle 的普通方法必须失败；再用一个独立 actor 做正向对照——
//!    `increment(2)` 把计数推到 9、`kill_actor(no_restart = false)` 之后有界重试
//!    （200ms × 15）直到计数回到初值 7，证明这个集群真的会重启、
//!    「`exit()` 之后拿不到值」不是因为重启机制失效。本地模式：`exit_actor()` 是空操作，
//!    `increment_and_exit(2)` 返回自己的值 9、actor 仍然可用且状态不变。
//! 5. 任务里调 `exit_actor()`：集群模式任务失败且消息里含 `exit_actor`；本地模式
//!    `exit_actor()` 是空操作，任务照常返回它自己的值（1）。
//! 6. handle 交接（集群）：actor 方法返回数值；`handle.serialize()` 的字节与裸
//!    `handle.id()` 一起交给工人任务、原样交回；驱动从**交回来的字节**复原 handle、
//!    调方法、断言数值。另加一条：畸形 handle 字节让 `from_serialized` 报错，而不是
//!    交出一个永远不就绪的 nil handle。
//! 7. 本地模式：第 2、3 项与第 6 项的集群部分跳过并打印原因；第 1 项、第 4、5、8 项的
//!    本地模式断言、本地模式下 `wait_ready` 与三个查询的报错、`kill_actor` 的报错（之后
//!    同一个 actor 仍然可用）、两个 handle 接口的报错实际跑。
//! 8. 构造函数里调 `exit_actor()`：集群模式 `actor_create` 照常返回 handle、第一次调用
//!    报 `RayError::Actor`；本地模式那个调用是空操作，actor 正常创建、方法拿到值 11。
//!
//! # 实测出来的上游行为（第 2、4、6 项按它写）
//!
//! - 移除一个 placement group 之后 GCS **不会删掉表项**：`RemovePlacementGroup`
//!   把行改成 `REMOVED` 之后 `PlacementGroupTable().Put(...)`
//!   （`src/ray/gcs/gcs_placement_group_manager.cc:458-469`），表项一直留着，
//!   所以移除后 `by_id` 仍然成功、`state` 是 3；名字映射在这一步被删掉
//!   （同文件 414-427），`by_name` 因此报错。
//! - `wait_ready` 的时限原样交给 GCS：正数与 `0` 等这次 GCS 查询（`0` 在已就绪的组上
//!   实测仍然返回 `true`），负数的截止时间已经过期，即使组已就绪也返回 `false`。
//! - actor id 是 16 字节（12 个唯一字节 + 4 字节 JobID，`src/ray/common/id.h:124-142`），
//!   不是 ObjectID 的 28 字节。
//! - 工人进程里 `ActorHandle::from_serialized` 与裸 id 的调用都返回
//!   `Ffi("... Ray runtime is not initialized")`：`GetRayRuntime()` 在工人 `.so` 里
//!   是一份没有初始化的 per-DSO 副本（#38）。所以第 6 项的数值断言在驱动侧做，
//!   工人侧只作为观察打印。
//! - 本地模式没有 core worker，`exit_actor()` 在那里是空操作：哨兵异常会被
//!   `TaskExecutor::Invoke` 的 `catch (std::exception &)` 打包成错误载荷，返回 `()` 的
//!   方法看到 `Ok(())`、返回值的方法看到一条与退出无关的 `Serialization` 错误。

use std::time::Duration;

use rayrust::prelude::*;

/// 与 worker .so 的 `exit_from_task` 同名同签名：集群模式下由工人执行。
#[rayrust::remote]
fn exit_from_task() -> i64 {
    rayrust::exit_actor();
    1
}

/// 与 worker .so 的 `handle_handoff` 同名同签名：收下两段字节、原样交回，并报告
/// 工人进程里复原/调用的结果。
#[rayrust::remote]
fn handle_handoff(serialized: Vec<u8>, raw_id: Vec<u8>) -> (Vec<u8>, Vec<u8>, String, String) {
    let restored = match ActorHandle::from_serialized(&serialized) {
        Ok(handle) => format!("Ok({} 字节 id)", handle.id().len()),
        Err(error) => format!("Err({})", error),
    };
    (
        serialized,
        raw_id,
        restored,
        "本地进程不重复尝试".to_string(),
    )
}

/// `Counter` 的方法名，第 6 项的裸 id 对照要用完整名字提交。
const COUNTER_GET: &str = "__rayrust_actor_factory_counter::get";

/// 与 worker .so 的 `Counter` 同名同实现：集群模式下由工人执行，本地模式下由这个
/// 进程执行。
struct Counter {
    value: i64,
}

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }
}

/// 与 worker .so 的 `SelfExiting` 同名同实现：`exit()` 里调用 `exit_actor()`。
struct SelfExiting {
    value: i64,
}

#[rayrust::actor]
impl SelfExiting {
    fn new(start: i64) -> Self {
        SelfExiting { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn exit(&self) {
        rayrust::exit_actor();
    }

    // 返回值的方法里调用 `exit_actor()`：本地模式用它验证「调用是空操作」这条，
    // 因为方法自己的返回值会被解出来（哨兵路径上那里得到的是 `Serialization` 错误）。
    fn increment_and_exit(&mut self, n: i64) -> i64 {
        self.value += n;
        rayrust::exit_actor();
        self.value
    }
}

/// 与 worker .so 的 `CtorExiting` 同名同实现：`new()` 里调用 `exit_actor()`。
struct CtorExiting {
    value: i64,
}

#[rayrust::actor]
impl CtorExiting {
    fn new(start: i64) -> Self {
        rayrust::exit_actor();
        CtorExiting { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }
}

/// 读一个对象的期限（毫秒）。
const GET_TIMEOUT_MS: i32 = 5000;

/// 等 actor 退出、等任务失败、等交接任务的期限（毫秒）。
const EXIT_TIMEOUT_MS: i32 = 15000;

/// 建组时传的 strategy：2 = STRICT_PACK。取非默认值，`strategy` 断言才有区分力。
const PG_STRATEGY: i32 = 2;

/// `PlacementGroupState::CREATED`。
const PG_STATE_CREATED: i32 = 2;

/// `PlacementGroupState::REMOVED`。
const PG_STATE_REMOVED: i32 = 3;

/// 移除之后巡检的轮数与间隔：GCS 把表项改成 REMOVED 是异步的。
const REMOVE_POLL_TIMES: usize = 50;
const REMOVE_POLL_INTERVAL_MS: u64 = 200;

/// 重启对照的轮数与间隔：`kill_actor(no_restart = false)` 之后等状态回到初值。
const RESTART_POLL_TIMES: usize = 15;
const RESTART_POLL_INTERVAL_MS: u64 = 200;

/// 重启对照里单次读数的期限（毫秒）：重启期间提交的调用可能要多等一会儿。
const RESTART_GET_TIMEOUT_MS: i32 = 3000;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// 本地模式 init 时配的 namespace，第 1 项的断言要拿它对比。
const LOCAL_NAMESPACE: &str = "rayrust_local_ns";

/// 集群模式 init 时配的 namespace。
const CLUSTER_NAMESPACE: &str = "rayrust_cluster_ns";

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
        .namespace(CLUSTER_NAMESPACE)
}

/// 1. is_local_mode：集群 false、本地 true；namespace：两边都与配置的相同。
fn is_local_mode_check(ray: &Ray, cluster: bool) {
    println!("\n--- 1. is_local_mode ---");
    let reported = ray.is_local_mode();
    println!("  is_local_mode() → {}", reported);
    assert_eq!(
        reported, !cluster,
        "is_local_mode() 报 {}，与本次运行的模式（cluster = {}）不符",
        reported, cluster
    );

    let expected = if cluster {
        CLUSTER_NAMESPACE
    } else {
        LOCAL_NAMESPACE
    };
    let namespace = ray.namespace().expect("namespace query");
    println!("  namespace() → {:?}", namespace);
    assert_eq!(
        namespace, expected,
        "namespace() 报 {:?}，配置的是 {:?}（cluster = {}）",
        namespace, expected, cluster
    );
}

/// 2. placement group 全流程。
async fn placement_group_flow(ray: &Ray) {
    println!("\n--- 2. placement group 全流程 ---");
    let name = format!("rayrust_runtime_api_pg_{}", std::process::id());
    let group_id = ray
        .placement_group_create(&name, r#"[{"CPU":0.5}]"#, PG_STRATEGY)
        .expect("placement_group_create");
    assert_eq!(
        group_id.len(),
        18,
        "group id 是 {} 字节，PlacementGroupID 的长度是 18",
        group_id.len()
    );
    println!(
        "  placement_group_create({}, CPU 0.5, STRICT_PACK) → {} 字节 id {}",
        name,
        group_id.len(),
        hex(&group_id)
    );

    let ready = ray
        .placement_group_wait_ready(&group_id, 10)
        .expect("placement_group_wait_ready");
    assert!(ready, "组在 10 秒内没有就绪");
    println!("  placement_group_wait_ready(group, 10) → true");

    let by_id = ray
        .placement_group_by_id(&group_id)
        .expect("placement_group_by_id");
    assert_eq!(by_id.id, group_id, "by_id 拿到的 id 与建组返回的不一样");
    assert_eq!(by_id.name, name, "by_id 拿到的名字不对");
    assert_eq!(
        by_id.state, PG_STATE_CREATED,
        "by_id 的 state 是 {}，CREATED 是 {}",
        by_id.state, PG_STATE_CREATED
    );
    assert_eq!(
        by_id.strategy, PG_STRATEGY,
        "by_id 的 strategy 是 {}，建组时传的是 {}",
        by_id.strategy, PG_STRATEGY
    );
    println!(
        "  placement_group_by_id → id 一致、name {:?}、state {} (CREATED)、strategy {} (STRICT_PACK)",
        by_id.name, by_id.state, by_id.strategy
    );

    let by_name = ray
        .placement_group_by_name(&name)
        .expect("placement_group_by_name");
    assert_eq!(
        (
            by_name.id.clone(),
            by_name.name.clone(),
            by_name.state,
            by_name.strategy
        ),
        (
            group_id.clone(),
            name.clone(),
            PG_STATE_CREATED,
            PG_STRATEGY
        ),
        "by_name 拿到的四个字段与 by_id 不一致"
    );
    println!("  placement_group_by_name → 同样四个字段");

    let all = ray.placement_groups().expect("placement_groups");
    let listed = all
        .iter()
        .find(|group| group.id == group_id)
        .unwrap_or_else(|| {
            panic!(
                "placement_groups() 的 {} 个组里没有本轮的 {}",
                all.len(),
                hex(&group_id)
            )
        });
    assert_eq!(listed.state, PG_STATE_CREATED);
    assert_eq!(listed.strategy, PG_STRATEGY);
    println!(
        "  placement_groups() → {} 个组，其中有它（state {}、strategy {}）",
        all.len(),
        listed.state,
        listed.strategy
    );

    ray.placement_group_remove(&group_id);
    println!("  placement_group_remove → 已提交");

    // 移除是异步的。GCS 把表项改成 REMOVED 之后就留着这一行，名字映射同时消失，
    // 所以这里等的是「state 变成 REMOVED + by_name 报错」，不是等表项消失。
    let mut observed: Option<(i32, i32, String)> = None;
    for attempt in 0..REMOVE_POLL_TIMES {
        let by_id = ray.placement_group_by_id(&group_id);
        let by_name = ray.placement_group_by_name(&name);
        let all = ray.placement_groups().unwrap_or_default();
        let listed_state = all
            .iter()
            .find(|group| group.id == group_id)
            .map(|group| group.state);
        if let (Ok(current), Err(by_name_error), Some(listed_state)) =
            (by_id, by_name, listed_state)
        {
            if current.state == PG_STATE_REMOVED && listed_state == PG_STATE_REMOVED {
                observed = Some((current.state, listed_state, by_name_error.to_string()));
                println!(
                    "  移除后第 {} 次巡检：by_id state {} (REMOVED)、placement_groups() 里 state {}、by_name 报错",
                    attempt, current.state, listed_state
                );
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(REMOVE_POLL_INTERVAL_MS)).await;
    }
    let (by_id_state, listed_state, by_name_error) = observed.unwrap_or_else(|| {
        panic!(
            "移除之后 {} 次巡检（{}ms 一次）没有看到 state 变成 REMOVED 或 by_name 报错",
            REMOVE_POLL_TIMES, REMOVE_POLL_INTERVAL_MS
        )
    });
    assert_eq!(by_id_state, PG_STATE_REMOVED);
    assert_eq!(listed_state, PG_STATE_REMOVED);
    assert!(
        by_name_error.contains(&name),
        "by_name 的错误里没有查询用的名字: {}",
        by_name_error
    );
    println!("  by_name 的错误文本带上名字: {}", by_name_error);
}

/// 3. 非法长度与查不到：全部是提交侧的错误，进程存活。
fn placement_group_rejections(ray: &Ray) {
    println!("\n--- 3. 非法与未知的 placement group ---");
    for (label, id) in [
        ("17 字节", vec![0u8; 17]),
        ("19 字节", vec![0u8; 19]),
        ("空字节", Vec::new()),
    ] {
        match ray.placement_group_by_id(&id) {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("18"),
                    "{} 的 by_id 错误里没有说明长度: {}",
                    label,
                    message
                );
                println!("  by_id({}) → Ffi({})", label, message);
            }
            Err(other) => panic!("by_id({}) 返回了别的错误变体: {}", label, other),
            Ok(group) => panic!("by_id({}) 成功返回了 {:?}", label, group),
        }
        match ray.placement_group_wait_ready(&id, 1) {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("18"),
                    "{} 的 wait_ready 错误里没有说明长度: {}",
                    label,
                    message
                );
                println!("  wait_ready({}) → Ffi({})", label, message);
            }
            Err(other) => panic!("wait_ready({}) 返回了别的错误变体: {}", label, other),
            Ok(ready) => panic!("wait_ready({}) 成功返回了 {}", label, ready),
        }
    }

    // 非法长度之后合法调用照常完成：失败在进 SDK 之前，运行时不处于坏状态。
    let all = ray
        .placement_groups()
        .expect("非法长度之后 placement_groups()");
    println!(
        "  进程存活，随后 placement_groups() 照常返回 {} 个组",
        all.len()
    );

    let unknown = [0xabu8; 18];
    match ray.placement_group_by_id(&unknown) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains(&hex(&unknown)),
                "未知 id 的错误里没有查询用的 id: {}",
                message
            );
            println!("  by_id(未知 18 字节) → Ffi({})", message);
        }
        Err(other) => panic!("未知 18 字节 id 返回了别的错误变体: {}", other),
        Ok(group) => panic!("未知 18 字节 id 成功返回了 {:?}", group),
    }

    let unknown_name = "rayrust_no_such_placement_group";
    match ray.placement_group_by_name(unknown_name) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains(unknown_name),
                "未知名字的错误里没有查询用的名字: {}",
                message
            );
            println!("  by_name(未知名字) → Ffi({})", message);
        }
        Err(other) => panic!("未知名字返回了别的错误变体: {}", other),
        Ok(group) => panic!("未知名字成功返回了 {:?}", group),
    }
}

/// 4. exit_actor：集群模式下 actor 自己结束且不被 max_restarts 重启；本地模式下这个调用
/// 是空操作，方法返回它自己的值、actor 仍然可用。
async fn exit_actor_check(ray: &Ray, cluster: bool) {
    println!("\n--- 4. exit_actor ---");
    let name = format!("rayrust_runtime_api_exiting_{}", std::process::id());
    let opts = ActorOptions::new().name(name.clone()).max_restarts(2);
    let handle = selfexiting_actor_create(ray, &opts, 7).expect("创建 SelfExiting");
    let before = selfexiting_get_sync(ray, &handle)
        .expect("调用 exit 之前的 get")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读 exit 之前的 get");
    assert_eq!(before, 7, "actor 的初值不对");
    println!("  exit 之前 get() → {}", before);

    if !cluster {
        // 本地模式：先给 actor 留下一个可辨认的状态，再证明 exit_actor() 什么都没做。
        // 用带返回值的方法：哨兵路径上它的返回值会被错误载荷顶掉（`Serialization`），
        // 空操作路径上拿到的就是 9。
        let bumped = selfexiting_increment_and_exit_sync(ray, &handle, 2)
            .expect("调用 increment_and_exit")
            .get_timeout(EXIT_TIMEOUT_MS)
            .expect("本地模式里 exit_actor() 是空操作，方法必须返回它自己的值");
        assert_eq!(bumped, 9, "方法自己的返回值不对");
        println!(
            "  increment_and_exit(2) → {}（本地模式 exit_actor() 是空操作，方法返回自己的值）",
            bumped
        );

        let exit_value = selfexiting_exit_sync(ray, &handle)
            .expect("调用 exit()")
            .get_timeout(EXIT_TIMEOUT_MS)
            .expect("本地模式的 () 方法必须正常返回");
        println!(
            "  exit() 的对象 → Ok({:?})（本地模式 () 方法正常返回）",
            exit_value
        );

        let after = selfexiting_get_sync(ray, &handle)
            .expect("调用 exit 之后的 get")
            .get_timeout(GET_TIMEOUT_MS)
            .expect("本地模式的 exit_actor() 之后 actor 必须还能用");
        assert_eq!(after, 9, "本地模式的 exit_actor() 不该改变 actor 的状态");
        println!("  exit 之后 get() → {}（actor 仍然可用，状态没变）", after);
        return;
    }

    let exited = selfexiting_exit_sync(ray, &handle).expect("调用 exit()");
    match exited.get_timeout(EXIT_TIMEOUT_MS) {
        Ok(()) => panic!("exit() 的对象拿到结果了，退出路径不该写结果"),
        Err(error) => println!("  exit() 的对象 → {}（退出路径不写结果）", error),
    }

    let after = match selfexiting_get_sync(ray, &handle) {
        Ok(object) => object.get_timeout(GET_TIMEOUT_MS),
        Err(error) => Err(error),
    };
    match after {
        Err(RayError::Actor(message)) => println!("  exit 之后 get() → Actor({})", message),
        Err(other) => panic!("exit 之后 get() 返回了别的错误变体: {}", other),
        Ok(value) => panic!(
            "actor 退出之后 get() 还返回了 {}——说明它被 max_restarts=2 重启了",
            value
        ),
    }

    match ray.get_actor(&name, "") {
        Ok(Some(found)) => println!(
            "  get_actor → 拿到 {} 字节 id，与原 handle 相同: {}（驱动进程的 core worker 缓存了名字映射，actor 本身已经没了）",
            found.id().len(),
            found.id() == handle.id()
        ),
        Ok(None) => println!("  get_actor → None（名字随 actor 一起消失）"),
        Err(error) => println!("  get_actor → {}（查询本身失败）", error),
    }

    // 正向对照：同一个集群、同样的 `max_restarts=2`，`kill_actor(no_restart = false)`
    // 之后这个 actor 会被重启，状态回到初值。没有这条，「exit 之后调用失败」区分不了
    // 「actor 永远没了」与「这次调用正好在重启窗口里」。
    let control_name = format!("rayrust_runtime_api_restart_{}", std::process::id());
    let control_opts = ActorOptions::new().name(control_name).max_restarts(2);
    let control = selfexiting_actor_create(ray, &control_opts, 7).expect("创建重启对照 actor");
    let before_kill = selfexiting_increment_sync(ray, &control, 2)
        .expect("对照 actor 的 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读对照 actor 的 increment");
    assert_eq!(before_kill, 9, "对照 actor 的计数不对");
    ray.kill_actor(&control, false)
        .expect("kill_actor(no_restart = false)");
    println!(
        "  对照：increment(2) → {}，kill_actor(no_restart = false) 已提交",
        before_kill
    );

    // 重启的判据是「状态回到初值 7」，不是「某次调用成功」：重启窗口里成功的调用
    // 也可能拿到被杀之前的状态，甚至一次既不是之前值也不是初值的过渡读数（实测恒为
    // 97，与 actor 的初值、增量都无关）。
    let mut restarted = None;
    for attempt in 0..RESTART_POLL_TIMES {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(RESTART_POLL_INTERVAL_MS)).await;
        }
        match selfexiting_get_sync(ray, &control)
            .expect("对照 actor 提交 get")
            .get_timeout(RESTART_GET_TIMEOUT_MS)
        {
            Ok(value) if value == 7 => {
                println!("  第 {} 次轮询：get() → {}（回到初值）", attempt, value);
                restarted = Some(value);
                break;
            }
            Ok(value) => println!(
                "  第 {} 次轮询：get() → {}（还没回到初值，重启窗口里的读数不能当判据）",
                attempt, value
            ),
            Err(error) => println!("  第 {} 次轮询：get() → {}（还没就绪）", attempt, error),
        }
    }
    let restarted = restarted.unwrap_or_else(|| {
        panic!(
            "kill_actor(no_restart = false) 之后 {} 次轮询（{}ms 一次）没看到计数回到初值 7",
            RESTART_POLL_TIMES, RESTART_POLL_INTERVAL_MS
        )
    });
    println!(
        "  重启前计数 {} → 重启后计数 {}（这个集群确实会重启这个 actor）",
        before_kill, restarted
    );
}

/// 5. 任务里调 exit_actor()：集群模式下该次调用失败、消息里是我们自己抛的文案；
/// 本地模式下这个调用是空操作，任务照常返回它自己的值。
async fn exit_from_task_check(ray: &Ray, cluster: bool) {
    println!("\n--- 5. 任务里调 exit_actor() ---");
    let object = exit_from_task_remote(ray).expect("提交 exit_from_task");
    if !cluster {
        let value = object
            .get_timeout(EXIT_TIMEOUT_MS)
            .expect("本地模式里 exit_actor() 是空操作，任务必须正常返回");
        assert_eq!(value, 1, "exit_from_task 的返回值不对");
        println!(
            "  exit_from_task → Ok({})（本地模式 exit_actor() 是空操作，任务照常返回）",
            value
        );
        return;
    }
    match object.get_timeout(EXIT_TIMEOUT_MS) {
        Err(RayError::Task(message)) => {
            assert!(
                message.contains("exit_actor"),
                "任务失败的消息里没有我们自己抛的文案: {}",
                message
            );
            println!("  exit_from_task → Task({})", message);
        }
        Err(other) => panic!("任务失败返回了别的错误变体: {}", other),
        Ok(value) => panic!("任务里调用 exit_actor() 还拿到了结果 {}", value),
    }
    println!("  进程存活：这次调用只是失败，没有停在任务里");
}

/// 8. 构造函数里调 exit_actor()：集群模式创建任务失败、第一次调用报 `Actor`；
/// 本地模式那个调用是空操作，actor 正常创建。
async fn ctor_exit_check(ray: &Ray, cluster: bool) {
    println!("\n--- 8. 构造函数里调 exit_actor() ---");
    let name = format!("rayrust_runtime_api_ctor_exiting_{}", std::process::id());
    let opts = ActorOptions::new().name(name).max_restarts(0);
    let handle = ctorexiting_actor_create(ray, &opts, 11).expect("actor_create 照常返回 handle");
    println!("  actor_create → {} 字节 id", handle.id().len());

    if !cluster {
        let value = ctorexiting_get_sync(ray, &handle)
            .expect("本地模式提交 get")
            .get_timeout(GET_TIMEOUT_MS)
            .expect("本地模式构造里的 exit_actor() 是空操作，actor 必须能用");
        assert_eq!(value, 11, "本地模式构造出来的 actor 值不对");
        println!(
            "  第一次 get() → {}（本地模式构造里的 exit_actor() 同样是空操作）",
            value
        );
        return;
    }

    let outcome = match ctorexiting_get_sync(ray, &handle) {
        Ok(object) => object.get_timeout(EXIT_TIMEOUT_MS),
        Err(error) => Err(error),
    };
    match outcome {
        Err(RayError::Actor(message)) => println!(
            "  第一次 get() → Actor({})（构造任务失败，actor 从未启动）",
            message
        ),
        Err(other) => panic!("构造函数退出之后 get() 返回了别的错误变体: {}", other),
        Ok(value) => panic!("构造函数里 exit_actor() 之后 get() 还拿到了 {}", value),
    }
    println!("  进程存活：actor_create 与 get 都没有把进程停住");
}

/// 6. handle 交接：字节穿过工人进程再回来，驱动用它复原 handle 并调用。
async fn handle_handoff_check(ray: &Ray) {
    println!("\n--- 6. actor handle 交接 ---");
    let handle = counter_actor_create(ray, &ActorOptions::new(), 21).expect("创建 Counter");
    let incremented = counter_increment_sync(ray, &handle, 4)
        .expect("调用 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读 increment");
    assert_eq!(incremented, 25, "actor 的计数不对");
    println!("  increment(4) → {}", incremented);

    let serialized = handle.serialize().expect("serialize");
    assert!(!serialized.is_empty(), "serialize 返回了空字节");
    println!(
        "  handle.serialize() → {} 字节（actor id 是 {} 字节）",
        serialized.len(),
        handle.id().len()
    );

    let object = handle_handoff_remote(ray, serialized.clone(), handle.id().to_vec())
        .expect("提交 handle_handoff");
    let (back_serialized, back_id, restored_in_worker, called_in_worker) = object
        .get_timeout(EXIT_TIMEOUT_MS)
        .expect("读 handle_handoff 的结果");
    assert_eq!(
        back_serialized, serialized,
        "序列化字节穿过工人进程之后变了"
    );
    assert_eq!(back_id, handle.id(), "裸 id 穿过工人进程之后变了");
    println!(
        "  工人交回 {} 字节序列化 handle 与 {} 字节裸 id，都与原值相同",
        back_serialized.len(),
        back_id.len()
    );
    println!("  工人侧 from_serialized → {}", restored_in_worker);
    println!("  工人侧裸 id 调用 → {}", called_in_worker);

    let restored = ActorHandle::from_serialized(&back_serialized).expect("驱动侧复原 handle");
    assert_eq!(
        restored.id(),
        handle.id(),
        "复原出来的 actor id 与原 handle 不同"
    );
    println!(
        "  驱动侧 from_serialized → {} 字节 id，与原 handle 相同",
        restored.id().len()
    );
    let value = counter_get_sync(ray, &restored)
        .expect("调用复原的 handle")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读复原 handle 的 get");
    assert_eq!(value, 25, "复原的 handle 拿到的值不对");
    println!("  复原的 handle 调 get() → {}", value);

    // 对照：同一个 actor 的裸 id 在驱动侧也能调用——驱动进程登记过这个 handle。
    // 工人进程里两条路都过不去（上面两行），所以这个对照只在驱动侧有区分力。
    let by_raw_id: i64 = ray
        .actor_call(handle.id(), COUNTER_GET, &[], &[])
        .expect("用裸 id 提交")
        .cast()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读裸 id 的结果");
    assert_eq!(by_raw_id, 25, "裸 id 调用拿到的值不对");
    println!(
        "  对照：驱动侧直接用裸 id 提交同一个方法 → {}（驱动登记过这个 handle）",
        by_raw_id
    );

    // 畸形字节：上游会把它登记成 nil actor，那种 handle 的调用永远不就绪、
    // serialize() 还能给它一个空 handle 继续传播，所以 C 侧看到 nil 就报错。
    match ActorHandle::from_serialized(&[0x5au8; 64]) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("nil actor"),
                "畸形 handle 的错误文本没有说明登记结果是 nil actor: {}",
                message
            );
            println!("  from_serialized(64 字节 0x5a) → Ffi({})", message);
        }
        Err(other) => panic!("畸形 handle 返回了别的错误变体: {}", other),
        Ok(dead) => panic!(
            "畸形 handle 被收下了，得到 {} 字节 id（全 0xff: {}）",
            dead.id().len(),
            dead.id().iter().all(|byte| *byte == 0xff)
        ),
    }
}

/// 7. 本地模式：这四条查询/等待、两个 handle 接口、kill_actor 与 cancel 都必须报错，进程存活。
async fn local_mode_rejections(ray: &Ray) {
    println!("\n--- 7. 本地模式的拒绝 ---");
    let group_id = [0xa1u8; 18];
    match ray.placement_group_wait_ready(&group_id, 1) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "wait_ready 的错误没有说明本地模式: {}",
                message
            );
            println!("  wait_ready → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 wait_ready 返回了别的错误变体: {}", other),
        Ok(ready) => panic!("本地模式 wait_ready 返回了 {}", ready),
    }
    match ray.placement_groups() {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "placement_groups 的错误没有说明本地模式: {}",
                message
            );
            println!("  placement_groups → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 placement_groups 返回了别的错误变体: {}", other),
        Ok(groups) => panic!("本地模式 placement_groups 返回了 {} 个组", groups.len()),
    }
    match ray.placement_group_by_id(&group_id) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "by_id 的错误没有说明本地模式: {}",
                message
            );
            println!("  placement_group_by_id → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 by_id 返回了别的错误变体: {}", other),
        Ok(group) => panic!("本地模式 by_id 返回了 {:?}", group),
    }
    match ray.placement_group_by_name("rayrust_local") {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "by_name 的错误没有说明本地模式: {}",
                message
            );
            println!("  placement_group_by_name → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 by_name 返回了别的错误变体: {}", other),
        Ok(group) => panic!("本地模式 by_name 返回了 {:?}", group),
    }

    let handle = counter_actor_create(ray, &ActorOptions::new(), 1).expect("本地创建 Counter");
    let incremented = counter_increment_sync(ray, &handle, 0)
        .expect("本地 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地读 increment");
    assert_eq!(incremented, 1, "本地的 Counter 计数不对");
    println!(
        "  本地模式的 actor 本身可用：increment(0) → {}",
        incremented
    );
    match ray.kill_actor(&handle, true) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "kill_actor 的错误没有说明本地模式: {}",
                message
            );
            println!("  kill_actor → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 kill_actor 返回了别的错误变体: {}", other),
        Ok(()) => panic!("本地模式 kill_actor 返回了 Ok"),
    }
    let after_kill = counter_increment_sync(ray, &handle, 1)
        .expect("本地 kill 被拒之后 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地 kill 被拒之后读 increment");
    assert_eq!(after_kill, 2, "本地 kill 被拒之后的计数不对");
    println!(
        "  kill 被拒之后 actor 仍可用：increment(1) → {}",
        after_kill
    );
    match handle.serialize() {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "serialize 的错误没有说明本地模式: {}",
                message
            );
            println!("  serialize → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 serialize 返回了别的错误变体: {}", other),
        Ok(bytes) => panic!("本地模式 serialize 返回了 {} 字节", bytes.len()),
    }
    match ActorHandle::from_serialized(&[1u8, 2, 3]) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "from_serialized 的错误没有说明本地模式: {}",
                message
            );
            println!("  ActorHandle::from_serialized → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 from_serialized 返回了别的错误变体: {}", other),
        Ok(handle) => panic!(
            "本地模式 from_serialized 返回了 {} 字节 id",
            handle.id().len()
        ),
    }
    let put_ref = ray.put(&7i64).expect("本地 put");
    match ray.cancel(put_ref.id(), false, false) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "cancel 的错误没有说明本地模式: {}",
                message
            );
            println!("  cancel → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 cancel 返回了别的错误变体: {}", other),
        Ok(()) => panic!("本地模式 cancel 返回了 Ok"),
    }
    let after_cancel = counter_increment_sync(ray, &handle, 2)
        .expect("本地 cancel 被拒之后 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地 cancel 被拒之后读 increment");
    assert_eq!(after_cancel, 4, "本地 cancel 被拒之后的计数不对");
    println!(
        "  cancel 被拒之后进程仍可用：increment(2) → {}",
        after_cancel
    );
    println!("  进程存活：以上被拒的调用都是提交侧的报错，没有 abort");
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== RayRuntime 接口 ({}) ===", mode);
    let cluster = mode == "cluster";
    let config = if cluster {
        cluster_config()
    } else {
        RayConfig::local().namespace(LOCAL_NAMESPACE)
    };
    let ray = Ray::connect(&config).expect("Ray::connect");

    is_local_mode_check(&ray, cluster);
    if cluster {
        placement_group_flow(&ray).await;
        placement_group_rejections(&ray);
    } else {
        println!("\n--- 2、3. placement group 流程与非法输入 ---");
        println!("  本地模式跳过：三个查询在本地模式没有 GCS accessor，C 侧直接报错；");
        println!("  第 7 项会实际跑这四条报错。");
    }
    exit_actor_check(&ray, cluster).await;
    exit_from_task_check(&ray, cluster).await;
    ctor_exit_check(&ray, cluster).await;
    if cluster {
        handle_handoff_check(&ray).await;
    } else {
        println!("\n--- 6. handle 交接的集群部分 ---");
        println!("  本地模式跳过：本地模式不启动 core worker 进程，两个 handle 接口都报错");
        println!("  （第 7 项实际跑这两个报错）；交接到工人进程的流程需要真实工人。");
        local_mode_rejections(&ray).await;
    }

    // 让 blocking pool 里的 FFI 调用收尾，避免退出竞态。
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("\n✓ RayRuntime 接口检查全部通过");
}
