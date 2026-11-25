//! Example: `Ray::connect` 的地址校验，以及 `RayConfig::address` 的实测行为。
//!
//! 运行模式只看 `RayConfig::local_mode`。本地模式完全不看地址：地址不交给 SDK，也不校验。
//! 集群模式里地址非空时必须是 `host:port`：IPv6 主机带方括号、host 非空、十进制端口
//! 0..=65535，端口按整串读。上游会静默丢掉的尾缀（`"host:6379\n"`）在这里被拒绝，
//! 这是比上游严的三处之一（另两处是空 host 与端口超出 0..=65535，见 `Ray::connect`）。
//! 地址为空时 SDK 先读 `RAY_ADDRESS`（空串也算被设置），那里也没有时自己起一个 head 节点。
//! 不合规的地址与不合规的 `RAY_ADDRESS` 都在 C 入口 `ray_init` 里、进 SDK 之前返回
//! `RayError::Ffi`，错误消息点名 `address` 或 `RAY_ADDRESS`；上游对同样输入的结局是
//! `ConfigInternal::SetBootstrapAddress` 的 RAY_CHECK（cpp/src/ray/config_internal.cc:255、
//! :261）、约 35 秒后的 cpp/src/ray/util/process_helper.cc:110、
//! cpp/src/ray/util/util.cc:42，或者把空 host 当成没给地址去起 head 节点。
//!
//! 一次运行跑完全部形态，不需要集群：
//! ```text
//! cargo run --release --example connect_address_test
//! ```
//!
//! 检查项，每项都打印实际结果：
//! 1. 集群 + `"local"`：没有 `:` 分隔符
//! 2. 集群 + `"127.0.0.1:abc"`：端口不是十进制数字
//! 3. 集群 + `"127.0.0.1"`：没有 `:` 分隔符
//! 4. 集群 + `"::1"`：IPv6 主机没带方括号
//! 5. 集群 + `":6399"`：host 为空
//! 6. 集群 + `"127.0.0.1:"`：端口为空
//! 7. 集群 + `"127.0.0.1:65536"`：端口超出 0..=65535
//! 8. 集群 + `"127.0.0.1:-1"`：端口不是十进制数字
//! 9. 集群 + 空地址 + `RAY_ADDRESS="local"`：环境变量那一路同样被拒
//! 10. 本地 + `"local"`：接受，并且 `put`/`get` 跑通
//! 11. 本地 + `"127.0.0.1:abc"`：接受
//! 12. 本地 + `"127.0.0.1:6399"` + `RAY_ADDRESS="local"`：接受（`RAY_ADDRESS` 只在集群模式
//!     看），并且 `put`/`get` 跑通
//!
//! 第 1-9 项不建运行时，可以连续跑；第 10-12 项各建一个本地模式运行时，条目之间 drop
//! 掉句柄，因为 `address` 参与 `Ray::connect` 的配置比较，不 drop 会拿到
//! `RayError::Runtime` 的配置不一致。第 12 项临时设置 `RAY_ADDRESS`，用完删掉。

use rayrust::prelude::*;

/// 一次应当被拒绝的 `connect`：必须是 `RayError::Ffi`，消息点名 `field`，并带上该形态的
/// 判别子串。
fn expect_rejected(label: &str, config: &RayConfig, field: &str, sentence: &str) {
    match Ray::connect(config) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains(&format!("{} \"", field)),
                "{}: error message does not name {}: {}",
                label,
                field,
                message
            );
            assert!(
                message.contains(sentence),
                "{}: error message does not contain {:?}: {}",
                label,
                sentence,
                message
            );
            println!("  {} -> Ffi({})", label, message);
        }
        Err(other) => panic!("{}: returned unexpected error variant: {}", label, other),
        Ok(_) => panic!("{}: unexpectedly succeeded", label),
    }
}

/// 本地模式接受这个地址：句柄建得出来。
fn connect_local(label: &str, address: &str) -> Ray {
    let mut config = RayConfig::local();
    config.address = address.to_string();
    match Ray::connect(&config) {
        Ok(ray) => ray,
        Err(e) => panic!("{}: local mode rejected address {:?}: {}", label, address, e),
    }
}

/// 运行时可用：`put`/`get` 跑得通。
fn expect_local_works(label: &str, ray: &Ray) {
    let object = match ray.put(&42i32) {
        Ok(object) => object,
        Err(e) => panic!("{}: local mode put failed: {}", label, e),
    };
    match object.get() {
        Ok(value) => assert_eq!(value, 42, "{}: get returned {} instead of 42", label, value),
        Err(e) => panic!("{}: local mode get failed: {}", label, e),
    }
    println!("  {} -> Ok, put/get returned 42", label);
}

fn main() {
    // 拒绝项全部用集群模式，地址非空时不会走到 `RAY_ADDRESS` 那一路。
    println!("--- 1. cluster + \"local\" ---");
    expect_rejected(
        "cluster \"local\"",
        &RayConfig::new("local"),
        "address",
        "no ':' separator",
    );

    println!("\n--- 2. cluster + \"127.0.0.1:abc\" ---");
    expect_rejected(
        "cluster \"127.0.0.1:abc\"",
        &RayConfig::new("127.0.0.1:abc"),
        "address",
        "the port must be decimal digits",
    );

    println!("\n--- 3. cluster + \"127.0.0.1\" ---");
    expect_rejected(
        "cluster \"127.0.0.1\"",
        &RayConfig::new("127.0.0.1"),
        "address",
        "no ':' separator",
    );

    println!("\n--- 4. cluster + \"::1\" ---");
    expect_rejected(
        "cluster \"::1\"",
        &RayConfig::new("::1"),
        "address",
        "an IPv6 host must be bracketed",
    );

    println!("\n--- 5. cluster + \":6399\" ---");
    expect_rejected(
        "cluster \":6399\"",
        &RayConfig::new(":6399"),
        "address",
        "the host is empty",
    );

    println!("\n--- 6. cluster + \"127.0.0.1:\" ---");
    expect_rejected(
        "cluster \"127.0.0.1:\"",
        &RayConfig::new("127.0.0.1:"),
        "address",
        "the port is empty",
    );

    println!("\n--- 7. cluster + \"127.0.0.1:65536\" ---");
    expect_rejected(
        "cluster \"127.0.0.1:65536\"",
        &RayConfig::new("127.0.0.1:65536"),
        "address",
        "the port must be 0..=65535",
    );

    println!("\n--- 8. cluster + \"127.0.0.1:-1\" ---");
    expect_rejected(
        "cluster \"127.0.0.1:-1\"",
        &RayConfig::new("127.0.0.1:-1"),
        "address",
        "the port must be decimal digits",
    );

    println!("\n--- 9. cluster + empty address + RAY_ADDRESS=\"local\" ---");
    std::env::set_var("RAY_ADDRESS", "local");
    let result = Ray::connect(&RayConfig::new(""));
    std::env::remove_var("RAY_ADDRESS");
    match result {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("RAY_ADDRESS \""),
                "case 9: error message does not name RAY_ADDRESS: {}",
                message
            );
            assert!(
                message.contains("no ':' separator"),
                "case 9: error message missing separator detail: {}",
                message
            );
            println!("  cluster \"\" + RAY_ADDRESS=\"local\" -> Ffi({})", message);
        }
        Err(other) => panic!("case 9: returned unexpected error variant: {}", other),
        Ok(_) => panic!("case 9: unexpectedly succeeded"),
    }

    println!("\n--- 10. local + \"local\" ---");
    expect_local_works(
        "local \"local\"",
        &connect_local("local \"local\"", "local"),
    );

    println!("\n--- 11. local + \"127.0.0.1:abc\" ---");
    expect_local_works(
        "local \"127.0.0.1:abc\"",
        &connect_local("local \"127.0.0.1:abc\"", "127.0.0.1:abc"),
    );

    println!("\n--- 12. local + \"127.0.0.1:6399\" + RAY_ADDRESS=\"local\" ---");
    // `RAY_ADDRESS` 那一路只在集群模式跑：本地模式必须忽略它。去掉 `ray_init` 里的
    // `!local &&`，这里的 connect 会拿到 `Ffi`。
    std::env::set_var("RAY_ADDRESS", "local");
    let mut config = RayConfig::local();
    config.address = "127.0.0.1:6399".to_string();
    let connected = Ray::connect(&config);
    std::env::remove_var("RAY_ADDRESS");
    let ray = match connected {
        Ok(ray) => ray,
        Err(e) => panic!("case 12: local mode was affected by RAY_ADDRESS: {}", e),
    };
    assert!(
        std::env::var_os("RAY_ADDRESS").is_none(),
        "case 12: RAY_ADDRESS was not removed from env"
    );
    expect_local_works("local \"127.0.0.1:6399\" + RAY_ADDRESS=\"local\"", &ray);
    drop(ray);

    println!("\nPASS: address validation passed on all 12 cases");
}
