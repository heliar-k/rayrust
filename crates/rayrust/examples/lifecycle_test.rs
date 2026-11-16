//! Example: `Ray` handles share one runtime; a stale reference fails typed.
//!
//! Run one mode per invocation:
//! ```text
//! ./target/release/examples/lifecycle_test local
//! ./target/release/examples/lifecycle_test cluster
//! ```
//! Environment (cluster mode): RAY_ADDRESS, RAY_NODE_IP.
//!
//! Checks, in order:
//! 1. two `connect` calls with the same config both succeed
//! 2. a value put through the first handle keeps its reference
//! 3. dropping the first handle leaves the runtime initialized and the
//!    reference readable
//! 4. a third `connect` with the same config succeeds and the same reference
//!    is still readable, so the runtime was not re-initialized
//! 5. a `connect` whose address differs is refused with `RayError::Runtime`
//!    naming the field, and the process keeps working
//! 6. after the last handle is dropped the reference fails with
//!    `RayError::Runtime`, a reference rebuilt from the same id bytes fails
//!    the same way, and the process stays alive
//! 7. a `connect` after the last handle was dropped initializes a new runtime
//!    that can put/get; the first runtime's reference is still refused, and a
//!    reference rebuilt after the reconnect reaches the new runtime and fails
//!    there instead of being refused by the epoch guard
//!
//! 检查项顺序不能调整：检查项 7 会改变 epoch，必须放在检查项 6 之后。

use rayrust::prelude::*;

/// The value carried through every check; it must survive the handle drops.
const VALUE: i32 = 42;

/// A second value put after the refused connect, to show the process still works.
const AFTER_VALUE: i32 = 7;

fn cluster_config() -> RayConfig {
    let address =
        std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    RayConfig::new(&address).node_ip(&node_ip)
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    let config = match mode.as_str() {
        "local" => RayConfig::local(),
        "cluster" => cluster_config(),
        other => panic!("unknown mode {:?}, expected local or cluster", other),
    };
    println!("=== Ray handle lifecycle ({}) ===", mode);

    // ── 1. Two connects with the same config ───────────────
    println!("\n--- 1. two connects with the same config ---");
    let first = match Ray::connect(&config) {
        Ok(ray) => {
            println!("  first connect → Ok");
            ray
        }
        Err(e) => panic!("first connect failed: {}", e),
    };
    let second = match Ray::connect(&config) {
        Ok(ray) => {
            println!("  second connect → Ok");
            ray
        }
        Err(e) => panic!("second connect with the same config failed: {}", e),
    };

    // ── 2. A reference put through the first handle ────────
    println!("\n--- 2. put through the first handle ---");
    let value_ref = match first.put(&VALUE) {
        Ok(obj_ref) => {
            println!("  put({}) → id_len={}", VALUE, obj_ref.id().len());
            obj_ref
        }
        Err(e) => panic!("put through the first handle failed: {}", e),
    };

    // ── 3. Drop the first handle ──────────────────────────
    println!("\n--- 3. drop the first handle ---");
    drop(first);
    if !second.is_initialized() {
        panic!("dropping the first of two handles shut the runtime down");
    }
    println!("  runtime still initialized");
    match value_ref.get() {
        Ok(value) => {
            println!("  reference still reads {}", value);
            assert_eq!(
                value, VALUE,
                "the reference returned {} instead of {}",
                value, VALUE
            );
        }
        Err(e) => panic!(
            "the reference failed after one of two handles was dropped: {}",
            e
        ),
    }

    // ── 4. Third connect with the same config ─────────────
    println!("\n--- 4. third connect with the same config ---");
    let third = match Ray::connect(&config) {
        Ok(ray) => {
            println!("  third connect → Ok");
            ray
        }
        Err(e) => panic!("third connect with the same config failed: {}", e),
    };
    match value_ref.get() {
        Ok(value) => {
            println!(
                "  reference still reads {} (the runtime was not re-initialized)",
                value
            );
            assert_eq!(
                value, VALUE,
                "the reference returned {} instead of {}",
                value, VALUE
            );
        }
        Err(e) => panic!("the reference failed after the third connect: {}", e),
    }

    // ── 5. Connect with a different address ───────────────
    println!("\n--- 5. connect with a different address ---");
    let mut other = config.clone();
    other.address = "127.0.0.2:6399".to_string();
    match Ray::connect(&other) {
        Ok(_) => panic!(
            "connect with a different address returned Ok, the live runtime was reused silently"
        ),
        Err(RayError::Runtime(msg)) => {
            println!("  refused: {}", msg);
            assert!(
                msg.contains("address"),
                "the message does not name the field that differs: {}",
                msg
            );
        }
        Err(e) => panic!(
            "connect with a different address returned the wrong error variant: {}",
            e
        ),
    }
    let after = match third.put(&AFTER_VALUE) {
        Ok(obj_ref) => obj_ref,
        Err(e) => panic!("put after the refused connect failed: {}", e),
    };
    match after.get() {
        Ok(value) => {
            println!("  put/get after the refused connect → {}", value);
            assert_eq!(
                value, AFTER_VALUE,
                "put/get after the refused connect returned {} instead of {}",
                value, AFTER_VALUE
            );
        }
        Err(e) => panic!("get after the refused connect failed: {}", e),
    }

    // ── 6. Drop the last handle ───────────────────────────
    println!("\n--- 6. drop the last handle ---");
    drop(second);
    drop(third);
    match value_ref.get() {
        Ok(value) => panic!(
            "the reference read {} from a runtime whose last handle was dropped",
            value
        ),
        Err(RayError::Runtime(msg)) => {
            println!("  stale reference refused: {}", msg);
            assert!(
                msg.contains("epoch"),
                "the message does not describe the runtime generation: {}",
                msg
            );
        }
        Err(e) => panic!(
            "the stale reference returned the wrong error variant: {}",
            e
        ),
    }
    // 用 id 字节重建的引用打的是“活着的运行时”的代次，此刻没有活着的运行时，
    // 所以它同样被拒。
    let rebuilt_after_close: ObjectRef<i32> = ObjectRef::from(value_ref.id().to_vec());
    match rebuilt_after_close.get() {
        Ok(value) => panic!(
            "a reference rebuilt from the id bytes read {} after the last handle was dropped",
            value
        ),
        Err(RayError::Runtime(msg)) => println!("  rebuilt reference refused: {}", msg),
        Err(e) => panic!(
            "the rebuilt reference returned the wrong error variant: {}",
            e
        ),
    }
    println!("  process still alive after the refusal");

    // ── 7. Connect after the last handle was dropped ───────
    // 探针在两种模式下都确认过：全部句柄 drop 之后再次 connect（再一次
    // ray_init）能成功，新运行时可以正常 put/get。
    println!("\n--- 7. connect after the last handle was dropped ---");
    let again = match Ray::connect(&config) {
        Ok(ray) => {
            println!("  reconnect → Ok");
            ray
        }
        Err(e) => panic!("reconnect after every handle was dropped failed: {}", e),
    };
    let fresh = match again.put(&AFTER_VALUE) {
        Ok(obj_ref) => obj_ref,
        Err(e) => panic!("put on the new runtime failed: {}", e),
    };
    match fresh.get() {
        Ok(value) => {
            println!("  put/get on the new runtime → {}", value);
            assert_eq!(
                value, AFTER_VALUE,
                "the new runtime returned {} instead of {}",
                value, AFTER_VALUE
            );
        }
        Err(e) => panic!("get on the new runtime failed: {}", e),
    }
    match value_ref.get() {
        Ok(value) => panic!(
            "the reference from the first runtime read {} after a second initialization",
            value
        ),
        Err(RayError::Runtime(msg)) => {
            println!(
                "  the reference from the first runtime is still refused: {}",
                msg
            )
        }
        Err(e) => panic!(
            "the reference from the first runtime returned the wrong error variant: {}",
            e
        ),
    }

    // 重建的引用打的是新代次，守卫放行，所以这次调用真的进了 FFI：新运行时里
    // 没有这个对象，拿到的既不是 Ok 也不是 Runtime。
    let rebuilt_after_reconnect: ObjectRef<i32> = ObjectRef::from(value_ref.id().to_vec());
    match rebuilt_after_reconnect.get_timeout(2000) {
        Ok(value) => panic!(
            "a reference rebuilt from the id bytes read {} from the new runtime",
            value
        ),
        Err(RayError::Runtime(msg)) => panic!(
            "the rebuilt reference was refused by the epoch guard ({}), it never reached the new runtime",
            msg
        ),
        Err(e) => println!(
            "  rebuilt reference reached the new runtime and failed there: {}",
            e
        ),
    }

    println!("\n✓ lifecycle checks passed");
}
