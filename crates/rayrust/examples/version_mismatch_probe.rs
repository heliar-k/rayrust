//! Probe binary for verifying runtime SDK shared-library and cluster version
//! mismatch rejection.
//!
//! Usage:
//!   version_mismatch_probe local
//!   version_mismatch_probe connect <host:port> [node_ip]
//!
//! Exit codes:
//!   0   — initialization and basic put/get succeeded
//!   2   — initialization failed cleanly with `ray_is_initialized() == false`
//!   101 — unexpected state (for example `ray_is_initialized() == true` after a
//!         refused connect) or invalid arguments

use rayrust::prelude::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("");

    let build_ver = Ray::sdk_version();
    let loaded_ver = Ray::loaded_sdk_version();
    println!("BUILD_RAY_VERSION={build_ver}");
    match &loaded_ver {
        Ok(v) => println!("LOADED_RAY_VERSION={v}"),
        Err(e) => println!("LOADED_RAY_VERSION_ERR={e}"),
    }

    match mode {
        "local" => match Ray::local() {
            Ok(ray) => {
                let obj = ray.put(&42i64).expect("local put failed");
                let val = obj.get().expect("local get failed");
                assert_eq!(val, 42);
                println!("LOCAL_CONNECT_OK put_get={val}");
                drop(ray);
            }
            Err(err) => {
                let is_init = unsafe { rayrust::sys::ray_is_initialized() };
                println!("LOCAL_CONNECT_ERR={err}");
                println!("RAY_IS_INITIALIZED={is_init}");
                assert!(
                    !is_init,
                    "ray_is_initialized() became true after refused local init"
                );
                std::process::exit(2);
            }
        },
        "connect" => {
            let address = args.get(2).unwrap_or_else(|| {
                panic!("usage: version_mismatch_probe connect <host:port> [node_ip]")
            });
            let mut config = RayConfig::new(address);
            if let Some(ip) = args
                .get(3)
                .cloned()
                .or_else(|| std::env::var("RAY_NODE_IP").ok().filter(|s| !s.is_empty()))
            {
                config = config.node_ip(ip);
            }
            match Ray::connect(&config) {
                Ok(ray) => {
                    let obj = ray.put(&99i64).expect("cluster put failed");
                    let val = obj.get().expect("cluster get failed");
                    assert_eq!(val, 99);
                    println!("CLUSTER_CONNECT_OK put_get={val}");
                    drop(ray);
                }
                Err(err) => {
                    let is_init = unsafe { rayrust::sys::ray_is_initialized() };
                    println!("CLUSTER_CONNECT_ERR={err}");
                    println!("RAY_IS_INITIALIZED={is_init}");
                    assert!(
                        !is_init,
                        "ray_is_initialized() became true after refused cluster connect"
                    );
                    std::process::exit(2);
                }
            }
        }
        other => {
            panic!("unknown mode {other:?}; expected 'local' or 'connect <host:port> [node_ip]'");
        }
    }
}
