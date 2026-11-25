//! Example: Ray remote task in local mode.
//!
//! Tests:
//! - #[remote] macro: callback generation + registration + caller
//! - put/get (i32, String)
//! - remote task: add(1, 2) → 3
//! - namespace query: local mode reports the namespace given to init, which is
//!   the empty string here (local mode starts no core worker, so there is no job
//!   config to ask)

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[rayrust::remote]
fn greet(name: String) -> String {
    format!("Hello, {}!", name)
}

fn main() {
    println!("Registering remote functions...");

    println!("\n--- Init (local mode) ---");
    let ray = Ray::local().expect("local init failed");
    println!("✓ Ray initialized (local mode)");

    // ── Put / Get ──────────────────────────────────────────
    println!("\n--- Put / Get ---");
    let obj = ray.put(&42i32).unwrap();
    println!("Put 42i32 → ObjectRef(id_len={})", obj.id().len());
    match obj.get() {
        Ok(val) => println!("Get → {} ✓", val),
        Err(e) => println!("Get failed: {}", e),
    }

    let obj2 = ray.put(&"hello ray from rust!".to_string()).unwrap();
    match obj2.get() {
        Ok(val) => println!("Get String → {} ✓", val),
        Err(e) => println!("Get String failed: {}", e),
    }

    // ── Remote Task (local mode) ───────────────────────────
    println!("\n--- Remote Task (local mode) ---");

    // Register the function with FunctionManager
    add_register();
    greet_register();
    println!("✓ Functions registered");

    // Call add(1, 2)
    let obj_ref = add_remote(&ray, 1, 2).expect("submit add(1, 2)");
    println!("Task 'add(1, 2)' submitted → ObjectRef(id_len={})", obj_ref.id().len());
    match obj_ref.get() {
        Ok(val) => println!("Task result: {} ✓", val),
        Err(e) => println!("Task result get failed: {}", e),
    }

    // Call greet("Ray")
    let obj_ref2 = greet_remote(&ray, "Ray".to_string()).expect("submit greet(\"Ray\")");
    println!("Task 'greet(\"Ray\")' submitted");
    match obj_ref2.get() {
        Ok(val) => println!("Greet result: {} ✓", val),
        Err(e) => println!("Greet result get failed: {}", e),
    }

    // ── Namespace ─────────────────────────────────────────
    println!("\n--- Namespace ---");
    let namespace = ray.namespace().expect("namespace query");
    println!("Namespace: {:?}", namespace);
    assert_eq!(
        namespace, "",
        "local mode without namespace config returned {:?}",
        namespace
    );

    // ── Shutdown (automatic on drop) ──────────────────────
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ Ray shutdown");
}
