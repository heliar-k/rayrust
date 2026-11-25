# Python baseline benchmark paired with crates/rayrust/examples/raybench.rs.
# Run from the repository root against a running Ray cluster:
#   RAY_ADDRESS=127.0.0.1:6379 python3 tests/python/pybench.py

import os
import sys
import time
import ray

python_dir = os.path.dirname(os.path.abspath(__file__))
if python_dir not in sys.path:
    sys.path.insert(0, python_dir)

address = os.environ.get("RAY_ADDRESS") or "127.0.0.1:6379"
raw_node_ip = os.environ.get("RAY_NODE_IP") or "127.0.0.1"
node_ip = ray._private.services.resolve_ip_for_localhost(raw_node_ip)

print("=== Ray Python Baseline Benchmark ===")
print(f"address: {address}")

ray.init(
    address=address,
    _node_ip_address=node_ip,
    runtime_env={"env_vars": {"PYTHONPATH": python_dir}},
)
print("status: Ray initialized")

import rayrust_test


@ray.remote
def add(a, b):
    return a + b


@ray.remote
def compute(n):
    return sum(range(n))


assert ray.get(add.remote(1, 1)) == 2
assert ray.get(compute.remote(1_000)) == 499_500
assert ray.get(rayrust_test.echo_list.remote([[1]])) == [[1]]
ray.get([rayrust_test.sleep_for.remote(0.30) for _ in range(16)])
ray.get([add.remote(i, 1) for i in range(16)])
ray.get([compute.remote(1_000) for _ in range(16)])
print("status: Warmup completed\n")

# [1] Python -> Python sequential sync (N=500)
print("[1] SDK dispatch: Python -> Python sequential sync (N=500, add(i, 1))")
N = 500
t0 = time.monotonic()
s = 0
for i in range(N):
    s += ray.get(add.remote(i, 1))
sync_elapsed = time.monotonic() - t0
assert s == 125_250
sync_tps = N / sync_elapsed
print(
    f"  {N} tasks in {sync_elapsed * 1000:.2f}ms -> {sync_tps:.0f} tasks/sec, "
    f"{sync_elapsed / N * 1000:.2f}ms/task (checksum={s})\n"
)

# [2] Python -> Python concurrent + batched ray.get (N=500)
print("[2] SDK dispatch: Python -> Python concurrent + batched ray.get (N=500, add(i, 1))")
t0 = time.monotonic()
refs = [add.remote(i, 1) for i in range(N)]
results = ray.get(refs)
s = sum(results)
batched_elapsed = time.monotonic() - t0
assert s == 125_250
batched_tps = N / batched_elapsed
print(
    f"  {N} tasks in {batched_elapsed * 1000:.2f}ms -> {batched_tps:.0f} tasks/sec, "
    f"{batched_elapsed / N * 1000:.2f}ms/task (checksum={s})\n"
)

# [3] Python -> Python single-task round-trip latency (N=100, median & p99)
print("[3] Single-task round-trip latency (N=100, median & p99)")
times = []
for _ in range(100):
    t0 = time.monotonic()
    v = ray.get(add.remote(1, 2))
    assert v == 3
    times.append(time.monotonic() - t0)
times.sort()
median = times[len(times) // 2]
p99 = times[len(times) * 99 // 100]
print(f"  Python -> Python:   median {median * 1000:6.2f}ms  p99 {p99 * 1000:6.2f}ms\n")

# [4] Task-body language baseline: sum(range(1_000_000)) x 10 tasks
print("[4] Task-body language baseline: sum(range(1_000_000)) x 10 concurrent tasks")
print("  Language execution comparison baseline (interpreted CPython, end-to-end for 10 tasks)")
N_TASKS = 10
N_COMPUTE = 1_000_000
t0 = time.monotonic()
refs = [compute.remote(N_COMPUTE) for _ in range(N_TASKS)]
comp_vals = ray.get(refs)
py_compute_elapsed = time.monotonic() - t0
assert all(v == 499_999_500_000 for v in comp_vals)
print(
    f"  Python (CPython):         {py_compute_elapsed * 1000:7.2f}ms "
    f"({N_TASKS / py_compute_elapsed:.0f} tasks/sec)\n"
)

# [5] Complex type round-trip (100x100 nested list, N=50)
print("[5] Complex payload round-trip (100x100 nested list, N=50)")
big_list = [[i * 100 + j for j in range(100)] for i in range(100)]
N_ITER = 50
t0 = time.monotonic()
for _ in range(N_ITER):
    ref = rayrust_test.echo_list.remote(big_list)
    ray.get(ref)
echo_elapsed = time.monotonic() - t0
print(
    f"  100x100 nested list, {N_ITER} iterations: {echo_elapsed * 1000:.2f}ms "
    f"({echo_elapsed / N_ITER * 1000:.2f}ms/iter)\n"
)

print("=== Summary ===")
print(f"py_sync_tps={sync_tps:.1f}")
print(f"py_batched_tps={batched_tps:.1f}")
print(f"py_lat_median_ms={median * 1000:.2f}")
print(f"py_lat_p99_ms={p99 * 1000:.2f}")
print(f"compute_py_10x1m_ms={py_compute_elapsed * 1000:.2f}")
print(f"echo_py_ms={echo_elapsed / N_ITER * 1000:.2f}")

ray.shutdown()
