#!/usr/bin/env python3
"""
A/B 对比测试 - 优化前 vs 优化后 (release 构建)
"""
import subprocess
import time
import requests
import os
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

BASE_URL = "http://localhost:8090/openai/v1"
MOCK_PORT = 8765

def start_mock():
    env = os.environ.copy()
    env["MOCK_DELAY_MS"] = "50"
    env["MOCK_ERROR_RATE"] = "0.01"
    env["MOCK_RATE_LIMIT_RATE"] = "0.02"
    proc = subprocess.Popen(
        ["python", "-u", "tests/mock_server.py"],
        cwd="D:/rustllmproxy", env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    return proc

def start_proxy(binary_path):
    if not os.path.isabs(binary_path):
        binary_path = os.path.join("D:/rustllmproxy", binary_path)
    proc = subprocess.Popen(
        [binary_path, "configs/test_perf.toml"],
        cwd="D:/rustllmproxy",
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(5)
    return proc

def kill(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except:
        proc.kill()

def make_request(i):
    start = time.time()
    try:
        r = requests.post(
            f"{BASE_URL}/chat/completions",
            json={"model": "glm-4.7", "messages": [{"role": "user", "content": "x" * 100}]},
            headers={"Authorization": "Bearer sk-perf-test"},
            timeout=30
        )
        ms = (time.time() - start) * 1000
        return (i, r.status_code, ms)
    except:
        ms = (time.time() - start) * 1000
        return (i, None, ms)

def run_test(name, num_requests, concurrency):
    print(f"\n  [{name}] {num_requests} requests, {concurrency} concurrency")
    lats = []
    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(make_request, i) for i in range(num_requests)]
        for f in as_completed(futs):
            _, sc, ms = f.result()
            if sc == 200:
                ok += 1
                lats.append(ms)
            else:
                fail += 1

    if not lats:
        print(f"    FAIL: 0 success, {fail} failed")
        return None

    lats.sort()
    n = len(lats)
    qps = num_requests / (sum(lats) / 1000) if lats else 0
    return {
        "ok": ok, "fail": fail,
        "rate": ok / (ok + fail) * 100,
        "p50": lats[int(n * 0.50)],
        "p75": lats[int(n * 0.75)],
        "p90": lats[int(n * 0.90)],
        "p95": lats[int(n * 0.95)],
        "p99": lats[min(int(n * 0.99), n - 1)],
        "avg": sum(lats) / n,
        "min": min(lats),
        "max": max(lats),
    }

def fmt(ms):
    return f"{ms:.1f}ms" if ms >= 1 else f"{ms:.3f}ms"

def main():
    old_bin = "target/release/unified-proxy-old.exe"
    new_bin = "target/release/unified-proxy-new.exe"

    scenarios = [
        ("Low concurrency (10)",  300, 10),
        ("Medium concurrency (50)", 300, 50),
        ("High concurrency (200)", 500, 200),
    ]

    results = {}

    for label, binary in [("BEFORE (old)", old_bin), ("AFTER (new)", new_bin)]:
        print(f"\n{'='*70}")
        print(f"  Testing: {label}")
        print(f"  Binary: {binary}")
        print(f"{'='*70}")

        mock = start_mock()
        proxy = start_proxy(binary)

        # warmup
        run_test("warmup", 20, 5)

        results[label] = []
        for name, n, c in scenarios:
            r = run_test(name, n, c)
            results[label].append((name, r))
            if r:
                print(f"    OK={r['ok']} FAIL={r['fail']} Rate={r['rate']:.1f}%")
                print(f"    P50={fmt(r['p50'])} P95={fmt(r['p95'])} P99={fmt(r['p99'])} Avg={fmt(r['avg'])}")

        kill(proxy)
        kill(mock)
        time.sleep(2)

    # Summary
    print(f"\n{'='*70}")
    print("  A/B COMPARISON (release build)")
    print(f"{'='*70}")

    old_results = results["BEFORE (old)"]
    new_results = results["AFTER (new)"]

    for i, (name, n, c) in enumerate(scenarios):
        old_r = old_results[i][1]
        new_r = new_results[i][1]
        if not old_r or not new_r:
            print(f"\n  [{name}] MISSING DATA")
            continue

        print(f"\n  [{name}]")
        print(f"  {'Metric':<12} {'BEFORE':>12} {'AFTER':>12} {'Delta':>12} {'%':>8}")
        print(f"  {'-'*56}")
        for metric in ["p50", "p75", "p90", "p95", "p99", "avg"]:
            o = old_r[metric]
            n = new_r[metric]
            delta = n - o
            pct = (delta / o) * 100 if o > 0 else 0
            sign = "+" if delta > 0 else ""
            tag = "faster" if delta < 0 else "slower"
            print(f"  {metric.upper():<12} {fmt(o):>12} {fmt(n):>12} {sign}{delta:.1f}ms {sign}{pct:.1f}% ({tag})")

        print(f"  {'Success':<12} {old_r['rate']:>11.1f}% {new_r['rate']:>11.1f}%")

if __name__ == "__main__":
    main()
