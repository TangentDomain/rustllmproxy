"""性能负载测试脚本 - 无外部依赖，仅用 stdlib"""
import json
import time
import sys
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

PROXY_URL = "http://127.0.0.1:8090"
AUTH_KEY = "sk-perf-test"
MOCK_PORT = 8765
CONCURRENCY = 50
TOTAL_REQUESTS = 500

BODY = json.dumps({
    "model": "glm-4.7",
    "messages": [{"role": "user", "content": "perf test"}]
}).encode()


def check(url, expect=200):
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status == expect
    except Exception:
        return False


def send_request(i):
    """发送单个请求，返回 (success, latency_ms)"""
    req = urllib.request.Request(
        f"{PROXY_URL}/v1/chat/completions",
        data=BODY,
        headers={
            "Authorization": f"Bearer {AUTH_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            latency = (time.perf_counter() - start) * 1000
            return r.status == 200, latency
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return False, latency


def main():
    print("=== LLM Proxy 性能测试 ===\n")

    # 检查 proxy 是否运行
    print("[1/3] 检查 Proxy 可用性...")
    if not check(f"{PROXY_URL}/health"):
        print(f"  ERROR: Proxy 未运行于 {PROXY_URL}")
        print(f"  请先启动: ./target/release/unified-proxy.exe configs/test_perf.toml")
        sys.exit(1)
    print("  Proxy OK\n")

    # 单请求验证
    print("[2/3] 功能验证...")
    ok, lat = send_request(0)
    if not ok:
        print("  ERROR: 单请求失败")
        sys.exit(1)
    print(f"  单请求 OK ({lat:.0f}ms)\n")

    # 并发测试
    print(f"[3/3] 并发负载测试 ({CONCURRENCY} 并发, {TOTAL_REQUESTS} 请求)...\n")
    latencies = []
    success = 0
    fail = 0
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(send_request, i) for i in range(TOTAL_REQUESTS)]
        for f in as_completed(futures):
            ok, lat = f.result()
            latencies.append(lat)
            if ok:
                success += 1
            else:
                fail += 1

    elapsed = (time.perf_counter() - start) * 1000
    latencies.sort()

    # 统计
    rps = TOTAL_REQUESTS / (elapsed / 1000)
    p50 = latencies[int(len(latencies) * 0.50)]
    p90 = latencies[int(len(latencies) * 0.90)]
    p99 = latencies[int(len(latencies) * 0.99)]
    avg = sum(latencies) / len(latencies)

    print("=== 性能测试结果 ===")
    print(f"  总请求数: {TOTAL_REQUESTS}")
    print(f"  成功/失败: {success}/{fail}")
    print(f"  总耗时: {elapsed:.0f}ms")
    print(f"  吞吐量: {rps:.1f} req/s")
    print(f"  平均延迟: {avg:.1f}ms")
    print(f"  P50: {p50:.1f}ms")
    print(f"  P90: {p90:.1f}ms")
    print(f"  P99: {p99:.1f}ms")
    print(f"  并发数: {CONCURRENCY}")


if __name__ == "__main__":
    main()
