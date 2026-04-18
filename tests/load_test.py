#!/usr/bin/env python3
"""
性能测试脚本 - 对 Rust LLM Proxy 进行负载测试
测量优化前后的 P50/P95/P99 延迟
"""
import subprocess
import time
import os
import requests
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict

# 测试配置
BASE_URL = "http://localhost:8090/openai/v1"
TEST_REQUEST = {
    "model": "glm-5.1",
    "messages": [{"role": "user", "content": "test performance"}],
    "stream": False
}

def start_mock_server(delay_ms: int = 50, error_rate: float = 0.05, rate_limit_rate: float = 0.1):
    """启动 mock 后端服务器"""
    env = os.environ.copy()
    env.update({
        "MOCK_DELAY_MS": str(delay_ms),
        "MOCK_ERROR_RATE": str(error_rate),
        "MOCK_RATE_LIMIT_RATE": str(rate_limit_rate),
    })
    proc = subprocess.Popen(
        ["python", "tests/mock_server.py"],
        cwd="D:/rustllmproxy",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)  # 等待服务器启动
    return proc

def start_proxy(config_path: str = "configs/test_perf.toml"):
    """启动 proxy 服务器"""
    proc = subprocess.Popen(
        ["cargo", "run", "--bin", "unified-proxy", config_path],
        cwd="D:/rustllmproxy",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(5)  # 等待服务器启动
    return proc

def make_request(thread_id: int, seq: int) -> Tuple[int, int, float, bool]:
    """
    发送请求并返回 (thread_id, seq, latency_ms, success)
    """
    start = time.time()
    try:
        resp = requests.post(
            f"{BASE_URL}/chat/completions",
            json=TEST_REQUEST,
            headers={"Authorization": "Bearer sk-perf-test"},
            timeout=10
        )
        latency = (time.time() - start) * 1000
        success = resp.status_code == 200
        return (thread_id, seq, latency, success)
    except Exception as e:
        latency = (time.time() - start) * 1000
        return (thread_id, seq, latency, False)

def run_load_test(num_requests: int, concurrency: int) -> List[Tuple[int, int, float, bool]]:
    """
    运行负载测试
    返回 [(thread_id, seq, latency_ms, success), ...]
    """
    results = []
    completed = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(make_request, i % concurrency, i): i
            for i in range(num_requests)
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                completed += 1
                if result[3]:  # success
                    print(f"[{completed}/{num_requests}] [OK] Req #{result[2]}: {result[2]:.1f}ms")
                else:
                    print(f"[{completed}/{num_requests}] [FAIL] Req #{result[2]}: {result[2]:.1f}ms")
            except Exception as e:
                print(f"[{completed}/{num_requests}] ✗ Request failed: {e}")
    return results

def analyze_results(results: List[Tuple[int, int, float, bool]]) -> Dict:
    """分析测试结果"""
    latencies = [r[2] for r in results if r[3]]
    if not latencies:
        print("[ERROR] No successful requests!")
        return {}

    latencies.sort()
    n = len(latencies)
    p50 = latencies[int(n * 0.50)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]
    avg = sum(latencies) / n

    success_rate = len(latencies) / len(results) * 100

    print(f"\n{'='*60}")
    print(f"Load Test Results ({len(results)} total requests)")
    print(f"{'='*60}")
    print(f"Success Rate: {success_rate:.1f}%")
    print(f"Avg Latency:  {avg:.1f} ms")
    print(f"P50:          {p50:.1f} ms")
    print(f"P95:          {p95:.1f} ms")
    print(f"P99:          {p99:.1f} ms")
    print(f"Min:          {min(latencies):.1f} ms")
    print(f"Max:          {max(latencies):.1f} ms")
    print(f"{'='*60}\n")

    return {
        "total": len(results),
        "success": len(latencies),
        "success_rate": success_rate,
        "avg": avg,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "min": min(latencies),
        "max": max(latencies),
    }

def main():
    parser = argparse.ArgumentParser(description="Rust LLM Proxy 性能测试")
    parser.add_argument("--requests", type=int, default=1000, help="总请求数")
    parser.add_argument("--concurrency", type=int, default=20, help="并发数")
    parser.add_argument("--warmup", type=int, default=100, help="预热请求数")
    parser.add_argument("--mock-delay", type=int, default=50, help="mock 后端延迟 (ms)")
    parser.add_argument("--mock-error-rate", type=float, default=0.05, help="mock 后端错误率")
    parser.add_argument("--mock-rate-limit", type=float, default=0.1, help="mock 后端 429 率")
    args = parser.parse_args()

    print("[*] Starting performance test environment...")
    print(f"Config: {args.requests} requests, {args.concurrency} concurrency")
    print(f"Mock: delay={args.mock_delay}ms, error_rate={args.mock_error_rate}, rate_limit={args.mock_rate_limit}")

    # 启动 mock 服务器
    print("\n[*] Starting mock LLM server...")
    mock_proc = start_mock_server(args.mock_delay, args.mock_error_rate, args.mock_rate_limit)
    print(f"   [OK] Mock server started (http://127.0.0.1:8765)")

    # 启动 proxy
    print("\n[*] Starting proxy server...")
    proxy_proc = start_proxy()
    print(f"   [OK] Proxy server started (http://localhost:8090)")

    # 预热
    print(f"\n[*] Warming up with {args.warmup} requests...")
    warmup_results = run_load_test(args.warmup, min(args.concurrency, 5))
    warmup_success = len([r for r in warmup_results if r[3]])
    print(f"   Warmup complete: {warmup_success}/{args.warmup} success")

    # 正式测试
    print(f"\n[*] Starting load test...")
    start_time = time.time()
    results = run_load_test(args.requests, args.concurrency)
    total_time = time.time() - start_time

    stats = analyze_results(results)
    if stats:
        qps = len(results) / total_time
        print(f"Throughput: {qps:.1f} requests/sec")

    # 清理
    print("\n[*] Cleaning up...")
    mock_proc.terminate()
    proxy_proc.terminate()
    try:
        mock_proc.wait(timeout=5)
        proxy_proc.wait(timeout=5)
    except:
        mock_proc.kill()
        proxy_proc.kill()
    print("[OK] Test complete")

if __name__ == "__main__":
    main()
