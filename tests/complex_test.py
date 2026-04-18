#!/usr/bin/env python3
"""
复杂场景性能测试 - 模拟真实生产环境
"""
import subprocess
import time
import requests
import argparse
import threading
import os
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
import json

@dataclass
class TestScenario:
    name: str
    num_requests: int
    concurrency: int
    delay_ms: int
    error_rate: float
    rate_limit_rate: float
    request_sizes: List[int]  # 不同请求体大小（字节）

class PerformanceMetrics:
    def __init__(self):
        self.latencies: List[float] = []
        self.success_count = 0
        self.fail_count = 0
        self.rate_limit_count = 0
        self.server_error_count = 0
        self.client_error_count = 0
        self.timeout_count = 0

    def add_result(self, latency: float, status_code: Optional[int]):
        if status_code == 200:
            self.latencies.append(latency)
            self.success_count += 1
        else:
            self.fail_count += 1
            if status_code == 429:
                self.rate_limit_count += 1
            elif status_code is not None and 500 <= status_code < 600:
                self.server_error_count += 1
            elif status_code is not None and 400 <= status_code < 500:
                self.client_error_count += 1
            elif latency > 10000:
                self.timeout_count += 1

    def report(self) -> Dict:
        if not self.latencies:
            return {"error": "No successful requests"}

        latencies = sorted(self.latencies)
        n = len(latencies)
        total = len(self.latencies) + self.fail_count

        return {
            "total_requests": total,
            "success": self.success_count,
            "failed": self.fail_count,
            "success_rate": self.success_count / total * 100 if total > 0 else 0,
            "rate_limit": self.rate_limit_count,
            "server_error": self.server_error_count,
            "client_error": self.client_error_count,
            "timeout": self.timeout_count,
            "p50": latencies[int(n * 0.50)],
            "p75": latencies[int(n * 0.75)],
            "p90": latencies[int(n * 0.90)],
            "p95": latencies[int(n * 0.95)],
            "p99": latencies[int(n * 0.99)] if n >= 100 else latencies[-1],
            "avg": sum(latencies) / n,
            "min": min(latencies),
            "max": max(latencies),
        }

def make_request(
    base_url: str,
    thread_id: int,
    seq: int,
    size: int,
    scenario: TestScenario
) -> Tuple[int, int, float, Optional[int]]:
    """
    发送请求并返回详细信息
    """
    content = "x" * size
    request = {
        "model": "glm-4.7",
        "messages": [{"role": "user", "content": content}],
        "stream": False
    }

    start = time.time()
    try:
        resp = requests.post(
            f"{base_url}/chat/completions",
            json=request,
            headers={"Authorization": "Bearer sk-perf-test"},
            timeout=30
        )
        latency = (time.time() - start) * 1000
        return (thread_id, seq, latency, resp.status_code)
    except requests.exceptions.Timeout:
        latency = (time.time() - start) * 1000
        return (thread_id, seq, latency, None)
    except Exception as e:
        latency = (time.time() - start) * 1000
        return (thread_id, seq, latency, None)

def run_scenario(base_url: str, scenario: TestScenario) -> PerformanceMetrics:
    """运行单个测试场景"""
    print(f"\n[*] Running scenario: {scenario.name}")
    print(f"    Requests: {scenario.num_requests}, Concurrency: {scenario.concurrency}")
    print(f"    Delay: {scenario.delay_ms}ms, Error rate: {scenario.error_rate}, Rate limit: {scenario.rate_limit_rate}")

    metrics = PerformanceMetrics()

    with ThreadPoolExecutor(max_workers=scenario.concurrency) as executor:
        futures = {}
        request_id = 0

        # 交替发送不同大小的请求
        for size in scenario.request_sizes * (scenario.num_requests // len(scenario.request_sizes) + 1):
            if request_id >= scenario.num_requests:
                break

            future = executor.submit(
                make_request,
                base_url,
                request_id % scenario.concurrency,
                request_id,
                size,
                scenario
            )
            futures[future] = request_id
            request_id += 1

        for future in as_completed(futures):
            try:
                thread_id, seq, latency, status_code = future.result()
                metrics.add_result(latency, status_code)

                # 简单进度显示（每100个请求）
                if seq % 100 == 0 and len(metrics.latencies) > 4:
                    try:
                        success_rate = metrics.success_count / (metrics.success_count + metrics.fail_count) * 100
                        print(f"    Progress: {seq}/{scenario.num_requests} | Success: {success_rate:.1f}% | P50: {statistics.quantiles(metrics.latencies)[4]:.1f}ms")
                    except Exception:
                        pass
            except Exception as e:
                print(f"    [ERROR] Request failed: {e}")

    return metrics

def start_test_environment(
    delay_ms: int = 50,
    error_rate: float = 0.02,
    rate_limit_rate: float = 0.05
) -> Tuple[subprocess.Popen, subprocess.Popen]:
    """启动测试环境（mock server + proxy）"""

    # 启动 mock server
    env = os.environ.copy()
    env.update({
        "MOCK_DELAY_MS": str(delay_ms),
        "MOCK_ERROR_RATE": str(error_rate),
        "MOCK_RATE_LIMIT_RATE": str(rate_limit_rate),
    })
    mock_proc = subprocess.Popen(
        ["python", "-u", "tests/mock_server.py"],
        cwd="D:/rustllmproxy",
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    time.sleep(3)

    # 启动 proxy (使用预编译二进制)
    proxy_proc = subprocess.Popen(
        ["./target/debug/unified-proxy.exe", "configs/test_perf.toml"],
        cwd="D:/rustllmproxy",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    time.sleep(5)

    return mock_proc, proxy_proc

def stop_test_environment(mock_proc: subprocess.Popen, proxy_proc: subprocess.Popen):
    """停止测试环境"""
    print("\n[*] Stopping test environment...")
    mock_proc.terminate()
    proxy_proc.terminate()

    try:
        mock_proc.wait(timeout=5)
        proxy_proc.wait(timeout=5)
    except:
        mock_proc.kill()
        proxy_proc.kill()

    print("[OK] Test environment stopped")

def main():
    parser = argparse.ArgumentParser(description="Complex performance test suite")
    parser.add_argument("--no-warmup", action="store_true", help="Skip warmup phase")
    args = parser.parse_args()

    print("="*70)
    print(" COMPLEX PERFORMANCE TEST SUITE")
    print("="*70)
    print("\nThis test simulates various production scenarios:")
    print("1. Low concurrency with small requests (baseline)")
    print("2. High concurrency (200) to test HTTP/2 multiplexing")
    print("3. Mixed request sizes (1KB, 10KB, 50KB)")
    print("4. High error rate (10%) to test failover")
    print("5. High rate limit (20%) to test key switching")
    print("6. Sustained load (1000 requests) to test connection reuse")
    print()

    # 定义测试场景
    scenarios = [
        TestScenario(
            name="Baseline - Low concurrency, small requests",
            num_requests=500,
            concurrency=10,
            delay_ms=50,
            error_rate=0.01,
            rate_limit_rate=0.02,
            request_sizes=[100]  # ~100 bytes
        ),
        TestScenario(
            name="High Concurrency - Test HTTP/2 multiplexing",
            num_requests=1000,
            concurrency=200,  # 高并发测试 HTTP/2
            delay_ms=50,
            error_rate=0.01,
            rate_limit_rate=0.02,
            request_sizes=[100],
        ),
        TestScenario(
            name="Mixed Request Sizes - 1KB, 10KB, 50KB",
            num_requests=600,
            concurrency=50,
            delay_ms=50,
            error_rate=0.01,
            rate_limit_rate=0.02,
            request_sizes=[100, 10240, 51200],  # 100B, 10KB, 50KB
        ),
        TestScenario(
            name="High Error Rate - Test failover mechanism",
            num_requests=500,
            concurrency=20,
            delay_ms=50,
            error_rate=0.15,  # 15% 错误率
            rate_limit_rate=0.02,
            request_sizes=[100],
        ),
        TestScenario(
            name="High Rate Limit - Test key switching",
            num_requests=500,
            concurrency=20,
            delay_ms=50,
            error_rate=0.01,
            rate_limit_rate=0.30,  # 30% rate limit
            request_sizes=[100],
        ),
        TestScenario(
            name="Sustained Load - Test connection pool reuse",
            num_requests=2000,
            concurrency=50,
            delay_ms=50,
            error_rate=0.01,
            rate_limit_rate=0.02,
            request_sizes=[100],
        ),
    ]

    # 启动测试环境（使用默认配置：50ms 延迟，1% 错误率，2% rate limit）
    print("[*] Starting test environment...")
    mock_proc, proxy_proc = start_test_environment(
        delay_ms=50,
        error_rate=0.01,
        rate_limit_rate=0.02
    )
    print("[OK] Test environment ready")

    # 预热
    if not args.no_warmup:
        print("\n[*] Warming up...")
        warmup_scenario = TestScenario(
            name="Warmup",
            num_requests=50,
            concurrency=10,
            delay_ms=50,
            error_rate=0.01,
            rate_limit_rate=0.02,
            request_sizes=[100]
        )
        run_scenario("http://localhost:8090/openai/v1", warmup_scenario)

    # 运行所有场景
    results = []
    for i, scenario in enumerate(scenarios, 1):
        print(f"\n{'='*70}")
        print(f" SCENARIO {i}/{len(scenarios)}: {scenario.name}")
        print(f"{'='*70}")
        metrics = run_scenario("http://localhost:8090/openai/v1", scenario)
        results.append((scenario.name, metrics))

        # 打印结果
        r = metrics.report()
        if "error" not in r:
            print(f"\nResults:")
            print(f"  Total Requests:  {r['total_requests']}")
            print(f"  Success Rate:    {r['success_rate']:.1f}%")
            print(f"  Failures:         {r['failed']} (429: {r['rate_limit']}, 5xx: {r['server_error']}, 4xx: {r['client_error']}, Timeout: {r['timeout']})")
            print(f"  Latency (ms):")
            print(f"    P50: {r['p50']:.1f}")
            print(f"    P75: {r['p75']:.1f}")
            print(f"    P90: {r['p90']:.1f}")
            print(f"    P95: {r['p95']:.1f}")
            print(f"    P99: {r['p99']:.1f}")
            print(f"    Avg: {r['avg']:.1f}")
            print(f"    Min: {r['min']:.1f}")
            print(f"    Max: {r['max']:.1f}")

    # 对比分析
    print(f"\n{'='*70}")
    print(" SUMMARY - Comparing scenarios")
    print(f"{'='*70}")
    print(f"{'Scenario':<40} {'P50':>10} {'P95':>10} {'P99':>10} {'Success':>10}")
    print(f"{'-'*70}")

    for name, metrics in results:
        r = metrics.report()
        if "error" not in r:
            print(f"{name:<40} {r['p50']:>10.1f} {r['p95']:>10.1f} {r['p99']:>10.1f} {r['success_rate']:>9.1f}%")

    # 停止测试环境
    stop_test_environment(mock_proc, proxy_proc)

    print(f"\n{'='*70}")
    print(" TEST COMPLETE")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
