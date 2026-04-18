"""
简化的性能测试 - 分步测试
"""
import subprocess
import time
import requests
import os

BASE_URL = "http://localhost:8090/openai/v1"

def test_mock_server():
    """测试 mock 服务器"""
    print("[*] Testing mock server directly...")
    resp = requests.post("http://127.0.0.1:8765/v1/chat/completions",
        json={"model": "glm-4.7", "messages": [{"role": "user", "content": "test"}]},
        timeout=5)
    print(f"    Status: {resp.status_code}")
    print(f"    Latency: {(resp.elapsed.total_seconds() * 1000):.1f}ms")
    print(f"    Response: {resp.json()}")
    return resp.status_code == 200

def test_proxy():
    """测试 proxy"""
    print("\n[*] Testing proxy...")
    resp = requests.post(f"{BASE_URL}/chat/completions",
        json={"model": "glm-4.7", "messages": [{"role": "user", "content": "test"}]},
        headers={"Authorization": "Bearer sk-perf-test"},
        timeout=10)
    print(f"    Status: {resp.status_code}")
    print(f"    Latency: {(resp.elapsed.total_seconds() * 1000):.1f}ms")
    if resp.status_code == 200:
        print(f"    Response: {resp.json()}")
    else:
        print(f"    Error: {resp.text}")
    return resp.status_code == 200

def run_single_test():
    """运行单次测试"""
    print("="*60)
    print("SINGLE TEST - Testing current optimized proxy")
    print("="*60)

    # 测试 mock server
    if not test_mock_server():
        print("[ERROR] Mock server failed!")
        return

    # 测试 proxy
    if test_proxy():
        print("\n[SUCCESS] Both mock server and proxy work!")
    else:
        print("\n[ERROR] Proxy test failed!")

if __name__ == "__main__":
    run_single_test()
