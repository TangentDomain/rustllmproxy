#!/bin/bash
# 性能负载测试脚本
# 用法: bash tests/perf_test.sh

set -e

PROXY_URL="http://127.0.0.1:8090"
MOCK_PORT=8765
AUTH_KEY="sk-perf-test"
CONCURRENCY=50
TOTAL_REQUESTS=500

echo "=== LLM Proxy 性能测试 ==="
echo ""

# --- 清理 ---
cleanup() {
    echo "Cleaning up..."
    taskkill //F //IM mock_llm_server.exe 2>/dev/null || true
    taskkill //F //IM unified-proxy.exe 2>/dev/null || true
}
trap cleanup EXIT
cleanup

# --- 1. 启动 Mock 后端 ---
echo "[1/4] 启动 Mock 后端 (port=$MOCK_PORT, delay=20ms)..."
MOCK_DELAY_MS=20 MOCK_PORT=$MOCK_PORT ./target/release/mock_llm_server.exe &
sleep 1

# 验证 mock 可用
if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:$MOCK_PORT/v1/chat/completions \
    -X POST -H "Content-Type: application/json" \
    -d '{"model":"glm-4.7","messages":[{"role":"user","content":"hi"}]}' | grep -q "200"; then
    echo "  Mock backend OK"
else
    echo "  ERROR: Mock backend not responding"
    exit 1
fi

# --- 2. 启动 Proxy ---
echo "[2/4] 启动 Proxy (config=test_perf.toml)..."
./target/release/unified-proxy.exe configs/test_perf.toml &
sleep 1

# 验证 proxy 可用
if curl -s -o /dev/null -w "%{http_code}" $PROXY_URL/health | grep -q "200"; then
    echo "  Proxy OK"
else
    echo "  ERROR: Proxy not responding"
    exit 1
fi

# --- 3. 功能验证 ---
echo "[3/4] 功能验证..."
RESP=$(curl -s -w "\n%{http_code}" -X POST $PROXY_URL/v1/chat/completions \
    -H "Authorization: Bearer $AUTH_KEY" \
    -H "Content-Type: application/json" \
    -d '{"model":"glm-4.7","messages":[{"role":"user","content":"hi"}]}')
HTTP_CODE=$(echo "$RESP" | tail -1)
if [ "$HTTP_CODE" = "200" ]; then
    echo "  Single request OK (200)"
else
    echo "  ERROR: Single request returned $HTTP_CODE"
    echo "$RESP"
    exit 1
fi

# --- 4. 并发负载测试 ---
echo "[4/4] 并发负载测试 ($CONCURRENCY 并发, $TOTAL_REQUESTS 请求)..."
echo ""

START_MS=$(date +%s%3N)

# 用 curl 并发发送请求
SEQ=$(seq 1 $TOTAL_REQUESTS)
SUCCESS=0
FAIL=0
for i in $SEQ; do
    (
        CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST $PROXY_URL/v1/chat/completions \
            -H "Authorization: Bearer $AUTH_KEY" \
            -H "Content-Type: application/json" \
            -d '{"model":"glm-4.7","messages":[{"role":"user","content":"perf test"}]}')
        if [ "$CODE" = "200" ]; then
            echo "1" >> /tmp/perf_ok
        else
            echo "1" >> /tmp/perf_fail
        fi
    ) &
    # 控制并发数
    if (( i % CONCURRENCY == 0 )); then
        wait
    fi
done
wait

END_MS=$(date +%s%3N)

SUCCESS=$(wc -l < /tmp/perf_ok 2>/dev/null || echo 0)
FAIL=$(wc -l < /tmp/perf_fail 2>/dev/null || echo 0)
rm -f /tmp/perf_ok /tmp/perf_fail

ELAPSED=$(( END_MS - START_MS ))
RPS=$(echo "scale=1; $TOTAL_REQUESTS * 1000 / $ELAPSED" | bc)
AVG_MS=$(echo "scale=1; $ELAPSED * 1000 / $TOTAL_REQUESTS" | bc)

echo ""
echo "=== 性能测试结果 ==="
echo "  总请求数: $TOTAL_REQUESTS"
echo "  成功: $SUCCESS"
echo "  失败: $FAIL"
echo "  总耗时: ${ELAPSED}ms"
echo "  吞吐量: ${RPS} req/s"
echo "  平均延迟: ${AVG_MS}us"
echo "  并发数: $CONCURRENCY"
