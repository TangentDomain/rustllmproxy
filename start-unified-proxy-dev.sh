#!/bin/bash
cd "$(dirname "$0")"

pkill -f 'run-dev/unified-proxy-dev' 2>/dev/null

mkdir -p run-dev
cp -f target/release/unified-proxy run-dev/unified-proxy-dev

echo "Starting Unified LLM Proxy (DEV)..."
./run-dev/unified-proxy-dev configs/unified-dev.toml &

sleep 3
curl -s http://127.0.0.1:8091/health
echo ""
echo "Done. DEV Proxy running on :8091"
echo ""
echo "  OpenAI:    http://localhost:8091/openai/v1/chat/completions"
echo "  Anthropic: http://localhost:8091/anthropic/v1/messages"
