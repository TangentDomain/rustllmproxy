#!/bin/bash
cd "$(dirname "$0")"

pkill -f 'run-home/unified-proxy-home' 2>/dev/null

mkdir -p run-home
cp -f target/release/unified-proxy run-home/unified-proxy-home

echo "Starting Unified LLM Proxy (HOME)..."
./run-home/unified-proxy-home configs/unified-home.toml &

sleep 3
curl -s http://127.0.0.1:8092/health
echo ""
echo "Done. HOME Proxy running on :8092"
echo ""
echo "  OpenAI:    http://localhost:8092/openai/v1/chat/completions"
echo "  Anthropic: http://localhost:8092/anthropic/v1/messages"
