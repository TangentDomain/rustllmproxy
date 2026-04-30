#!/bin/bash
cd "$(dirname "$0")"

pkill -f 'run/unified-proxy' 2>/dev/null

mkdir -p run
cp -f target/release/unified-proxy run/unified-proxy

echo "Starting Unified LLM Proxy..."
./run/unified-proxy configs/unified.toml &

sleep 3
curl -s http://127.0.0.1:8090/health
echo ""
echo "Done. Unified Proxy running on :8090"
echo ""
echo "Supports both OpenAI and Anthropic APIs:"
echo "  OpenAI:    http://localhost:8090/openai/v1/chat/completions"
echo "  Anthropic: http://localhost:8090/anthropic/v1/messages"
