#!/bin/bash
cd "$(dirname "$0")"

pkill -f 'run-home/unified-proxy-home' 2>/dev/null

mkdir -p run-home
cp -f target/release/unified-proxy run-home/unified-proxy-home

echo "Starting Unified LLM Proxy (HOME)..."
exec ./run-home/unified-proxy-home configs/unified-home.toml
