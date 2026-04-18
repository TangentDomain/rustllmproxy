#!/usr/bin/env python3
"""
简单的 Mock LLM 服务器 - 用于性能测试
支持环境变量配置延迟和错误率
"""
import os
import time
import json
from flask import Flask, request, jsonify
from threading import Thread
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 配置
DELAY_MS = int(os.environ.get("MOCK_DELAY_MS", "50"))
ERROR_RATE = float(os.environ.get("MOCK_ERROR_RATE", "0.05"))
RATE_LIMIT_RATE = float(os.environ.get("MOCK_RATE_LIMIT_RATE", "0.0"))
PORT = int(os.environ.get("MOCK_PORT", "8765"))

import random

@app.route("/v1/chat/completions", methods=["POST"])
@app.route("/v4/chat/completions", methods=["POST"])
def chat_completions():
    # 模拟延迟
    time.sleep(DELAY_MS / 1000.0)

    # 检查 rate limit
    if random.random() < RATE_LIMIT_RATE:
        return jsonify({
            "error": {
                "message": "Rate limit exceeded",
                "type": "rate_limit_error"
            }
        }), 429

    # 检查错误
    if random.random() < ERROR_RATE:
        return jsonify({
            "error": {
                "message": "Mock server error",
                "type": "server_error"
            }
        }), 500

    data = request.get_json()
    model = data.get("model", "unknown")

    return jsonify({
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "This is a mock response for performance testing."
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "total_tokens": 30
        }
    })

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "delay_ms": DELAY_MS})

if __name__ == "__main__":
    logger.info(f"Mock LLM server starting on port {PORT}")
    logger.info(f"Config: delay={DELAY_MS}ms, error_rate={ERROR_RATE}, rate_limit_rate={RATE_LIMIT_RATE}")
    app.run(host="127.0.0.1", port=PORT, threaded=True)
