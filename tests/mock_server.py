#!/usr/bin/env python3
"""
简单的 Mock LLM 服务器 - 用于性能测试
支持环境变量配置延迟和错误率
"""
import os
import time
import json
from flask import Flask, request, jsonify
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 配置
DELAY_MS = int(os.environ.get("MOCK_DELAY_MS", "50"))
ERROR_RATE = float(os.environ.get("MOCK_ERROR_RATE", "0.05"))
RATE_LIMIT_RATE = float(os.environ.get("MOCK_RATE_LIMIT_RATE", "0.0"))
PORT = int(os.environ.get("MOCK_PORT", "8766"))

import random

@app.route("/v1/chat/completions", methods=["POST"])
@app.route("/v4/chat/completions", methods=["POST"])
def chat_completions():
    data = request.get_json()
    stream = data.get("stream", False)
    model = data.get("model", "unknown")
    logger.info(f"chat_completions: stream={stream}, model={model}")

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

    if stream:
        # SSE streaming response
        from flask import Response
        import uuid

        def generate():
            req_id = f"chatcmpl-{uuid.uuid4().hex[:8]}"
            created = int(time.time())

            # 模拟 TTFB (首字节延迟)
            time.sleep(DELAY_MS / 1000.0)

            # 发送多个chunk模拟token流
            chunks = [
                "This", " is", " a", " mock", " response",
                " for", " performance", " testing", "."
            ]

            for chunk in chunks:
                chunk_data = {
                    "id": req_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {"content": chunk},
                        "finish_reason": None
                    }]
                }
                yield f"data: {json.dumps(chunk_data)}\n\n"
                time.sleep(DELAY_MS / 2000.0)  # 每个chunk间隔

            # 最后一个chunk
            final_data = {
                "id": req_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop"
                }]
            }
            yield f"data: {json.dumps(final_data)}\n\n"
            yield "data: [DONE]\n\n"

        return Response(generate(), mimetype="text/event-stream")
    else:
        # Non-streaming response
        time.sleep(DELAY_MS / 1000.0)
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

@app.route("/v1/messages", methods=["POST"])
@app.route("/anthropic/v1/messages", methods=["POST"])
def messages():
    from flask import Response
    import uuid

    data = request.get_json()
    stream = data.get("stream", False)
    model = data.get("model", "unknown")

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

    req_id = f"msg_{uuid.uuid4().hex[:8]}"

    if stream:
        def generate():
            # 模拟 TTFB
            time.sleep(DELAY_MS / 1000.0)

            # 发送事件类型
            chunks = [
                ("message_start", {
                    "type": "message_start",
                    "message": {
                        "id": req_id,
                        "type": "message",
                        "role": "assistant",
                        "content": [],
                        "model": model,
                        "stop_reason": None
                    }
                }),
                ("content_block_start", {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""}
                }),
            ]

            words = ["This", " is", " a", " mock", " response", "."]

            for word in words:
                chunks.append(("content_block_delta", {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": word}
                }))
                time.sleep(DELAY_MS / 2000.0)

            chunks.append(("content_block_stop", {
                "type": "content_block_stop",
                "index": 0
            }))
            chunks.append(("message_stop", {
                "type": "message_stop"
            }))

            for event_type, data in chunks:
                yield f"event: {event_type}\n"
                yield f"data: {json.dumps(data)}\n\n"

        return Response(generate(), mimetype="text/event-stream")
    else:
        time.sleep(DELAY_MS / 1000.0)
        return jsonify({
            "id": req_id,
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "This is a mock response for performance testing."}],
            "model": model,
            "stop_reason": "stop"
        })

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "delay_ms": DELAY_MS})

if __name__ == "__main__":
    logger.info(f"Mock LLM server starting on port {PORT}")
    logger.info(f"Config: delay={DELAY_MS}ms, error_rate={ERROR_RATE}, rate_limit_rate={RATE_LIMIT_RATE}")
    app.run(host="127.0.0.1", port=PORT, threaded=True)
