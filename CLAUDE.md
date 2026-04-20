# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build release
cargo build --release

# Production (port 8090)
start-unified-proxy.bat

# Development (port 8091) - isolated environment
start-unified-proxy-dev.bat

# Stop all
taskkill /F /IM unified-proxy.exe
```

**Important**: The startup scripts copy binaries to `run/` or `run-dev/` directories, allowing continued development without locking the running process.

## Environment Setup

1. Copy `.env.example` to `.env` and configure API keys
2. Both prod and dev share the same `.env` file
3. Dev environment writes logs to `logs-dev/`, prod to `logs/`
4. When testing changes during active development, use port 8091 (dev env)

## Architecture

### Request Flow

```
Request → unified.rs (route by path prefix)
  → auth_layer (API key + rate limit check)
  → proxy::handle()
    → Extract model from JSON (fast scan, no full parse)
    → Get fallback chain for model
    → For each model in chain:
      → Find backends supporting model (filter by protocol)
      → Filter to healthy backends only
      → Try each healthy backend once (no retry, move to next):
        → WeightedRoundRobin::select()
        → do_forward() → backend API
        → On 429/5xx/4xx(except 401): continue to next backend
        → On 200: instrument_stream() → track metrics → return
```

### Key Components

- **`proxy.rs`**: Main proxy logic with fallback chains. Forwards requests, handles errors, tracks metrics (TTFB, TTFT, tok/s)
- **`balancer.rs`**: `WeightedRoundRobin` with O(1) health checks and binary search for weighted selection
- **`config.rs`**: TOML parsing with environment variable expansion (`${VAR}`)
- **`middleware.rs`**: API key authentication and rate limiting

### Error Handling & Fallback Strategy

| Status | Behavior |
|--------|----------|
| 401 | Return directly (key invalid, no retry) |
| 429 | Switch to next key immediately |
| Other 4xx | Fallback to next backend (capability mismatch) |
| 5xx | Fallback to next backend |
| Healthy check | Backend skipped if marked unhealthy by balancer |

### Protocol Handling

- **OpenAI**: Path prefix `/openai/v1/` → strip prefix when forwarding
- **Anthropic**: Path prefix `/anthropic/v1/` → strip prefix when forwarding
- **Protocol detection**: Set by `unified.rs` via `request.extensions_mut().insert(protocol)`
- **Special case**: Bigmodel OpenAI API rewrites `/v1/` → `/v4/` (coding API requirement)

### Configuration Structure

```toml
[server]
port = 8090              # or 8091 for dev
log_dir = "logs"         # or "logs-dev" for dev

[[backends]]
name = "backend-name"
protocol = "openai"      # or "anthropic"
url = "..."
api_key = "${VAR}"      # env var expansion
models = ["model-1"]
weight = 10              # for weighted selection

[fallback]
"model-name" = ["fallback-1", "fallback-2"]
```

### Performance Tracking

Every request logs:
- `ttfb`: Time to first byte (backend initial response)
- `ttft`: Time to first text token (after thinking, if any)
- `total`: Complete request duration (including streaming)
- `tokens`: Output token count
- `tok/s`: Throughput

For streaming responses, the proxy wraps the body to track tokens in SSE chunks and logs metrics when stream completes.

### Weighted Round-Robin

- Maintains cumulative weight prefix for binary search selection
- O(log n) backend selection, O(1) health check by name
- Unhealthy backends excluded from selector until health check recovers them
- Health check runs every 30s in background task

### HTTP/2 Connection Pooling

- `pool_max_idle_per_host(50)`: 50 idle connections per backend
- `pool_idle_timeout(120s)`: Reuse connections for 2 minutes
- HTTP/2 multiplexing enabled via `reqwest` feature
- DNS caching via `trust-dns` feature
