# WRETH Local Development

This directory contains the infrastructure for running WRETH locally for development and testing. It provides a complete stack with:

- **wreth**: Reth node with Remote ExEx gRPC server
- **wavs**: WAVS node configured to use the ExEx endpoint
- **jaeger**: Distributed tracing UI
- **prometheus**: Metrics collection and monitoring

## Overview

WRETH enables direct streaming of blockchain execution data from reth to WAVS via gRPC, bypassing the standard WebSocket JSON-RPC interface. This provides:

- Lower latency event delivery
- Binary serialization (bincode) instead of JSON
- Direct access to execution engine data
- Reduced network overhead

## Prerequisites

### Docker (Recommended)

- Docker 20.10+
- Docker Compose v2.0+

### Native Development

- Rust 1.88+ (nightly recommended)
- Clang and LLVM for building reth
- Protocol Buffers compiler (`protoc`)

## Quick Start (Docker)

1. **Navigate to the wreth directory:**
   ```bash
   cd wreth
   ```

2. **Copy the environment file:**
   ```bash
   cp .env.example .env
   ```

3. **Start the stack:**
   ```bash
   docker compose up -d
   ```

4. **View logs:**
   ```bash
   docker compose logs -f
   ```

5. **Access services:**
   - **Reth RPC**: http://localhost:8545
   - **Reth WebSocket**: ws://localhost:8546
   - **ExEx gRPC**: localhost:10000
   - **WAVS API**: http://localhost:8000
   - **Jaeger UI**: http://localhost:16686
   - **Prometheus**: http://localhost:9090

6. **Stop the stack:**
   ```bash
   docker compose down
   ```

## Native Development

For development without Docker, you can run components individually.

### Building wreth

```bash
# From repository root
cargo build -p wreth --release
```

### Building WAVS with ExEx Support

```bash
cd lib/WAVS
cargo build -p wavs --features reth-exex --release
```

### Running Manually

**Terminal 1 - Start wreth:**
```bash
./target/release/wreth node --dev \
  --http --http.addr 0.0.0.0 \
  --ws --ws.addr 0.0.0.0 \
  --exex.addr [::]:10000
```

**Terminal 2 - Start WAVS:**
```bash
cd lib/WAVS
RUST_LOG=info,wavs=debug ./target/release/wavs
```

## Configuration Reference

### wreth CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--exex.addr` | `[::]:10000` | Address for the ExEx gRPC server to bind to |
| `--exex.channel-capacity` | `256` | Channel capacity for notification broadcast |

Standard reth options also apply (e.g., `--dev`, `--http`, `--ws`, etc.).

### WAVS ExEx Configuration

In `wavs.toml`, configure the ExEx endpoint for a chain:

```toml
[default.chains.evm.local-wreth]
chain_id = 1337
ws_endpoints = ["ws://wreth:8546"]
http_endpoint = "http://wreth:8545"
exex_endpoint = "http://wreth:10000"  # Key configuration
```

When `exex_endpoint` is configured and WAVS is built with `--features reth-exex`:
- WAVS connects to the ExEx gRPC server
- Block and log events flow directly from reth's execution engine
- WebSocket subscriptions are bypassed for this chain

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Logging level (e.g., `info,wavs=debug,reth=info`) |
| `WAVS_SUBMISSION_MNEMONIC` | Mnemonic for transaction signing |

## Verifying the Integration

### 1. Check gRPC Connection

Look for this log message in WAVS:
```
Connected to ExEx server at http://wreth:10000
```

### 2. Monitor Block Events

With debug logging enabled, you should see:
```
Received ExEx notification: ChainCommitted { ... }
```

### 3. Deploy a Test Component

```bash
# Build a test WASM component (from lib/WAVS)
cd lib/WAVS
cargo component build -p evm-trigger-echo --release

# Deploy via wavs-cli
docker compose exec wavs wavs-cli deploy-service \
  --component /path/to/evm_trigger_echo.wasm
```

### 4. Trigger an Event

Send a transaction to the dev network and observe the trigger flowing through:

```bash
# Using cast (from foundry)
cast send --private-key <DEV_KEY> \
  0x... "someFunction()" \
  --rpc-url http://localhost:8545
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        WRETH NODE                                │
│  ┌──────────────────┐    ┌──────────────────────────────────┐  │
│  │ Execution Engine │───▶│ Remote ExEx (gRPC Server :10000) │  │
│  └──────────────────┘    └───────────────┬──────────────────┘  │
└──────────────────────────────────────────┼──────────────────────┘
                                           │ gRPC Stream
                                           │ (bincode ExExNotification)
                                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        WAVS NODE                                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    TriggerManager                           │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌────────────┐  │ │
│  │  │ ExExStream      │  │ EvmTriggerStream│  │ CronStream │  │ │
│  │  │ (gRPC Client)   │  │ (WebSocket)     │  │            │  │ │
│  │  └────────┬────────┘  └────────┬────────┘  └─────┬──────┘  │ │
│  │           └───────────────┬────┴─────────────────┘         │ │
│  │                           ▼                                 │ │
│  │               MultiplexedStream (SelectAll)                 │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### Connection Refused Errors

```
ExEx connection error: Failed to connect to http://wreth:10000
```

**Solutions:**
- Verify wreth is running and healthy: `docker compose ps`
- Check wreth logs for startup errors: `docker compose logs wreth`
- Ensure the `--exex.addr` flag is set correctly
- Verify network connectivity between containers

### Bincode Deserialization Errors

```
ExEx deserialization error: Bincode decode error
```

**Solutions:**
- Version mismatch between WAVS reth dependencies and wreth
- Ensure both use compatible versions (currently aligned with reth v1.10.1)
- Rebuild both services after updating dependencies

### Feature Not Enabled

```
ExEx endpoint configured but reth-exex feature not enabled
```

**Solutions:**
- Rebuild WAVS with the feature: `cargo build --features reth-exex`
- Or use the provided `Dockerfile.wavs` which includes the feature

### No Events Received

**Check:**
1. wreth is producing blocks (check logs)
2. ExEx gRPC server started successfully
3. WAVS connected to the ExEx endpoint
4. Chain ID matches between wreth and wavs.toml

### High Memory Usage

If wreth consumes too much memory with `--dev`:
- Reduce channel capacity: `--exex.channel-capacity 64`
- Enable pruning if running for extended periods

## Files in This Directory

| File | Purpose |
|------|---------|
| `Dockerfile` | Builds the wreth binary |
| `Dockerfile.wavs` | Builds WAVS with reth-exex feature |
| `docker-compose.yml` | Orchestrates the full stack |
| `wavs.toml` | WAVS configuration with ExEx endpoint |
| `.env.example` | Environment variable template |
| `prometheus.yml` | Prometheus scrape configuration |
| `README.md` | This documentation |
