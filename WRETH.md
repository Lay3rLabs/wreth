# WRETH: Reth ExEx Integration with WAVS

WRETH integrates reth's remote Execution Extension (ExEx) with WAVS, enabling blockchain execution data to flow directly from reth to WAVS triggers via gRPC, bypassing the standard WebSocket JSON-RPC interface.

## Overview

Traditional blockchain event monitoring relies on WebSocket subscriptions to JSON-RPC endpoints (`eth_subscribe`). This approach has limitations:
- Additional serialization/deserialization overhead
- Limited to RPC-exposed data
- Potential latency from RPC layer

WRETH provides a direct pipeline from reth's execution engine to WAVS using reth's ExEx (Execution Extension) system, which streams execution notifications via gRPC with bincode serialization.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RETH NODE                                 │
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
│  │                           │                                 │ │
│  │                           ▼                                 │ │
│  │                 LookupMaps → DispatcherCommand              │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Input: ExExNotification from reth

Reth streams three types of notifications:

| Notification | Description | WAVS Handling |
|-------------|-------------|---------------|
| `ChainCommitted` | New blocks committed | Process all blocks, emit triggers |
| `ChainReorged` | Chain reorganization | Process only new chain (old is reverted) |
| `ChainReverted` | Blocks reverted | Skip (no new events to emit) |

Each notification contains:
- `Chain`: Collection of blocks with execution outcomes
- `ExecutionOutcome`: Receipts containing transaction logs

### Output: StreamTriggers to WAVS

The ExEx stream converts notifications to WAVS trigger types:

```rust
StreamTriggers::Evm {
    chain: ChainKey,
    log: Box<alloy_rpc_types_eth::Log>,
    block_number: u64,
    tx_hash, block_hash, tx_index, block_timestamp, log_index,
}

StreamTriggers::EvmBlock {
    chain: ChainKey,
    block_height: u64,
}
```

## Configuration

### WAVS Chain Configuration

Add `exex_endpoint` to your EVM chain configuration:

```toml
[chains.evm.local-reth]
ws_endpoints = ["ws://127.0.0.1:8546"]      # Fallback WebSocket
http_endpoint = "http://127.0.0.1:8545"
exex_endpoint = "http://[::1]:10000"        # ExEx gRPC endpoint
```

When `exex_endpoint` is configured and the `reth-exex` feature is enabled:
- WAVS connects to the ExEx gRPC server
- WebSocket subscriptions are bypassed for this chain
- Block and log events flow directly from reth's execution engine

When `exex_endpoint` is not configured or the feature is disabled:
- WAVS uses standard WebSocket subscriptions (existing behavior)

### Reth Configuration

The `reth-exex-remote` crate provides the gRPC server that streams ExEx notifications. Install it as an ExEx on your reth node:

```rust
use reth_exex_remote::{RemoteExExServer, RemoteExExConfig, remote_exex};
use reth_node_ethereum::EthereumNode;

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let config = RemoteExExConfig::default();
        let (server, notifications) = RemoteExExServer::new(config);

        let handle = builder
            .node(EthereumNode::default())
            .install_exex("remote-exex", |ctx| {
                remote_exex(ctx, notifications)
            })
            .launch()
            .await?;

        // Start gRPC server as a critical task
        handle.node.task_executor.spawn_critical("gRPC server", server.serve());
        handle.wait_for_node_exit().await
    })
}
```

#### Server Configuration Options

```rust
let config = RemoteExExConfig::default()
    .with_addr("[::1]:10000".parse()?)  // gRPC listen address
    .with_channel_capacity(512);         // Notification buffer size
```

| Option | Default | Description |
|--------|---------|-------------|
| `addr` | `[::1]:10000` | Address for the gRPC server to bind to |
| `channel_capacity` | `256` | Broadcast channel capacity for notifications |
| `max_encoding_message_size` | `usize::MAX` | Maximum gRPC message encoding size |
| `max_decoding_message_size` | `usize::MAX` | Maximum gRPC message decoding size |

## Building

### Reth with Remote ExEx Server

Build the reth-exex-remote crate:

```bash
cargo build -p reth-exex-remote
```

To use it in your custom reth node, add to your `Cargo.toml`:

```toml
[dependencies]
reth-exex-remote = { path = "crates/exex/remote" }
```

### WAVS with ExEx Support

Build WAVS with the `reth-exex` feature:

```bash
cd lib/WAVS
cargo build -p wavs --features reth-exex
```

### Feature Flag

The `reth-exex` feature enables:
- gRPC client for ExEx connection
- reth type dependencies for notification deserialization
- ExEx stream integration in TriggerManager

Without this feature, WAVS builds normally and ignores `exex_endpoint` configuration.

## Implementation Details

### Files Modified/Created

#### Reth Server Side (`crates/exex/remote/`)

| File | Purpose |
|------|---------|
| `crates/exex/remote/Cargo.toml` | Crate manifest with dependencies |
| `crates/exex/remote/proto/exex.proto` | gRPC service definition |
| `crates/exex/remote/build.rs` | Proto compilation for server |
| `crates/exex/remote/src/lib.rs` | RemoteExExServer, NotificationSender, remote_exex function |

#### WAVS Client Side (`lib/WAVS/`)

| File | Purpose |
|------|---------|
| `lib/WAVS/packages/types/src/chain_config.rs` | Added `exex_endpoint` field |
| `lib/WAVS/packages/wavs/Cargo.toml` | Added dependencies and feature flag |
| `lib/WAVS/packages/wavs/proto/exex.proto` | gRPC service definition (client) |
| `lib/WAVS/packages/wavs/build.rs` | Proto compilation for client |
| `lib/WAVS/packages/wavs/src/subsystems/trigger/streams/exex_stream.rs` | ExEx stream implementation |
| `lib/WAVS/packages/wavs/src/subsystems/trigger/streams.rs` | Module registration |
| `lib/WAVS/packages/wavs/src/subsystems/trigger.rs` | TriggerManager integration |
| `lib/WAVS/packages/wavs/src/subsystems/trigger/error.rs` | Error types |

### Server-Side API (`reth-exex-remote`)

| Type | Description |
|------|-------------|
| `RemoteExExServer<N>` | gRPC server that streams notifications to clients |
| `RemoteExExConfig` | Server configuration (address, channel capacity, message sizes) |
| `NotificationSender<N>` | Handle for sending notifications to all connected clients |
| `remote_exex()` | ExEx function that forwards execution notifications to gRPC clients |

### Reconnection Strategy (Client)

The WAVS ExEx stream implements automatic reconnection:
- Base delay: 1 second
- Max delay: 60 seconds
- Max reconnects: 10 attempts
- Exponential backoff with jitter

### Dependencies

#### Reth Server Side

```toml
reth-exex = { workspace = true }
reth-exex-types = { workspace = true, features = ["serde", "serde-bincode-compat"] }
reth-node-api = { workspace = true }
tonic = { workspace = true }
prost = { workspace = true }
bincode = "1"
```

#### WAVS Client Side

WAVS alloy dependencies are aligned with reth v1.10.1:
- alloy-* packages: 1.4.3
- alloy-primitives: 1.5.0
- alloy-sol-types: 1.5.0

## Serialization

The ExEx notifications are serialized using bincode with reth's `serde_bincode_compat` wrappers, which provide stable binary serialization for reth's primitive types.

**Wire format:**
```
gRPC message → protobuf bytes field → bincode-serialized ExExNotification
```

**Server-side serialization:**
```rust
let wrapper = ExExNotificationWrapper { notification: &notification };
let data = bincode::serialize(&wrapper)?;
let proto = ProtoExExNotification { data };
```

**Client-side deserialization:**
```rust
let notification: ExExNotification<EthPrimitives> =
    bincode::deserialize(&proto.data)?;
```

Both sides must use compatible reth versions to ensure bincode compatibility.

## Comparison: ExEx vs WebSocket

| Aspect | ExEx (gRPC) | WebSocket (JSON-RPC) |
|--------|-------------|---------------------|
| Serialization | bincode (binary) | JSON |
| Data Source | Execution engine | RPC layer |
| Latency | Lower | Higher |
| Data Richness | Full execution context | RPC-filtered |
| Setup Complexity | Requires ExEx config | Standard RPC |
| Network Protocol | HTTP/2 (gRPC) | WebSocket |

## Troubleshooting

### Connection Issues

```
ExEx connection error: Failed to connect
```
- Verify reth is running with `--exex remote`
- Check the ExEx endpoint address and port
- Ensure network connectivity between WAVS and reth

### Deserialization Errors

```
ExEx deserialization error: Bincode decode error
```
- Version mismatch between WAVS reth dependencies and reth node
- Ensure both use compatible versions (currently aligned with reth v1.10.1)

### Feature Not Enabled

```
ExEx endpoint configured but reth-exex feature not enabled
```
- Rebuild WAVS with `--features reth-exex`

## Running Locally

A complete local development stack is available in the [`wreth/`](./wreth/) directory. This includes Docker Compose configuration for running wreth + WAVS + observability tools.

### Quick Start (Docker)

```bash
cd wreth
cp .env.example .env
docker compose up -d
docker compose logs -f
```

### Services

| Service | Port | Description |
|---------|------|-------------|
| wreth | 8545, 8546, 10000 | Reth node with ExEx gRPC server |
| wavs | 8000 | WAVS with ExEx client |
| jaeger | 16686 | Distributed tracing UI |
| prometheus | 9090 | Metrics dashboard |

### Native Development

```bash
# Build wreth
cargo build -p wreth --release

# Run wreth with ExEx
./target/release/wreth node --dev \
  --http --http.addr 0.0.0.0 \
  --ws --ws.addr 0.0.0.0 \
  --exex.addr [::]:10000

# Build and run WAVS (separate terminal)
cd lib/WAVS
cargo build -p wavs --features reth-exex --release
./target/release/wavs
```

See [`wreth/README.md`](./wreth/README.md) for complete documentation.

## Future Improvements

- [ ] Metrics for ExEx stream performance
- [ ] Configurable reconnection parameters
- [ ] Support for filtered subscriptions (specific addresses/events)
- [ ] Integration tests with reth test utilities
