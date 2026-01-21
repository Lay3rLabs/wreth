# Plan

We are going to hook WAVS (Web Assembly Verifiable Services runtime) up to a reth remote execution extension: https://reth.rs/exex/remote

We've installed WAVS as a git submodule in `lib/WAVS`.

Explore the reth Exex docs and `wavs` codebase to determine how best to integrate WAVS as a remote exex and come up with a detailed implmentation plan. Ask questions as you need.
## Why?

Run a validator and WAVS node at the same time *with as little latency* as possible. 

This a stack you can use to make your own L1 / L2 EVM blockchain. Leverage WAVS for custom oracles, bridging, intelligent DeFi strategies, compliance, reputation scores, and more.

# Integration Plan: Reth Remote ExEx → WAVS

  ## Overview

  Integrate reth's remote ExEx (Execution Extension) with WAVS so blockchain execution data flows directly from reth to
  WAVS triggers via gRPC, bypassing the standard WebSocket JSON-RPC interface.

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
  │  │  │ (NEW - gRPC)    │  │ (WebSocket)     │  │            │  │ │
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

  ## Data Transformation

  ### Input (from reth via gRPC)
  ```rust
  // ExExNotification variants
  ChainCommitted { new: Arc<Chain> }
  ChainReorged { old: Arc<Chain>, new: Arc<Chain> }
  ChainReverted { old: Arc<Chain> }

  // Chain contains
  blocks: BTreeMap<BlockNumber, RecoveredBlock>
  execution_outcome: ExecutionOutcome<Receipt>  // receipts contain logs
  ```

  ### Output (to WAVS trigger system)
  ```rust
  StreamTriggers::Evm {
  chain: ChainKey,
  log: Box<alloy_rpc_types_eth::Log>,
  block_number: u64,
  tx_hash, block_hash, tx_index, block_timestamp, log_index,
  }
  StreamTriggers::EvmBlock { chain: ChainKey, block_height: u64 }
  ```

  ### Conversion Logic
  1. **ChainCommitted**: Process `new` chain, emit triggers
  2. **ChainReorged**: Skip `old` chain (reverted blocks filtered out), process only `new` chain
  3. **ChainReverted**: Skip entirely (no new events to emit)

  > **Design decision**: Removed logs are filtered out (not emitted), matching current WAVS eth_subscribe behavior. This
  simplifies workflow logic since handlers don't need to check for reverted events.

  ## Implementation

  ### New Files

  | File | Purpose |
  |------|---------|
  | `lib/WAVS/packages/wavs/proto/exex.proto` | gRPC service definition (copy from reth) |
  | `lib/WAVS/packages/wavs/build.rs` | tonic-build proto compilation |
  | `lib/WAVS/packages/wavs/src/subsystems/trigger/streams/exex_stream.rs` | Main module - follows `atproto_jetstream.rs`
  pattern |

  ### Modified Files

  | File | Changes |
  |------|---------|
  | `lib/WAVS/packages/types/src/chain_config.rs` | Add `exex_endpoint: Option<String>` to `EvmChainConfig` |
  | `lib/WAVS/packages/wavs/src/subsystems/trigger/streams.rs` | Add `pub mod exex_stream;` |
  | `lib/WAVS/packages/wavs/src/subsystems/trigger.rs` | Add `StartListeningExEx` command, integrate ExEx stream |
  | `lib/WAVS/packages/wavs/Cargo.toml` | Add tonic, prost, bincode, reth-exex-types dependencies |

  ### Key Implementation Details

  **exex_stream.rs** (following atproto_jetstream.rs pattern ~250-350 lines):

  ```rust
  // Config
  pub struct ExExConfig {
  pub endpoint: String,  // e.g., "http://[::1]:10000"
  pub chain: ChainKey,
  }

  // Main stream function
  pub async fn start_exex_stream(
  config: ExExConfig,
  metrics: TriggerMetrics,
  ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamTriggers, TriggerError>> + Send>>, TriggerError>

  // Connection with reconnect loop (same pattern as jetstream)
  async fn create_exex_connection(config: &ExExConfig)
  -> Result<tonic::Streaming<proto::ExExNotification>, TriggerError>

  // Converter
  fn convert_notification(
  notification: ExExNotification,
  chain: &ChainKey,
  ) -> Vec<StreamTriggers>
  ```

  **Reconnection strategy** (same as atproto_jetstream.rs):
  - Base delay: 1 second
  - Max delay: 60 seconds
  - Max reconnects: 10
  - Exponential backoff with jitter

  ### Dependencies to Add

  ```toml
  [dependencies]
  tonic = "0.12"
  prost = "0.13"
  bincode = "1.3"
  reth-exex-types = { version = "1.0", features = ["serde-bincode-compat"] }
  reth-execution-types = { version = "1.0", features = ["serde-bincode-compat"] }

  [build-dependencies]
  tonic-build = "0.12"
  ```

  ## Configuration Example

  ```toml
  [[chains]]
  type = "evm"
  chain_id = "local-reth"
  exex_endpoint = "http://[::1]:10000"  # NEW - use ExEx instead of WS
  # ws_endpoints still available as fallback
  ws_endpoints = ["ws://127.0.0.1:8546"]
  http_endpoint = "http://127.0.0.1:8545"
  ```

  ## Verification

  1. **Unit tests**: Parse mock ExExNotification, verify StreamTriggers output
  2. **Integration test**:
  - Start reth node with remote ExEx enabled
  - Start WAVS with exex_endpoint configured
  - Deploy test contract, emit events
  - Verify WAVS receives triggers
  3. **Reorg test**: Simulate reorg, verify correct removed/new event ordering

  ## Critical Files Reference

  - `lib/WAVS/packages/wavs/src/subsystems/trigger/streams/atproto_jetstream.rs` - Pattern to follow
  - `lib/WAVS/packages/wavs/src/subsystems/trigger.rs` - TriggerManager integration point
  - `docs/vocs/docs/snippets/sources/exex/remote/src/consumer.rs` - Reth ExEx client example
  - `crates/exex/types/src/notification.rs` - ExExNotification definition
