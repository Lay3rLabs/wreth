//! WRETH: Reth node with Remote ExEx gRPC server for WAVS integration.
//!
//! This binary runs a reth Ethereum node with a Remote ExEx that streams
//! execution notifications via gRPC to external consumers like WAVS.

#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

use clap::Parser;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, Cli};
use reth_exex_remote::{remote_exex, RemoteExExConfig, RemoteExExServer};
use reth_node_builder::NodeHandle;
use reth_node_ethereum::EthereumNode;
use reth_tracing::tracing::info;
use std::net::SocketAddr;

/// Extra CLI arguments for WRETH.
#[derive(Debug, Clone, Parser)]
pub struct WrethArgs {
    /// Address for the ExEx gRPC server to bind to.
    #[arg(long = "exex.addr", default_value = "[::]:10000")]
    pub exex_addr: SocketAddr,

    /// Channel capacity for the ExEx notification broadcast.
    #[arg(long = "exex.channel-capacity", default_value = "256")]
    pub exex_channel_capacity: usize,
}

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) =
        Cli::<EthereumChainSpecParser, WrethArgs>::parse().run(async move |builder, wreth_args| {
            info!(target: "wreth::cli", "Launching WRETH node with ExEx gRPC server");

            // Configure the Remote ExEx server
            let config = RemoteExExConfig::default()
                .with_addr(wreth_args.exex_addr)
                .with_channel_capacity(wreth_args.exex_channel_capacity);

            let (server, notifications) = RemoteExExServer::new(config);

            info!(
                target: "wreth::cli",
                exex_addr = %wreth_args.exex_addr,
                "Remote ExEx gRPC server configured"
            );

            // Build and launch the node with the Remote ExEx installed
            let NodeHandle { node, node_exit_future } = builder
                .node(EthereumNode::default())
                .install_exex(
                    "remote-exex",
                    |ctx| async move { Ok(remote_exex(ctx, notifications)) },
                )
                .launch_with_debug_capabilities()
                .await?;

            // Start the gRPC server as a critical task
            node.task_executor.spawn_critical("ExEx gRPC server", async move {
                if let Err(err) = server.serve().await {
                    panic!("ExEx gRPC server failed: {err}");
                }
            });

            info!(target: "wreth::cli", "WRETH node started successfully");

            node_exit_future.await
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
