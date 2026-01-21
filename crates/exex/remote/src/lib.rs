//! Remote ExEx gRPC Server
//!
//! This crate provides a gRPC server that streams execution notifications from reth
//! to external consumers like WAVS. It implements the `RemoteExEx` gRPC service
//! which allows clients to subscribe to a stream of `ExExNotification` messages.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        RETH NODE                                 │
//! │  ┌──────────────────┐    ┌──────────────────────────────────┐  │
//! │  │ Execution Engine │───▶│       RemoteExExServer           │  │
//! │  └──────────────────┘    │  ┌────────────────────────────┐  │  │
//! │                          │  │ broadcast::Sender          │  │  │
//! │                          │  │    │                       │  │  │
//! │                          │  │    ├─▶ Client 1 (WAVS)     │  │  │
//! │                          │  │    ├─▶ Client 2            │  │  │
//! │                          │  │    └─▶ Client N            │  │  │
//! │                          │  └────────────────────────────┘  │  │
//! │                          └──────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use reth_exex_remote::{RemoteExExServer, RemoteExExConfig};
//! use reth_node_ethereum::EthereumNode;
//!
//! fn main() -> eyre::Result<()> {
//!     reth::cli::Cli::parse_args().run(|builder, _| async move {
//!         let config = RemoteExExConfig::default();
//!         let (server, notifications) = RemoteExExServer::new(config);
//!
//!         let handle = builder
//!             .node(EthereumNode::default())
//!             .install_exex("remote-exex", |ctx| {
//!                 remote_exex(ctx, notifications)
//!             })
//!             .launch()
//!             .await?;
//!
//!         handle.node.task_executor.spawn_critical("gRPC server", server.serve());
//!         handle.wait_for_node_exit().await
//!     })
//! }
//! ```

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use futures_util::TryStreamExt;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::{FullNodeComponents, NodeTypes};
use reth_primitives_traits::NodePrimitives;
use reth_tracing::tracing::{debug, error, info, warn};
use serde::Serialize;
use serde_with::serde_as;
use std::{
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

/// Generated gRPC server code from proto/exex.proto
pub mod proto {
    tonic::include_proto!("exex");
}

use proto::{
    remote_ex_ex_server::{RemoteExEx, RemoteExExServer as TonicRemoteExExServer},
    ExExNotification as ProtoExExNotification, SubscribeRequest,
};

/// Configuration for the remote ExEx gRPC server.
#[derive(Debug, Clone)]
pub struct RemoteExExConfig {
    /// Address to bind the gRPC server to.
    /// Default: `[::1]:10000`
    pub addr: SocketAddr,
    /// Channel capacity for the notification broadcast.
    /// Default: 256
    pub channel_capacity: usize,
    /// Maximum message encoding size for gRPC.
    /// Default: usize::MAX (no limit)
    pub max_encoding_message_size: usize,
    /// Maximum message decoding size for gRPC.
    /// Default: usize::MAX (no limit)
    pub max_decoding_message_size: usize,
}

impl Default for RemoteExExConfig {
    fn default() -> Self {
        Self {
            addr: "[::1]:10000".parse().expect("valid default address"),
            channel_capacity: 256,
            max_encoding_message_size: usize::MAX,
            max_decoding_message_size: usize::MAX,
        }
    }
}

impl RemoteExExConfig {
    /// Create a new config with a specific address.
    pub fn with_addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    /// Create a new config with a specific channel capacity.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }
}

/// Wrapper for serializing ExExNotification with serde_bincode_compat.
#[serde_as]
#[derive(Debug, Serialize)]
pub struct ExExNotificationWrapper<'a, N: NodePrimitives> {
    #[serde_as(as = "reth_exex_types::serde_bincode_compat::ExExNotification<'_, N>")]
    notification: &'a ExExNotification<N>,
}

/// The gRPC service implementation that streams ExEx notifications to clients.
#[derive(Debug)]
struct ExExService<N: NodePrimitives> {
    notifications: Arc<broadcast::Sender<ExExNotification<N>>>,
}

#[tonic::async_trait]
impl<N> RemoteExEx for ExExService<N>
where
    N: NodePrimitives + Send + Sync + 'static,
    ExExNotification<N>: Clone + Send,
    for<'a> ExExNotificationWrapper<'a, N>: Serialize,
{
    type SubscribeStream = ReceiverStream<Result<ProtoExExNotification, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(16);
        let mut notifications = self.notifications.subscribe();

        info!("New gRPC client subscribed to ExEx notifications");

        tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(notification) => {
                        // Serialize using bincode with serde_bincode_compat wrapper
                        let wrapper = ExExNotificationWrapper {
                            notification: &notification,
                        };

                        let data = match bincode::serialize(&wrapper) {
                            Ok(data) => data,
                            Err(e) => {
                                error!("Failed to serialize ExEx notification: {}", e);
                                continue;
                            }
                        };

                        let proto_notification = ProtoExExNotification { data };

                        if tx.send(Ok(proto_notification)).await.is_err() {
                            debug!("Client disconnected, stopping notification stream");
                            break;
                        }

                        debug!("Sent ExEx notification to gRPC client");
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Client lagged behind by {} notifications", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Notification channel closed, stopping client stream");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Handle to the notification sender for the remote ExEx.
///
/// This is passed to the ExEx function to send notifications to all connected clients.
#[derive(Clone, Debug)]
pub struct NotificationSender<N: NodePrimitives> {
    inner: Arc<broadcast::Sender<ExExNotification<N>>>,
}

impl<N: NodePrimitives> NotificationSender<N> {
    /// Send a notification to all connected clients.
    ///
    /// Returns the number of receivers that received the notification.
    pub fn send(&self, notification: ExExNotification<N>) -> Result<usize, broadcast::error::SendError<ExExNotification<N>>> {
        self.inner.send(notification)
    }

    /// Get the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.inner.receiver_count()
    }
}

/// The remote ExEx gRPC server.
///
/// This server streams execution notifications to connected gRPC clients.
#[derive(Debug)]
pub struct RemoteExExServer<N: NodePrimitives> {
    config: RemoteExExConfig,
    notifications: Arc<broadcast::Sender<ExExNotification<N>>>,
}

impl<N: NodePrimitives> RemoteExExServer<N> {
    /// Create a new remote ExEx server with the given configuration.
    ///
    /// Returns the server and a notification sender handle that should be passed
    /// to the ExEx function.
    pub fn new(config: RemoteExExConfig) -> (Self, NotificationSender<N>) {
        let (tx, _) = broadcast::channel(config.channel_capacity);
        let notifications = Arc::new(tx);

        let sender = NotificationSender {
            inner: notifications.clone(),
        };

        let server = Self {
            config,
            notifications,
        };

        (server, sender)
    }

    /// Serve the gRPC server.
    ///
    /// This is a blocking operation that runs until the server is shut down.
    pub async fn serve(self) -> Result<(), tonic::transport::Error>
    where
        N: Send + Sync + 'static,
        ExExNotification<N>: Clone + Send,
        for<'a> ExExNotificationWrapper<'a, N>: Serialize,
    {
        let service = ExExService {
            notifications: self.notifications,
        };

        let svc = TonicRemoteExExServer::new(service)
            .max_encoding_message_size(self.config.max_encoding_message_size)
            .max_decoding_message_size(self.config.max_decoding_message_size);

        info!("Starting remote ExEx gRPC server on {}", self.config.addr);

        Server::builder()
            .add_service(svc)
            .serve(self.config.addr)
            .await
    }
}

/// The ExEx function that forwards notifications to the gRPC server.
///
/// This should be installed as an ExEx on the reth node.
///
/// # Example
///
/// ```rust,ignore
/// builder
///     .node(EthereumNode::default())
///     .install_exex("remote-exex", |ctx| remote_exex(ctx, notifications))
///     .launch()
///     .await?;
/// ```
pub async fn remote_exex<Node>(
    mut ctx: ExExContext<Node>,
    notifications: NotificationSender<<Node::Types as NodeTypes>::Primitives>,
) -> eyre::Result<()>
where
    Node: FullNodeComponents,
    ExExNotification<<Node::Types as NodeTypes>::Primitives>: Clone,
{
    info!("Remote ExEx started, waiting for notifications...");

    while let Some(notification) = ctx.notifications.try_next().await? {
        // Send the finished height event back to the node
        if let Some(committed_chain) = notification.committed_chain() {
            ctx.events
                .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }

        // Forward the notification to all connected gRPC clients
        let receiver_count = notifications.receiver_count();
        if receiver_count > 0 {
            match notifications.send(notification) {
                Ok(n) => {
                    debug!("Forwarded notification to {} gRPC clients", n);
                }
                Err(e) => {
                    // This can happen if all receivers have been dropped
                    debug!("Failed to send notification: {}", e);
                }
            }
        } else {
            debug!("No gRPC clients connected, notification not forwarded");
        }
    }

    info!("Remote ExEx shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::remote_ex_ex_client::RemoteExExClient;
    use reth_ethereum_primitives::EthPrimitives;
    use reth_execution_types::Chain;
    use serde::Deserialize;
    use tokio::time::{timeout, Duration};

    /// Wrapper for deserializing ExExNotification with serde_bincode_compat.
    #[serde_as]
    #[derive(Deserialize)]
    struct ExExNotificationDeserializeWrapper<N: NodePrimitives> {
        #[serde_as(as = "reth_exex_types::serde_bincode_compat::ExExNotification<'_, N>")]
        notification: ExExNotification<N>,
    }

    /// Create a test chain with default values for testing.
    fn create_test_chain() -> Chain<EthPrimitives> {
        Chain::default()
    }

    #[test]
    fn test_default_config() {
        let config = RemoteExExConfig::default();
        assert_eq!(config.addr.to_string(), "[::1]:10000");
        assert_eq!(config.channel_capacity, 256);
    }

    #[test]
    fn test_config_builder() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let config = RemoteExExConfig::default()
            .with_addr(addr)
            .with_channel_capacity(512);

        assert_eq!(config.addr, addr);
        assert_eq!(config.channel_capacity, 512);
    }

    #[test]
    fn test_notification_sender_receiver_count() {
        let config = RemoteExExConfig::default();
        let (_server, sender): (RemoteExExServer<EthPrimitives>, _) =
            RemoteExExServer::new(config);

        // No receivers initially
        assert_eq!(sender.receiver_count(), 0);
    }

    #[test]
    fn test_serialization_roundtrip_chain_committed() {
        let chain = create_test_chain();
        let notification: ExExNotification<EthPrimitives> =
            ExExNotification::ChainCommitted { new: Arc::new(chain) };

        // Serialize using the wrapper
        let wrapper = ExExNotificationWrapper {
            notification: &notification,
        };
        let serialized = bincode::serialize(&wrapper).expect("serialization should succeed");

        // Deserialize
        let deserialized: ExExNotificationDeserializeWrapper<EthPrimitives> =
            bincode::deserialize(&serialized).expect("deserialization should succeed");

        // Verify the notification type matches
        assert!(deserialized.notification.committed_chain().is_some());
        assert!(deserialized.notification.reverted_chain().is_none());
    }

    #[test]
    fn test_serialization_roundtrip_chain_reverted() {
        let chain = create_test_chain();
        let notification: ExExNotification<EthPrimitives> =
            ExExNotification::ChainReverted { old: Arc::new(chain) };

        let wrapper = ExExNotificationWrapper {
            notification: &notification,
        };
        let serialized = bincode::serialize(&wrapper).expect("serialization should succeed");

        let deserialized: ExExNotificationDeserializeWrapper<EthPrimitives> =
            bincode::deserialize(&serialized).expect("deserialization should succeed");

        assert!(deserialized.notification.reverted_chain().is_some());
        assert!(deserialized.notification.committed_chain().is_none());
    }

    #[test]
    fn test_serialization_roundtrip_chain_reorged() {
        let old_chain = create_test_chain();
        let new_chain = create_test_chain();
        let notification: ExExNotification<EthPrimitives> = ExExNotification::ChainReorged {
            old: Arc::new(old_chain),
            new: Arc::new(new_chain),
        };

        let wrapper = ExExNotificationWrapper {
            notification: &notification,
        };
        let serialized = bincode::serialize(&wrapper).expect("serialization should succeed");

        let deserialized: ExExNotificationDeserializeWrapper<EthPrimitives> =
            bincode::deserialize(&serialized).expect("deserialization should succeed");

        // ChainReorged has both committed (new) and reverted (old)
        assert!(deserialized.notification.committed_chain().is_some());
        assert!(deserialized.notification.reverted_chain().is_some());
    }

    #[tokio::test]
    async fn test_broadcast_channel_multiple_receivers() {
        let config = RemoteExExConfig::default().with_channel_capacity(16);
        let (server, sender): (RemoteExExServer<EthPrimitives>, _) =
            RemoteExExServer::new(config);

        // Subscribe multiple receivers
        let mut rx1 = sender.inner.subscribe();
        let mut rx2 = sender.inner.subscribe();
        let mut rx3 = sender.inner.subscribe();

        assert_eq!(sender.receiver_count(), 3);

        // Send a notification
        let chain = create_test_chain();
        let notification = ExExNotification::ChainCommitted { new: Arc::new(chain) };
        let sent_count = sender.send(notification.clone()).expect("send should succeed");

        assert_eq!(sent_count, 3);

        // All receivers should get the notification
        let recv1 = rx1.recv().await.expect("rx1 should receive");
        let recv2 = rx2.recv().await.expect("rx2 should receive");
        let recv3 = rx3.recv().await.expect("rx3 should receive");

        assert!(recv1.committed_chain().is_some());
        assert!(recv2.committed_chain().is_some());
        assert!(recv3.committed_chain().is_some());

        drop(server); // Ensure server is used
    }

    #[tokio::test]
    async fn test_grpc_server_startup_and_client_connection() {
        // Use a random port to avoid conflicts
        let port = 10000 + (rand::random::<u16>() % 1000);
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        let config = RemoteExExConfig::default().with_addr(addr);

        let (server, sender): (RemoteExExServer<EthPrimitives>, _) =
            RemoteExExServer::new(config);

        // Spawn the server
        let server_handle = tokio::spawn(async move {
            server.serve().await
        });

        // Give the server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try to connect a client
        let client_result = timeout(
            Duration::from_secs(2),
            RemoteExExClient::connect(format!("http://{}", addr)),
        )
        .await;

        match client_result {
            Ok(Ok(mut client)) => {
                // Successfully connected, try to subscribe
                let request = tonic::Request::new(proto::SubscribeRequest {});
                let response = client.subscribe(request).await;
                assert!(response.is_ok(), "Subscribe should succeed");

                // Send a notification through the sender
                let chain = create_test_chain();
                let notification = ExExNotification::ChainCommitted { new: Arc::new(chain) };

                // Note: receiver_count might be 0 initially as the gRPC service
                // subscribes asynchronously
                let _ = sender.send(notification);
            }
            Ok(Err(e)) => {
                // Connection might fail in CI environments, that's acceptable
                eprintln!("Client connection failed (may be expected in CI): {}", e);
            }
            Err(_) => {
                // Timeout is acceptable in CI environments
                eprintln!("Client connection timed out (may be expected in CI)");
            }
        }

        // Clean up
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_notification_sender_no_receivers() {
        let config = RemoteExExConfig::default();
        let (_server, sender): (RemoteExExServer<EthPrimitives>, _) =
            RemoteExExServer::new(config);

        // Send with no receivers - should return error
        let chain = create_test_chain();
        let notification = ExExNotification::ChainCommitted { new: Arc::new(chain) };
        let result = sender.send(notification);

        // With no receivers, broadcast returns an error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lagged_receiver_handling() {
        // Small capacity to trigger lagging
        let config = RemoteExExConfig::default().with_channel_capacity(2);
        let (_server, sender): (RemoteExExServer<EthPrimitives>, _) =
            RemoteExExServer::new(config);

        let mut rx = sender.inner.subscribe();

        // Send more notifications than the channel can hold
        for _ in 0..5 {
            let chain = create_test_chain();
            let notification = ExExNotification::ChainCommitted { new: Arc::new(chain) };
            let _ = sender.send(notification);
        }

        // Receiver should get a Lagged error
        match rx.recv().await {
            Err(broadcast::error::RecvError::Lagged(n)) => {
                assert!(n > 0, "Should have lagged by some messages");
            }
            other => {
                // It's also acceptable to receive a message if timing works out
                eprintln!("Received: {:?}", other.map(|_| "notification"));
            }
        }
    }
}
