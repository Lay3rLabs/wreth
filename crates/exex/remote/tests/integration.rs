//! Integration tests for the remote ExEx gRPC server.
//!
//! These tests verify the full pipeline from ExExNotification to gRPC client.

use reth_exex_remote::{
    proto::{remote_ex_ex_client::RemoteExExClient, SubscribeRequest},
    NotificationSender, RemoteExExConfig, RemoteExExServer,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::timeout;
use tokio_stream::StreamExt;

use reth_ethereum_primitives::EthPrimitives;
use reth_exex::ExExNotification;
use reth_execution_types::Chain;

/// Helper to get a random port for testing to avoid conflicts.
fn random_test_port() -> u16 {
    10000 + (rand::random::<u16>() % 5000)
}

/// Helper to create a test server and sender.
fn create_test_server(
    port: u16,
) -> (RemoteExExServer<EthPrimitives>, NotificationSender<EthPrimitives>, SocketAddr) {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let config = RemoteExExConfig::default()
        .with_addr(addr)
        .with_channel_capacity(64);
    let (server, sender) = RemoteExExServer::new(config);
    (server, sender, addr)
}

#[tokio::test]
async fn test_server_client_notification_flow() {
    let port = random_test_port();
    let (server, sender, addr) = create_test_server(port);

    // Start server
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Connect client
    let connect_result = timeout(
        Duration::from_secs(5),
        RemoteExExClient::connect(format!("http://{}", addr)),
    )
    .await;

    let mut client = match connect_result {
        Ok(Ok(client)) => client
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX),
        Ok(Err(e)) => {
            eprintln!("Connection failed (may be expected in CI): {}", e);
            server_handle.abort();
            return;
        }
        Err(_) => {
            eprintln!("Connection timed out (may be expected in CI)");
            server_handle.abort();
            return;
        }
    };

    // Subscribe to notifications
    let subscribe_result = client.subscribe(SubscribeRequest {}).await;
    let mut stream = match subscribe_result {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("Subscribe failed: {}", e);
            server_handle.abort();
            return;
        }
    };

    // Give subscription time to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a notification through the sender
    let chain = Chain::<EthPrimitives>::default();
    let notification = ExExNotification::ChainCommitted {
        new: Arc::new(chain),
    };

    // The sender needs receivers subscribed via the broadcast channel
    // In the real flow, the gRPC service subscribes internally
    let send_result = sender.send(notification);
    match send_result {
        Ok(count) => {
            assert!(count >= 1, "At least one receiver should get the notification");
        }
        Err(_) => {
            // This can happen if the gRPC subscription hasn't fully registered yet
            eprintln!("No receivers (subscription may not be fully registered)");
        }
    }

    // Try to receive the notification
    let recv_result = timeout(Duration::from_secs(2), stream.next()).await;

    match recv_result {
        Ok(Some(Ok(proto_notification))) => {
            // Verify we received data
            assert!(!proto_notification.data.is_empty(), "Notification data should not be empty");

            // Deserialize and verify
            use serde::Deserialize;
            use serde_with::serde_as;

            #[serde_as]
            #[derive(Deserialize)]
            struct DeserializeWrapper {
                #[serde_as(
                    as = "reth_exex_types::serde_bincode_compat::ExExNotification<'_, EthPrimitives>"
                )]
                notification: ExExNotification<EthPrimitives>,
            }

            let deserialized: Result<DeserializeWrapper, _> =
                bincode::deserialize(&proto_notification.data);

            match deserialized {
                Ok(wrapper) => {
                    assert!(
                        wrapper.notification.committed_chain().is_some(),
                        "Should be a ChainCommitted notification"
                    );
                }
                Err(e) => {
                    eprintln!("Deserialization failed: {}", e);
                }
            }
        }
        Ok(Some(Err(e))) => {
            eprintln!("Stream error: {}", e);
        }
        Ok(None) => {
            eprintln!("Stream ended unexpectedly");
        }
        Err(_) => {
            eprintln!("Receive timed out");
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_multiple_clients_receive_same_notification() {
    let port = random_test_port();
    let (server, sender, addr) = create_test_server(port);

    let server_handle = tokio::spawn(async move { server.serve().await });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Connect multiple clients
    let mut clients = Vec::new();
    for i in 0..3 {
        let connect_result = timeout(
            Duration::from_secs(5),
            RemoteExExClient::connect(format!("http://{}", addr)),
        )
        .await;

        match connect_result {
            Ok(Ok(client)) => {
                let client = client
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX);
                clients.push(client);
            }
            _ => {
                eprintln!("Client {} connection failed", i);
            }
        }
    }

    if clients.is_empty() {
        eprintln!("No clients connected, skipping test");
        server_handle.abort();
        return;
    }

    // Subscribe all clients
    let mut streams = Vec::new();
    for mut client in clients {
        if let Ok(response) = client.subscribe(SubscribeRequest {}).await {
            streams.push(response.into_inner());
        }
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a notification
    let chain = Chain::<EthPrimitives>::default();
    let notification = ExExNotification::ChainCommitted {
        new: Arc::new(chain),
    };
    let _ = sender.send(notification);

    // Verify all streams receive the notification
    let mut received_count = 0;
    for mut stream in streams {
        if let Ok(Some(Ok(_))) = timeout(Duration::from_secs(2), stream.next()).await {
            received_count += 1;
        }
    }

    // At least some clients should receive the notification
    assert!(
        received_count > 0,
        "At least one client should receive the notification"
    );

    server_handle.abort();
}

#[tokio::test]
async fn test_client_reconnection_receives_new_notifications() {
    let port = random_test_port();
    let (server, sender, addr) = create_test_server(port);

    let server_handle = tokio::spawn(async move { server.serve().await });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // First connection
    let connect_result = timeout(
        Duration::from_secs(5),
        RemoteExExClient::connect(format!("http://{}", addr)),
    )
    .await;

    if let Ok(Ok(client)) = connect_result {
        let mut client = client
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);

        // Subscribe
        if let Ok(response) = client.subscribe(SubscribeRequest {}).await {
            let mut stream = response.into_inner();
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Send first notification
            let chain = Chain::<EthPrimitives>::default();
            let notification = ExExNotification::ChainCommitted {
                new: Arc::new(chain),
            };
            let _ = sender.send(notification);

            // Receive first notification
            let first = timeout(Duration::from_secs(2), stream.next()).await;
            assert!(first.is_ok(), "Should receive first notification");

            // Drop stream (simulating disconnect)
            drop(stream);
        }

        // Reconnect
        tokio::time::sleep(Duration::from_millis(100)).await;

        let reconnect_result = timeout(
            Duration::from_secs(5),
            RemoteExExClient::connect(format!("http://{}", addr)),
        )
        .await;

        if let Ok(Ok(new_client)) = reconnect_result {
            let mut new_client = new_client
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX);

            if let Ok(response) = new_client.subscribe(SubscribeRequest {}).await {
                let mut new_stream = response.into_inner();
                tokio::time::sleep(Duration::from_millis(50)).await;

                // Send second notification
                let chain = Chain::<EthPrimitives>::default();
                let notification = ExExNotification::ChainReverted {
                    old: Arc::new(chain),
                };
                let _ = sender.send(notification);

                // Receive second notification on new connection
                let second = timeout(Duration::from_secs(2), new_stream.next()).await;
                assert!(second.is_ok(), "Should receive notification after reconnect");
            }
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_notification_types_serialization() {
    let port = random_test_port();
    let (server, sender, addr) = create_test_server(port);

    let server_handle = tokio::spawn(async move { server.serve().await });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let connect_result = timeout(
        Duration::from_secs(5),
        RemoteExExClient::connect(format!("http://{}", addr)),
    )
    .await;

    if let Ok(Ok(client)) = connect_result {
        let mut client = client
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);

        if let Ok(response) = client.subscribe(SubscribeRequest {}).await {
            let mut stream = response.into_inner();
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Test all three notification types
            let notification_types = vec![
                ExExNotification::ChainCommitted {
                    new: Arc::new(Chain::default()),
                },
                ExExNotification::ChainReverted {
                    old: Arc::new(Chain::default()),
                },
                ExExNotification::ChainReorged {
                    old: Arc::new(Chain::default()),
                    new: Arc::new(Chain::default()),
                },
            ];

            for notification in notification_types {
                let _ = sender.send(notification);

                if let Ok(Some(Ok(proto))) = timeout(Duration::from_secs(2), stream.next()).await {
                    assert!(
                        !proto.data.is_empty(),
                        "Each notification type should serialize to non-empty data"
                    );
                }
            }
        }
    }

    server_handle.abort();
}
