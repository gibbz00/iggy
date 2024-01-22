use crate::test_server::MockClient;
use iggy::client::Client;
use iggy::quic::client::QuicClient;
use iggy::quic::config::QuicClientConfig;

impl MockClient for QuicClient {
    async fn mock(server_address: &str) -> Self {
        let config = QuicClientConfig {
            server_address: server_address.to_string(),
            ..QuicClientConfig::default()
        };
        let client = QuicClient::create(config).unwrap();
        client.connect().await.unwrap();
        client
    }
}
