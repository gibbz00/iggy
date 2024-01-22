use crate::test_server::MockClient;
use iggy::client::Client;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;

impl MockClient for TcpClient {
    async fn mock(server_address: &str) -> Self {
        let config = TcpClientConfig {
            server_address: server_address.to_string(),
            ..TcpClientConfig::default()
        };
        let client = TcpClient::create(config).unwrap();
        client.connect().await.unwrap();
        client
    }
}
