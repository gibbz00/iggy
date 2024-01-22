use crate::test_server::MockClient;
use iggy::http::client::HttpClient;
use iggy::http::config::HttpClientConfig;

impl MockClient for HttpClient {
    async fn mock(server_address: &str) -> Self {
        HttpClient::create(&HttpClientConfig {
            api_url: format!("http://{}", server_address),
            ..HttpClientConfig::default()
        })
        .unwrap()
    }
}
