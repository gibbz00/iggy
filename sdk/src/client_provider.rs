use crate::args::Args;
use crate::clients::client::IggyClientConfig;
use crate::http::config::HttpClientConfig;
use crate::quic::config::QuicClientConfig;
use crate::tcp::config::TcpClientConfig;

// TODO: move
pub trait ClientConfig {
    fn from_args(args: Args) -> Self;
}

// TODO: move
impl ClientConfig for HttpClientConfig {
    fn from_args(args: Args) -> Self {
        Self {
            api_url: args.http_api_url,
            retries: args.http_retries,
        }
    }
}

impl ClientConfig for QuicClientConfig {
    fn from_args(args: Args) -> Self {
        Self {
            client_address: args.quic_client_address,
            server_address: args.quic_server_address,
            server_name: args.quic_server_name,
            reconnection_retries: args.quic_reconnection_retries,
            reconnection_interval: args.quic_reconnection_interval,
            response_buffer_size: args.quic_response_buffer_size,
            max_concurrent_bidi_streams: args.quic_max_concurrent_bidi_streams,
            datagram_send_buffer_size: args.quic_datagram_send_buffer_size,
            initial_mtu: args.quic_initial_mtu,
            send_window: args.quic_send_window,
            receive_window: args.quic_receive_window,
            keep_alive_interval: args.quic_keep_alive_interval,
            max_idle_timeout: args.quic_max_idle_timeout,
            validate_certificate: args.quic_validate_certificate,
        }
    }
}

impl ClientConfig for TcpClientConfig {
    fn from_args(args: Args) -> Self {
        Self {
            server_address: args.tcp_server_address,
            reconnection_retries: args.tcp_reconnection_retries,
            reconnection_interval: args.tcp_reconnection_interval,
            tls_enabled: args.tcp_tls_enabled,
            tls_domain: args.tcp_tls_domain,
        }
    }
}

// TEMP: stub, don't think IggyClient should implement Client
impl ClientConfig for IggyClientConfig {
    fn from_args(args: Args) -> Self {
        todo!()
    }
}
