use crate::binary::binary_client::{BinaryClient, ClientState};
use crate::client::Client;
use crate::error::IggyError;
use crate::tcp::config::TcpClientConfig;

use bytes::BufMut;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_native_tls::native_tls::TlsConnector;
use tokio_native_tls::TlsStream;
use tracing::log::trace;
use tracing::{error, info};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const EMPTY_RESPONSE: Vec<u8> = vec![];
const NAME: &str = "Iggy";

/// TCP client for interacting with the Iggy API.
/// It requires a valid server address.
#[derive(Debug)]
pub struct TcpClient {
    pub(crate) server_address: SocketAddr,
    pub(crate) stream: Mutex<Option<Box<dyn ConnectionStream>>>,
    pub(crate) config: TcpClientConfig,
    pub(crate) state: Mutex<ClientState>,
}

unsafe impl Send for TcpClient {}
unsafe impl Sync for TcpClient {}

#[async_trait::async_trait]
pub(crate) trait ConnectionStream: Debug + Sync + Send {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError>;
    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError>;
}

#[derive(Debug)]
struct TcpConnectionStream {
    stream: TcpStream,
}

#[derive(Debug)]
struct TcpTlsConnectionStream {
    stream: TlsStream<TcpStream>,
}

unsafe impl Send for TcpConnectionStream {}
unsafe impl Sync for TcpConnectionStream {}

unsafe impl Send for TcpTlsConnectionStream {}
unsafe impl Sync for TcpTlsConnectionStream {}

#[async_trait::async_trait]
impl ConnectionStream for TcpConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        let result = self.stream.read_exact(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(result.unwrap())
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        let result = self.stream.write_all(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectionStream for TcpTlsConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        let result = self.stream.read_exact(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(result.unwrap())
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        let result = self.stream.write_all(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(())
    }
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient::create(TcpClientConfig::default()).unwrap()
    }
}

impl Client for TcpClient {
    type Config = TcpClientConfig;

    fn from_config(config: Self::Config) -> Result<Self, IggyError> {
        Self::create(config)
    }

    async fn connect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Connected {
            return Ok(());
        }

        let tls_enabled = self.config.tls_enabled;
        let mut retry_count = 0;
        let connection_stream: Box<dyn ConnectionStream>;
        let remote_address;
        loop {
            info!(
                "{} client is connecting to server: {}...",
                NAME, self.config.server_address
            );

            let connection = TcpStream::connect(self.server_address).await;
            if connection.is_err() {
                error!(
                    "Failed to connect to server: {}",
                    self.config.server_address
                );
                if retry_count < self.config.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying to connect to server ({}/{}): {} in: {} ms...",
                        retry_count,
                        self.config.reconnection_retries,
                        self.config.server_address,
                        self.config.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.config.reconnection_interval)).await;
                    continue;
                }

                return Err(IggyError::NotConnected);
            }

            let stream = connection.unwrap();
            remote_address = stream.peer_addr()?;

            if !tls_enabled {
                connection_stream = Box::new(TcpConnectionStream { stream });
                break;
            }

            let connector =
                tokio_native_tls::TlsConnector::from(TlsConnector::builder().build().unwrap());
            let stream = tokio_native_tls::TlsConnector::connect(
                &connector,
                &self.config.tls_domain,
                stream,
            )
            .await
            .unwrap();
            connection_stream = Box::new(TcpTlsConnectionStream { stream });
            break;
        }

        self.stream.lock().await.replace(connection_stream);
        self.set_state(ClientState::Connected).await;

        info!(
            "{} client has connected to server: {}",
            NAME, remote_address
        );

        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        info!("{} client is disconnecting from server...", NAME);
        self.set_state(ClientState::Disconnected).await;
        self.stream.lock().await.take();
        info!("{} client has disconnected from server.", NAME);
        Ok(())
    }
}

impl BinaryClient for TcpClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn send_with_response(&self, command: u32, payload: &[u8]) -> Result<Vec<u8>, IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Err(IggyError::NotConnected);
        }

        let mut stream = self.stream.lock().await;
        if let Some(stream) = stream.as_mut() {
            let payload_length = payload.len() + 4;
            let mut buffer = Vec::with_capacity(REQUEST_INITIAL_BYTES_LENGTH + payload_length);
            #[allow(clippy::cast_possible_truncation)]
            buffer.put_u32_le(payload_length as u32);
            buffer.put_u32_le(command);
            buffer.extend(payload);

            trace!("Sending a TCP request...");
            stream.write(&buffer).await?;
            trace!("Sent a TCP request, waiting for a response...");

            let mut response_buffer = [0u8; RESPONSE_INITIAL_BYTES_LENGTH];
            let read_bytes = stream.read(&mut response_buffer).await?;
            if read_bytes != RESPONSE_INITIAL_BYTES_LENGTH {
                error!("Received an invalid or empty response.");
                return Err(IggyError::EmptyResponse);
            }

            let status = u32::from_le_bytes(response_buffer[..4].try_into().unwrap());
            let length = u32::from_le_bytes(response_buffer[4..].try_into().unwrap());
            return self.handle_response(status, length, stream.as_mut()).await;
        }

        error!("Cannot send data. Client is not connected.");
        Err(IggyError::NotConnected)
    }
}

impl TcpClient {
    /// Create a new TCP client for the provided server address.
    pub fn new(server_address: &str) -> Result<Self, IggyError> {
        Self::create(TcpClientConfig {
            server_address: server_address.to_string(),
            ..Default::default()
        })
    }

    /// Create a new TCP client for the provided server address using TLS.
    pub fn new_tls(server_address: &str, domain: &str) -> Result<Self, IggyError> {
        Self::create(TcpClientConfig {
            server_address: server_address.to_string(),
            tls_enabled: true,
            tls_domain: domain.to_string(),
            ..Default::default()
        })
    }

    /// Create a new TCP client based on the provided configuration.
    pub fn create(config: TcpClientConfig) -> Result<Self, IggyError> {
        let server_address = config.server_address.parse::<SocketAddr>()?;

        Ok(Self {
            config,
            server_address,
            stream: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
        })
    }

    async fn handle_response(
        &self,
        status: u32,
        length: u32,
        stream: &mut dyn ConnectionStream,
    ) -> Result<Vec<u8>, IggyError> {
        if status != 0 {
            error!(
                "Received an invalid response with status: {} ({}).",
                status,
                IggyError::from_code_as_string(status)
            );
            return Err(IggyError::InvalidResponse(status));
        }

        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(EMPTY_RESPONSE);
        }

        let mut response_buffer = vec![0u8; length as usize];
        stream.read(&mut response_buffer).await?;
        Ok(response_buffer)
    }
}
