use std::net::SocketAddr;
use std::sync::Arc;

use crate::binary::command;
use crate::quic::quic_sender::QuicSender;
use crate::server_error::ServerError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::{anyhow, Context};
use iggy::command::Command;
use iggy::{bytes_serializable::BytesSerializable, messages::MAX_PAYLOAD_SIZE};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tracing::{debug, error, info};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_BYTES_LENGTH: usize = 4;

pub fn start(endpoint: Endpoint, system: SharedSystem) {
    for _ in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let system = system.clone();
        tokio::spawn(async move {
            while let Some(incoming_connection) = endpoint.accept().await {
                info!(
                    "Incoming connection from client: {}",
                    incoming_connection.remote_address()
                );
                let system = system.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(incoming_connection, system).await {
                        error!("Connection has failed: {error}");
                    }
                });
            }
        });
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    system: SharedSystem,
) -> Result<(), ServerError> {
    let connection = incoming_connection.await?;
    let address = connection.remote_address();
    info!("Client has connected: {address}");
    let client_id = system.read().add_client(&address, Transport::Quic).await;
    let session = Arc::new(Session::from_client_id(client_id, address));

    while let Some(stream) = accept_stream(&connection, &system, &address).await? {
        let system = system.clone();
        let session = session.clone();

        let handle_stream_task = async move {
            if let Err(err) = handle_stream(stream, system, session).await {
                error!("Error when handling QUIC stream: {:?}", err)
            }
        };
        let _handle = tokio::spawn(handle_stream_task);
    }
    Ok(())
}

type BiStream = (SendStream, RecvStream);

async fn accept_stream(
    connection: &Connection,
    system: &SharedSystem,
    address: &SocketAddr,
) -> Result<Option<BiStream>, ServerError> {
    match connection.accept_bi().await {
        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
            info!("Connection closed");
            system.read().delete_client(address).await;
            Ok(None)
        }
        Err(error) => {
            error!("Error when handling QUIC stream: {:?}", error);
            system.read().delete_client(address).await;
            Err(error.into())
        }
        Ok(stream) => Ok(Some(stream)),
    }
}

async fn handle_stream(
    stream: BiStream,
    system: SharedSystem,
    session: impl AsRef<Session>,
) -> anyhow::Result<()> {
    let (send_stream, mut recv_stream) = stream;
    let request = recv_stream
        .read_to_end(MAX_PAYLOAD_SIZE as usize)
        .await
        .with_context(|| "Error when reading the QUIC request.")?;

    if request.len() < INITIAL_BYTES_LENGTH {
        return Err(anyhow!(
            "Unable to read the QUIC request length, expected: {INITIAL_BYTES_LENGTH} bytes, received: {} bytes.",
            request.len()
        ));
    }

    debug!("Trying to read command...");
    let length = request[..INITIAL_BYTES_LENGTH]
        .try_into()
        .map(u32::from_le_bytes)
        .unwrap_or_default();
    let command = Command::from_bytes(&request[INITIAL_BYTES_LENGTH..])
        .with_context(|| "Error when reading the QUIC request command.")?;

    debug!("Received a QUIC command: {command}, payload size: {length}");

    let mut sender = QuicSender {
        send: send_stream,
        recv: recv_stream,
    };
    command::handle(&command, &mut sender, session.as_ref(), system.clone())
        .await
        .with_context(|| "Error when handling the QUIC request.")
}
