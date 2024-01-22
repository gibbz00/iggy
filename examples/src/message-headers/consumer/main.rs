use std::error::Error;

use anyhow::Result;
use clap::Parser;
use iggy::client::Client;
use iggy::client_provider::ClientConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig, PollMessagesConfig, StoreOffsetKind};
use iggy::http::client::HttpClient;
use iggy::models::header::HeaderKey;
use iggy::models::messages::Message;
use iggy::quic::client::QuicClient;
use iggy::tcp::client::TcpClient;
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages::*;
use iggy_examples::shared::system;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Message headers consumer has started, selected transport: {}",
        args.transport
    );

    match args.transport.as_str() {
        "tcp" => run::<TcpClient>(&args).await,
        "http" => run::<HttpClient>(&args).await,
        "quic" => run::<QuicClient>(&args).await,
        // TEMP: args.transport should be an enum
        _ => unimplemented!(),
    }
}

async fn run<C: Client>(args: &Args) -> Result<(), Box<dyn Error>> {
    let client_config = <C::Config as ClientConfig>::from_args(args.to_sdk_args());
    let transport_client = C::from_config(client_config)?;

    let client = IggyClient::builder(transport_client)
        .with_config(IggyClientConfig {
            poll_messages: PollMessagesConfig {
                interval: args.interval,
                store_offset_kind: StoreOffsetKind::WhenMessagesAreProcessed,
            },
            ..Default::default()
        })
        .build();
    system::login_root(&client).await;
    system::init_by_consumer(args, &client).await;
    system::consume_messages(args, &client, &handle_message).await
}

fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a JSON string.
    let payload = std::str::from_utf8(&message.payload)?;
    // The message type is stored in the custom message header.
    let header_key = HeaderKey::new("message_type").unwrap();
    let message_type = message
        .headers
        .as_ref()
        .unwrap()
        .get(&header_key)
        .unwrap()
        .as_str()?;
    info!(
        "Handling message type: {} at offset: {}...",
        message_type, message.offset
    );
    match message_type {
        ORDER_CREATED_TYPE => {
            let order_created = serde_json::from_str::<OrderCreated>(payload)?;
            info!("{:#?}", order_created);
        }
        ORDER_CONFIRMED_TYPE => {
            let order_confirmed = serde_json::from_str::<OrderConfirmed>(payload)?;
            info!("{:#?}", order_confirmed);
        }
        ORDER_REJECTED_TYPE => {
            let order_rejected = serde_json::from_str::<OrderRejected>(payload)?;
            info!("{:#?}", order_rejected);
        }
        _ => {
            warn!("Received unknown message type: {}", message_type);
        }
    }
    Ok(())
}
