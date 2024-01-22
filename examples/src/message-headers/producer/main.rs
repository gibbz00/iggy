use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use iggy::client::{Client, MessageClient};
use iggy::client_provider::ClientConfig;
use iggy::clients::client::IggyClient;
use iggy::http::client::HttpClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::quic::client::QuicClient;
use iggy::tcp::client::TcpClient;
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages_generator::MessagesGenerator;
use iggy_examples::shared::system;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Message headers producer has started, selected transport: {}",
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

    let client = IggyClient::builder(transport_client).build();
    system::login_root(&client).await;
    system::init_by_producer(args, &client).await?;
    produce_messages(args, &client).await
}

async fn produce_messages<C: Client>(
    args: &Args,
    client: &IggyClient<C>,
) -> Result<(), Box<dyn Error>> {
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.stream_id, args.topic_id, args.partition_id, args.interval
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut message_generator = MessagesGenerator::new();
    let mut sent_batches = 0;
    loop {
        if args.message_batches_limit > 0 && sent_batches == args.message_batches_limit {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        let mut messages = Vec::new();
        let mut serializable_messages = Vec::new();
        for _ in 0..args.messages_per_batch {
            let serializable_message = message_generator.generate();
            // You can send the different message types to the same partition, or stick to the single type.
            let message_type = serializable_message.get_message_type();
            let json = serializable_message.to_json();

            // The message type will be stored in the custom message header.
            let mut headers = HashMap::new();
            headers.insert(
                HeaderKey::new("message_type").unwrap(),
                HeaderValue::from_str(message_type).unwrap(),
            );

            let message = Message::new(None, Bytes::from(json), Some(headers));
            messages.push(message);
            // This is used for the logging purposes only.
            serializable_messages.push(serializable_message);
        }
        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(args.stream_id)?,
                topic_id: Identifier::numeric(args.topic_id)?,
                partitioning: Partitioning::partition_id(args.partition_id),
                messages,
            })
            .await?;
        sent_batches += 1;
        info!("Sent messages: {:#?}", serializable_messages);
        interval.tick().await;
    }
}
