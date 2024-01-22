use clap::Parser;
use iggy::client::Client;
use iggy::client_provider::ClientConfig;
use iggy::http::client::HttpClient;
use iggy::models::messages::Message;
use iggy::quic::client::QuicClient;
use iggy::tcp::client::TcpClient;
use iggy_examples::shared::args::Args;
use iggy_examples::shared::system;
use std::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Basic consumer has started, selected transport: {}",
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
    let client = C::from_config(client_config)?;

    system::login_root(&client).await;
    system::init_by_consumer(args, &client).await;
    system::consume_messages(args, &client, &handle_message).await
}

fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.offset, payload
    );
    Ok(())
}
