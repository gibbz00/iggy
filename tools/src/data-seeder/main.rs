mod seeder;

use anyhow::Result;
use clap::Parser;
use iggy::args::Args;
use iggy::client::{Client, UserClient};
use iggy::client_provider::{self, ClientConfig};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::http::client::HttpClient;
use iggy::quic::client::QuicClient;
use iggy::tcp::client::TcpClient;
use iggy::users::login_user::LoginUser;
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::error::Error;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct DataSeederArgs {
    #[clap(flatten)]
    pub(crate) iggy: Args,

    #[arg(long, default_value = "iggy")]
    pub username: String,

    #[arg(long, default_value = "iggy")]
    pub password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = DataSeederArgs::parse();
    tracing_subscriber::fmt::init();
    info!("Selected transport: {}", args.iggy.transport);

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

    let username = args.username.clone();
    let password = args.password.clone();
    let client = IggyClient::create(
        transport_client,
        IggyClientConfig::default(),
        None,
        None,
        (!args.iggy.encryption_key.is_empty()).then_some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&args.iggy.encryption_key).unwrap(),
        )),
    );

    client
        .login_user(&LoginUser { username, password })
        .await
        .unwrap();

    info!("Data seeder has started...");
    seeder::seed(&client).await.unwrap();
    info!("Data seeder has finished.");
    Ok(())
}
