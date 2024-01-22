use iggy::client::{Client, StreamClient, TopicClient, UserClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use iggy::topics::create_topic::CreateTopic;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use std::env;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const BATCHES_LIMIT: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let tcp_client_config = TcpClientConfig {
        server_address: get_tcp_server_addr(),
        ..TcpClientConfig::default()
    };
    let tcp_client = TcpClient::create(tcp_client_config).unwrap();
    let client = IggyClient::create(tcp_client, IggyClientConfig::default(), None, None, None);

    // Or, instead of above lines, you can just use below code, which will create a Iggy
    // TCP client with default config (default server address for TCP is 127.0.0.1:8090):
    // let client = IggyClient::default();

    client.connect().await?;
    client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await?;
    init_system(&client).await;
    produce_messages(&client).await
}

async fn init_system(client: &IggyClient<TcpClient>) {
    match client
        .create_stream(&CreateStream {
            stream_id: Some(STREAM_ID),
            name: "sample-stream".to_string(),
        })
        .await
    {
        Ok(_) => info!("Stream was created."),
        Err(_) => warn!("Stream already exists and will not be created again."),
    }

    match client
        .create_topic(&CreateTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Some(TOPIC_ID),
            partitions_count: 1,
            name: "sample-topic".to_string(),
            message_expiry: None,
            max_topic_size: None,
            replication_factor: 1,
        })
        .await
    {
        Ok(_) => info!("Topic was created."),
        Err(_) => warn!("Topic already exists and will not be created again."),
    }
}

async fn produce_messages(client: &impl Client) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );

    let mut current_id = 0;
    let messages_per_batch = 10;
    let mut sent_batches = 0;
    loop {
        if sent_batches == BATCHES_LIMIT {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        let mut messages = Vec::new();
        for _ in 0..messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::from_str(&payload)?;
            messages.push(message);
        }
        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(STREAM_ID)?,
                topic_id: Identifier::numeric(TOPIC_ID)?,
                partitioning: Partitioning::partition_id(PARTITION_ID),
                messages,
            })
            .await?;
        sent_batches += 1;
        info!("Sent {messages_per_batch} message(s).");
        sleep(interval).await;
    }
}

fn get_tcp_server_addr() -> String {
    let default_server_addr = "127.0.0.1:8090".to_string();
    let argument_name = env::args().nth(1);
    let tcp_server_addr = env::args().nth(2);

    if argument_name.is_none() && tcp_server_addr.is_none() {
        default_server_addr
    } else {
        let argument_name = argument_name.unwrap();
        if argument_name != "--tcp-server-address" {
            panic!(
                "Invalid argument {}! Usage: {} --tcp-server-address <server-address>",
                argument_name,
                env::args().next().unwrap()
            );
        }
        let tcp_server_addr = tcp_server_addr.unwrap();
        if tcp_server_addr.parse::<std::net::SocketAddr>().is_err() {
            panic!(
                "Invalid server address {}! Usage: {} --tcp-server-address <server-address>",
                tcp_server_addr,
                env::args().next().unwrap()
            );
        }
        info!("Using server address: {}", tcp_server_addr);
        tcp_server_addr
    }
}
