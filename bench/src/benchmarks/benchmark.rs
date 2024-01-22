use super::{
    poll_benchmark::PollMessagesBenchmark, send_and_poll_benchmark::SendAndPollMessagesBenchmark,
    send_benchmark::SendMessagesBenchmark,
};
use crate::{
    args::{common::IggyBenchArgs, simple::BenchmarkKind},
    benchmark_result::BenchmarkResult,
};
use async_trait::async_trait;
use futures::Future;
use iggy::{
    client::{StreamClient, TopicClient},
    clients::client::{IggyClient, IggyClientConfig},
    error::IggyError,
    identifier::Identifier,
    streams::{create_stream::CreateStream, get_streams::GetStreams},
    topics::create_topic::CreateTopic,
};
use integration::test_server::{login_root, MockClient};
use std::{pin::Pin, sync::Arc};
use tracing::info;

pub type BenchmarkFutures = Result<
    Vec<Pin<Box<dyn Future<Output = Result<BenchmarkResult, IggyError>> + Send>>>,
    IggyError,
>;

pub trait Benchmarkable {
    type BenchArgs;

    fn new(bench_args: Self::BenchArgs) -> Self;

    async fn run(&mut self) -> BenchmarkFutures;
    fn kind(&self) -> BenchmarkKind;
    fn args(&self) -> &IggyBenchArgs;
    fn display_settings(&self);

    /// Below methods have common implementation for all benchmarks.

    /// Initializes the streams and topics for the benchmark.
    /// This method is called before the benchmark is executed.
    async fn init_streams(&self) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().number_of_streams();
        let topic_id: u32 = 1;
        let partitions_count: u32 = 1;
        let client = self.client().mock().await;
        let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);
        login_root(&client).await;
        let streams = client.get_streams(&GetStreams {}).await?;
        for i in 1..=number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                info!("Creating the test stream {}", stream_id);
                let name = format!("stream {}", stream_id);
                client
                    .create_stream(&CreateStream {
                        stream_id: Some(stream_id),
                        name,
                    })
                    .await?;

                info!(
                    "Creating the test topic {} for stream {}",
                    topic_id, stream_id
                );
                let name = format!("topic {}", topic_id);
                client
                    .create_topic(&CreateTopic {
                        stream_id: Identifier::numeric(stream_id)?,
                        topic_id: Some(topic_id),
                        partitions_count,
                        name,
                        message_expiry: None,
                        max_topic_size: None,
                        replication_factor: 1,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_streams(&self) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().number_of_streams();
        let client = self.client().mock().await;
        let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);
        login_root(&client).await;
        let streams = client.get_streams(&GetStreams {}).await?;
        for i in 1..=number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                return Err(IggyError::ResourceNotFound(format!(
                    "Streams for testing are not properly initialized. Stream with id: {} is missing.",
                    stream_id
                )));
            }
        }
        Ok(())
    }

    /// Returns the total number of messages that will be sent or polled by the benchmark.
    fn total_messages(&self) -> u64 {
        let messages_per_batch = self.args().messages_per_batch();
        let message_batches = self.args().message_batches();
        let streams = self.args().number_of_streams();
        (messages_per_batch * message_batches * streams) as u64
    }
}
