use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::args::common::IggyBenchArgs;
use crate::args::simple::BenchmarkKind;
use crate::consumer::Consumer;
use iggy::client::Client;
use iggy::tcp::client::TcpClient;
use integration::test_server::MockClient;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::info;

pub struct PollMessagesBenchmark<C: Client = TcpClient> {
    args: IggyBenchArgs,
    client_marker: PhantomData<C>,
}

impl<C: Client> PollMessagesBenchmark<C> {
    pub fn new(args: Arc<IggyBenchArgs>) -> Self {
        Self {
            args,
            client_marker: PhantomData,
        }
    }
}

impl<C: Client> Benchmarkable for PollMessagesBenchmark<C> {
    async fn run(&mut self) -> BenchmarkFutures {
        self.check_streams().await?;
        let clients_count = self.args.consumers();
        info!("Creating {} client(s)...", clients_count);
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();

        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity(clients_count as usize));
        for client_id in 1..=clients_count {
            let args = self.args.clone();
            info!("Executing the benchmark on client #{}...", client_id);
            let args = args.clone();
            let start_stream_id = args.start_stream_id();
            let parallel_consumer_streams = !args.disable_parallel_consumer_streams();
            let stream_id = match parallel_consumer_streams {
                true => start_stream_id + client_id,
                false => start_stream_id + 1,
            };

            let consumer = Consumer::new(client_id, stream_id, messages_per_batch, message_batches);

            let future = Box::pin(async move { consumer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!("Created {} client(s).", clients_count);
        futures
    }

    fn kind(&self) -> BenchmarkKind {
        BenchmarkKind::Poll
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client(&self) -> &Arc<dyn MockClient> {
        &self.client_factory
    }

    fn display_settings(&self) {
        let total_messages = self.total_messages();
        let total_size_bytes = total_messages * self.args().message_size() as u64;
        info!(
                "\x1B[32mBenchmark: {}, total messages: {}, total size: {} bytes, {} streams, {} messages per batch, {} batches, {} bytes per message, {} consumers\x1B[0m",
                self.kind(),
                total_messages,
                total_size_bytes,
                self.args().number_of_streams(),
                self.args().messages_per_batch(),
                self.args().message_batches(),
                self.args().message_size(),
                self.args().consumers(),
            );
    }
}
