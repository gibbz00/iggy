use crate::args::common::IggyBenchArgs;
use crate::args::kind::BenchmarkKindCommand;
use crate::benchmark_result::BenchmarkResults;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::benchmarks::poll_benchmark::PollMessagesBenchmark;
use crate::benchmarks::send_and_poll_benchmark::SendAndPollMessagesBenchmark;
use crate::benchmarks::send_benchmark::SendMessagesBenchmark;
use crate::server_starter::start_server_if_needed;
use futures::future::try_join_all;
use iggy::error::IggyError;
use integration::test_server::TestServer;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub struct BenchmarkRunner {
    args: Option<IggyBenchArgs>,
    test_server: Option<TestServer>,
}

impl BenchmarkRunner {
    pub fn new(args: IggyBenchArgs) -> Self {
        Self {
            args: Some(args),
            test_server: None,
        }
    }

    pub async fn run(&mut self) -> Result<(), IggyError> {
        let mut args = self.args.take().unwrap();
        self.test_server = start_server_if_needed(&mut args).await;

        let transport = args.transport();
        let server_addr = args.server_address();
        info!("Starting to benchmark: {transport} with server: {server_addr}",);

        // TODO(XXX): match by transport
        match args.benchmark_kind {
            BenchmarkKindCommand::Send(bench_args) => {
                Self::benchmark::<SendMessagesBenchmark>(bench_args).await
            }
            BenchmarkKindCommand::Poll(bench_args) => {
                Self::benchmark::<PollMessagesBenchmark>(bench_args).await
            }
            BenchmarkKindCommand::SendAndPoll(bench_args) => {
                Self::benchmark::<SendAndPollMessagesBenchmark>(bench_args).await
            }
            BenchmarkKindCommand::Examples => todo!(),
        }
    }

    async fn benchmark<B: Benchmarkable>(bench_args: B::BenchArgs) -> Result<(), IggyError> {
        let mut benchmark = B::new(bench_args);
        let results = try_join_all(benchmark.run().await?).await?;

        // Sleep just to see result prints after all the join handles are done and tcp connections are closed
        sleep(Duration::from_millis(10)).await;
        let results: BenchmarkResults = results.into();

        benchmark.display_settings();

        // TODO: should probably just use println
        results
            .to_string()
            .split('\n')
            .for_each(|result| info!("{}", result));
        Ok(())
    }
}
