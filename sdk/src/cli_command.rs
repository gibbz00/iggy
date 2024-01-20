use crate::client::Client;
use anyhow::{Error, Result};

pub static PRINT_TARGET: &str = "iggy::cli::output";

pub trait CliCommand {
    fn explain(&self) -> String;
    fn use_tracing(&self) -> bool {
        true
    }
    fn login_required(&self) -> bool {
        true
    }
    async fn execute_cmd(&mut self, client: &impl Client) -> Result<(), Error>;
}
