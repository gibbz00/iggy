use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::identifier::Identifier;
use anyhow::Context;

use tracing::{event, Level};

pub struct DeleteConsumerGroupCmd {
    delete_consumer_group: DeleteConsumerGroup,
}

impl DeleteConsumerGroupCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, consumer_group_id: Identifier) -> Self {
        Self {
            delete_consumer_group: DeleteConsumerGroup {
                stream_id,
                topic_id,
                consumer_group_id,
            },
        }
    }
}

impl CliCommand for DeleteConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "delete consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
            self.delete_consumer_group.consumer_group_id,
            self.delete_consumer_group.topic_id,
            self.delete_consumer_group.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &impl Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_consumer_group(&self.delete_consumer_group)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
                    self.delete_consumer_group.consumer_group_id, self.delete_consumer_group.topic_id, self.delete_consumer_group.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer group with ID: {} deleted for topic with ID: {} and stream with ID: {}",
            self.delete_consumer_group.consumer_group_id,
            self.delete_consumer_group.topic_id,
            self.delete_consumer_group.stream_id,
        );

        Ok(())
    }
}
