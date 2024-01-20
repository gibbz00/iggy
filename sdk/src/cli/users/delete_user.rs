use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::users::delete_user::DeleteUser;
use anyhow::Context;

use tracing::{event, Level};

pub struct DeleteUserCmd {
    delete_user: DeleteUser,
}

impl DeleteUserCmd {
    pub fn new(user_id: Identifier) -> Self {
        Self {
            delete_user: DeleteUser { user_id },
        }
    }
}

impl CliCommand for DeleteUserCmd {
    fn explain(&self) -> String {
        format!("delete user with ID: {}", self.delete_user.user_id)
    }

    async fn execute_cmd(&mut self, client: &impl Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_user(&self.delete_user)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting user with ID: {}",
                    self.delete_user.user_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO, "User with ID: {} deleted", self.delete_user.user_id);

        Ok(())
    }
}
