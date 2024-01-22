use crate::args::IggyConsoleArgs;
use crate::error::{CmdToolError, IggyCmdError};
use iggy::cli_command::PRINT_TARGET;
use keyring::Entry;
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};
use std::env::var;
use tracing::{event, Level};

static ENV_IGGY_USERNAME: &str = "IGGY_USERNAME";
static ENV_IGGY_PASSWORD: &str = "IGGY_PASSWORD";

pub(crate) struct IggyUserClient {
    pub username: String,
    pub password: String,
}

pub(crate) enum IggyCredentials {
    UserNameAndPassword(IggyUserClient),
    PersonalAccessToken(String),
}

impl IggyCredentials {
    pub fn from_args(args: &IggyConsoleArgs) -> anyhow::Result<Self> {
        if let Some(token_name) = &args.token_name {
            match args.get_server_address() {
                Some(server_address) => {
                    let server_address = format!("iggy:{}", server_address);
                    event!(target: PRINT_TARGET, Level::DEBUG,"Checking token presence under service: {} and name: {}",
                    server_address, token_name);
                    let entry = Entry::new(&server_address, token_name)?;
                    let token = entry.get_password()?;

                    Ok(Self::PersonalAccessToken(token))
                }
                None => Err(IggyCmdError::CmdToolError(CmdToolError::MissingServerAddress).into()),
            }
        } else if let Some(token) = &args.token {
            Ok(Self::PersonalAccessToken(token.clone()))
        } else if let Some(username) = &args.username {
            let password = match &args.password {
                Some(password) => password.clone(),
                None => {
                    if isatty(Stream::Stdin) {
                        prompt_password_tty(Some("Password: "))?
                    } else {
                        prompt_password_stdin(None, Stream::Stdout)?
                    }
                }
            };

            Ok(Self::UserNameAndPassword(IggyUserClient {
                username: username.clone(),
                password,
            }))
        } else if var(ENV_IGGY_USERNAME).is_ok() && var(ENV_IGGY_PASSWORD).is_ok() {
            Ok(Self::UserNameAndPassword(IggyUserClient {
                username: var(ENV_IGGY_USERNAME).unwrap(),
                password: var(ENV_IGGY_PASSWORD).unwrap(),
            }))
        } else {
            Err(IggyCmdError::CmdToolError(CmdToolError::MissingCredentials).into())
        }
    }
}
