mod args;
mod credentials;
mod error;
mod logging;

use crate::args::{
    client::ClientAction, consumer_group::ConsumerGroupAction, permissions::PermissionsArgs,
    personal_access_token::PersonalAccessTokenAction, stream::StreamAction, topic::TopicAction,
    Command, IggyConsoleArgs,
};
use crate::credentials::IggyCredentials;
use crate::error::IggyCmdError;
use crate::logging::Logging;
use anyhow::Context;
use args::message::MessageAction;
use args::partition::PartitionAction;
use args::user::UserAction;
use clap::Parser;
use iggy::args::Args;
use iggy::cli::{
    client::{get_client::GetClientCmd, get_clients::GetClientsCmd},
    consumer_group::{
        create_consumer_group::CreateConsumerGroupCmd,
        delete_consumer_group::DeleteConsumerGroupCmd, get_consumer_group::GetConsumerGroupCmd,
        get_consumer_groups::GetConsumerGroupsCmd,
    },
    message::{poll_messages::PollMessagesCmd, send_messages::SendMessagesCmd},
    partitions::{create_partitions::CreatePartitionsCmd, delete_partitions::DeletePartitionsCmd},
    personal_access_tokens::{
        create_personal_access_token::CreatePersonalAccessTokenCmd,
        delete_personal_access_tokens::DeletePersonalAccessTokenCmd,
        get_personal_access_tokens::GetPersonalAccessTokensCmd,
    },
    streams::{
        create_stream::CreateStreamCmd, delete_stream::DeleteStreamCmd, get_stream::GetStreamCmd,
        get_streams::GetStreamsCmd, update_stream::UpdateStreamCmd,
    },
    system::{me::GetMeCmd, ping::PingCmd, stats::GetStatsCmd},
    topics::{
        create_topic::CreateTopicCmd, delete_topic::DeleteTopicCmd, get_topic::GetTopicCmd,
        get_topics::GetTopicsCmd, update_topic::UpdateTopicCmd,
    },
    users::{
        change_password::ChangePasswordCmd,
        create_user::CreateUserCmd,
        delete_user::DeleteUserCmd,
        get_user::GetUserCmd,
        get_users::GetUsersCmd,
        update_permissions::UpdatePermissionsCmd,
        update_user::{UpdateUserCmd, UpdateUserType},
    },
    utils::personal_access_token_expiry::PersonalAccessTokenExpiry,
};
use iggy::cli_command::{CliCommand, PRINT_TARGET};
use iggy::client::{Client, PersonalAccessTokenClient, UserClient};
use iggy::client_provider::ClientConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::error::IggyError;
use iggy::http::client::HttpClient;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::quic::client::QuicClient;
use iggy::tcp::client::TcpClient;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::sync::Arc;
use tracing::{event, Level};

#[tokio::main]
async fn main() -> Result<(), IggyCmdError> {
    let args = IggyConsoleArgs::parse();

    if let Some(generator) = args.generator {
        args.generate_completion(generator);
        return Ok(());
    }

    if args.command.is_none() {
        IggyConsoleArgs::print_overview();
        return Ok(());
    }

    let mut logging = Logging::new();
    logging.init(args.quiet, &args.debug);

    match args.iggy.transport.as_str() {
        "tcp" => retrieve_and_execute_command::<TcpClient>(args).await?,
        "http" => retrieve_and_execute_command::<HttpClient>(args).await?,
        "quic" => retrieve_and_execute_command::<QuicClient>(args).await?,
        // TODO: iggy transport should be a parsed enum
        _ => unimplemented!(),
    }

    Ok(())
}

async fn retrieve_and_execute_command<C: Client>(
    console_args: IggyConsoleArgs,
) -> anyhow::Result<()> {
    let credentials = IggyCredentials::from_args(&console_args)?;
    let server_address = console_args.get_server_address().take();
    let iggy_args = console_args.iggy;

    match console_args.command.expect("some checked in main") {
        Command::Stream(sub_command) => match sub_command {
            StreamAction::Create(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    CreateStreamCmd::new(args.stream_id, args.name),
                )
                .await
            }
            StreamAction::Delete(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    DeleteStreamCmd::new(args.stream_id),
                )
                .await
            }
            StreamAction::Update(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    UpdateStreamCmd::new(args.stream_id, args.name),
                )
                .await
            }
            StreamAction::Get(args) => {
                execute_command::<C, _>(credentials, iggy_args, GetStreamCmd::new(args.stream_id))
                    .await
            }
            StreamAction::List(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetStreamsCmd::new(args.list_mode.into()),
                )
                .await
            }
        },
        Command::Topic(command) => match command {
            TopicAction::Create(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    CreateTopicCmd::new(
                        args.stream_id,
                        args.topic_id,
                        args.partitions_count,
                        args.name,
                        args.message_expiry.into(),
                        args.max_topic_size,
                        args.replication_factor,
                    ),
                )
                .await
            }
            TopicAction::Delete(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    DeleteTopicCmd::new(args.stream_id, args.topic_id),
                )
                .await
            }
            TopicAction::Update(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    UpdateTopicCmd::new(
                        args.stream_id,
                        args.topic_id,
                        args.name,
                        args.message_expiry.into(),
                        args.max_topic_size,
                        args.replication_factor,
                    ),
                )
                .await
            }
            TopicAction::Get(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetTopicCmd::new(args.stream_id, args.topic_id),
                )
                .await
            }
            TopicAction::List(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetTopicsCmd::new(args.stream_id, args.list_mode.into()),
                )
                .await
            }
        },
        Command::Partition(command) => match command {
            PartitionAction::Create(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    CreatePartitionsCmd::new(args.stream_id, args.topic_id, args.partitions_count),
                )
                .await
            }
            PartitionAction::Delete(args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    DeletePartitionsCmd::new(args.stream_id, args.topic_id, args.partitions_count),
                )
                .await
            }
        },
        Command::Ping(args) => {
            execute_command::<C, _>(credentials, iggy_args, PingCmd::new(args.count)).await
        }
        Command::Me => execute_command::<C, _>(credentials, iggy_args, GetMeCmd::new()).await,
        Command::Stats => execute_command::<C, _>(credentials, iggy_args, GetStatsCmd::new()).await,
        Command::Pat(command) => match command {
            PersonalAccessTokenAction::Create(pat_create_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    CreatePersonalAccessTokenCmd::new(
                        pat_create_args.name,
                        PersonalAccessTokenExpiry::new(pat_create_args.expiry),
                        console_args.quiet,
                        pat_create_args.store_token,
                        server_address.unwrap(),
                    ),
                )
                .await
            }
            PersonalAccessTokenAction::Delete(pat_delete_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    DeletePersonalAccessTokenCmd::new(
                        pat_delete_args.name,
                        server_address.unwrap(),
                    ),
                )
                .await
            }
            PersonalAccessTokenAction::List(pat_list_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetPersonalAccessTokensCmd::new(pat_list_args.list_mode.into()),
                )
                .await
            }
        },
        Command::User(command) => match command {
            UserAction::Create(create_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    CreateUserCmd::new(
                        create_args.username,
                        create_args.password,
                        create_args.user_status.into(),
                        PermissionsArgs::new(
                            create_args.global_permissions,
                            create_args.stream_permissions,
                        )
                        .into(),
                    ),
                )
                .await
            }
            UserAction::Delete(delete_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    DeleteUserCmd::new(delete_args.user_id),
                )
                .await
            }
            UserAction::Get(get_args) => {
                execute_command::<C, _>(credentials, iggy_args, GetUserCmd::new(get_args.user_id))
                    .await
            }
            UserAction::List(list_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetUsersCmd::new(list_args.list_mode.into()),
                )
                .await
            }
            UserAction::Name(name_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    UpdateUserCmd::new(name_args.user_id, UpdateUserType::Name(name_args.username)),
                )
                .await
            }
            UserAction::Status(status_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    UpdateUserCmd::new(
                        status_args.user_id,
                        UpdateUserType::Status(status_args.status.into()),
                    ),
                )
                .await
            }
            UserAction::Password(change_pwd_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    ChangePasswordCmd::new(
                        change_pwd_args.user_id,
                        change_pwd_args.current_password,
                        change_pwd_args.new_password,
                    ),
                )
                .await
            }
            UserAction::Permissions(permissions_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    UpdatePermissionsCmd::new(
                        permissions_args.user_id,
                        PermissionsArgs::new(
                            permissions_args.global_permissions,
                            permissions_args.stream_permissions,
                        )
                        .into(),
                    ),
                )
                .await
            }
        },
        Command::Client(command) => match command {
            ClientAction::Get(get_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetClientCmd::new(get_args.client_id),
                )
                .await
            }
            ClientAction::List(list_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetClientsCmd::new(list_args.list_mode.into()),
                )
                .await
            }
        },
        Command::ConsumerGroup(command) => match command {
            ConsumerGroupAction::Create(create_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    CreateConsumerGroupCmd::new(
                        create_args.stream_id,
                        create_args.topic_id,
                        create_args.consumer_group_id,
                        create_args.name,
                    ),
                )
                .await
            }
            ConsumerGroupAction::Delete(delete_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    DeleteConsumerGroupCmd::new(
                        delete_args.stream_id,
                        delete_args.topic_id,
                        delete_args.consumer_group_id,
                    ),
                )
                .await
            }
            ConsumerGroupAction::Get(get_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetConsumerGroupCmd::new(
                        get_args.stream_id,
                        get_args.topic_id,
                        get_args.consumer_group_id,
                    ),
                )
                .await
            }
            ConsumerGroupAction::List(list_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    GetConsumerGroupsCmd::new(
                        list_args.stream_id,
                        list_args.topic_id,
                        list_args.list_mode.into(),
                    ),
                )
                .await
            }
        },
        Command::Message(command) => match command {
            MessageAction::Send(send_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    SendMessagesCmd::new(
                        send_args.stream_id,
                        send_args.topic_id,
                        send_args.partition_id,
                        send_args.message_key,
                        send_args.messages,
                    ),
                )
                .await
            }
            MessageAction::Poll(poll_args) => {
                execute_command::<C, _>(
                    credentials,
                    iggy_args,
                    PollMessagesCmd::new(
                        poll_args.stream_id,
                        poll_args.topic_id,
                        poll_args.partition_id,
                        poll_args.message_count,
                        poll_args.auto_commit,
                        poll_args.offset,
                        poll_args.first,
                        poll_args.last,
                        poll_args.next,
                        poll_args.consumer,
                    ),
                )
                .await
            }
        },
    }
}

async fn execute_command<C: Client, T: CliCommand>(
    credentials: IggyCredentials,
    iggy_args: Args,
    mut command: T,
) -> anyhow::Result<()> {
    let encryption: Option<Box<dyn Encryptor>> =
        (!iggy_args.encryption_key.is_empty()).then_some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&iggy_args.encryption_key)?,
        ));

    let client_config = <C::Config as ClientConfig>::from_args(iggy_args);

    let transport_client = C::from_config(client_config)?;
    let iggy_client = IggyClient::create(
        transport_client,
        IggyClientConfig::default(),
        None,
        None,
        encryption,
    );

    let login_required = command.login_required();
    if login_required {
        match credentials {
            IggyCredentials::UserNameAndPassword(username_and_password) => {
                iggy_client
                    .login_user(&LoginUser {
                        username: username_and_password.username.clone(),
                        password: username_and_password.password.clone(),
                    })
                    .await
                    .context(format!(
                        "Problem with server login for username: {}",
                        &username_and_password.username
                    ))?;
            }
            IggyCredentials::PersonalAccessToken(token_value) => {
                iggy_client
                    .login_with_personal_access_token(&LoginWithPersonalAccessToken {
                        token: token_value.clone(),
                    })
                    .await
                    .with_context(|| {
                        format!("Problem with server login with token: {}", &token_value)
                    })?;
            }
        }
    }

    if command.use_tracing() {
        event!(target: PRINT_TARGET, Level::INFO, "Executing {}", command.explain());
    } else {
        println!("Executing {}", command.explain());
    }

    command.execute_cmd(&iggy_client).await?;

    if login_required {
        iggy_client
            .logout_user(&LogoutUser {})
            .await
            .with_context(|| "Problem with server logout".to_string())?;
    }

    Ok(())
}
