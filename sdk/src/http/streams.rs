use crate::client::StreamClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::models::stream::{Stream, StreamDetails};
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::purge_stream::PurgeStream;
use crate::streams::update_stream::UpdateStream;

const PATH: &str = "/streams";

impl StreamClient for HttpClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, IggyError> {
        let response = self
            .get(&get_details_path(&command.stream_id.as_string()))
            .await?;
        let stream = response.json().await?;
        Ok(stream)
    }

    async fn get_streams(&self, _command: &GetStreams) -> Result<Vec<Stream>, IggyError> {
        let response = self.get(PATH).await?;
        let streams = response.json().await?;
        Ok(streams)
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), IggyError> {
        self.post(PATH, &command).await?;
        Ok(())
    }

    async fn update_stream(&self, command: &UpdateStream) -> Result<(), IggyError> {
        self.put(&get_details_path(&command.stream_id.as_string()), command)
            .await?;
        Ok(())
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), IggyError> {
        let path = format!("{}/{}", PATH, command.stream_id.as_string());
        self.delete(&path).await?;
        Ok(())
    }

    async fn purge_stream(&self, command: &PurgeStream) -> Result<(), IggyError> {
        let path = format!("{}/{}/purge", PATH, command.stream_id.as_string());
        self.delete(&path).await?;
        Ok(())
    }
}

fn get_details_path(stream_id: &str) -> String {
    format!("{PATH}/{stream_id}")
}
