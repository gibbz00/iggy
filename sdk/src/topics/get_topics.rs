use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetTopics` command is used to retrieve the collection of topics from a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetTopics {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
}

impl CommandPayload for GetTopics {}

impl Validatable<Error> for GetTopics {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl BytesSerializable for GetTopics {
    fn as_bytes(&self) -> Vec<u8> {
        self.stream_id.as_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<GetTopics, Error> {
        if bytes.len() < 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = Identifier::from_bytes(bytes)?;
        let command = GetTopics { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetTopics {
            stream_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.as_bytes();
        let stream_id = Identifier::from_bytes(&bytes).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let bytes = stream_id.as_bytes();
        let command = GetTopics::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
