use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `LogoutUser` command is used to logout the authenticated user.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LogoutUser {}

impl CommandPayload for LogoutUser {}

impl Validatable<Error> for LogoutUser {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl BytesSerializable for LogoutUser {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<LogoutUser, Error> {
        if !bytes.is_empty() {
            return Err(Error::InvalidCommand);
        }

        let command = LogoutUser {};
        command.validate()?;
        Ok(command)
    }
}

impl Display for LogoutUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = LogoutUser {};
        let bytes = command.as_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![];
        let command = LogoutUser::from_bytes(&bytes);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![0];
        let command = LogoutUser::from_bytes(&bytes);
        assert!(command.is_err());
    }
}
