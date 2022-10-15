//! Contains definitions of errors within the SDK

/// Defines various errors in the SDK
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Trying to send a message in a channel where all receivers are dropped")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<crate::messages::Message>),

    #[error("IOError {0}")]
    IoError(#[from] std::io::Error),

    #[error("IOError {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Failed to deserialize the value {0}")]
    DeserializationError(#[from] serde_json::Error),

    #[error("Invalid conversion :: found ({0}) expected ({1})")]
    InvalidConversion(&'static str, &'static str),

    #[error("File could not be found: {0}")]
    FileNotFound(String),

    #[error("Option not set: {0}")]
    OptionNotSet(&'static str),

    #[error("Option is not valid: {0}")]
    InvalidOption(&'static str),

    #[error("The JSON schema for stream {0} has not been registered")]
    JSONSchemaNotRegistered(String),

    #[error("The value could not be compiled to a JSON schema")]
    JSONSchemaCompilationError,

    #[error("The value was invalid for the JSON schema. {0}")]
    JSONSchemaValidationError(String),

    #[error("An unexpected error occurred. {0}")]
    OtherError(&'static str),
}
