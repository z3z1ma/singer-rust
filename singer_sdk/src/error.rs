//! Contains definitions of errors within the SDK

/// Defines various errors in the SDK
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Occurs when a command fails to execute. This is differs from
    /// CommandError since it occurs when the command fails to execute
    /// rather than executing but failing with an error.
    #[error("Failed to exec the command")]
    ExecError(std::io::Error),

    /// Occurs when the command exits unsuccessfully. It contains the exit code
    /// as well as the output from stderr.
    #[error("Command failed to exit successfully. Exit code ({:0?}) \n stderr: {1}")]
    CommandError(Option<i32>, String),

    #[error("IOError {0}")]
    IoError(#[from] std::io::Error),

    #[error("Failed to deserialize the value {0}")]
    DeserializationError(#[from] serde_json::Error),

    #[error("Trying to send a message in a channel where all receivers are dropped")]
    SendError(Box<dyn std::error::Error + Send + Sync + 'static>),

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
