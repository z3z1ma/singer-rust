//! Contains definitions of messages within the  specification
//! as well as utility methods for working with them
use serde::{self, Deserialize, Serialize};
use serde_json::{de::IoRead, json, Deserializer, StreamDeserializer, Value};

use time::macros::format_description;
use time::{format_description::FormatItem, OffsetDateTime};

use std::{
    convert::TryFrom,
    default::Default,
    io::BufRead,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::error::Error;

type Result<T> = std::result::Result<T, Error>;

/// A date format description for singer messages
pub const SINGER_DATE_FORMAT: &[FormatItem] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// The [`Message::Schema`] message defines the structure of the data
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Schema {
    pub(crate) stream: String,
    pub(crate) schema: serde_json::Value,
    #[serde(default)]
    pub key_properties: Vec<String>,
    pub bookmark_properties: Option<Vec<String>>,
}

impl Schema {
    /// Create a record message with current time as time_extracted
    pub fn new<S: Into<String>>(stream: S, schema: serde_json::Value) -> Self {
        Self { stream: stream.into(), schema, ..Default::default() }
    }
    /// A builder pattern implementation for adding key props
    pub fn set_key_properties(mut self, props: Vec<String>) -> Self {
        self.key_properties = props;
        self
    }
    /// A builder pattern implementation for adding bookmark props
    pub fn set_bookmark_properties(mut self, props: Vec<String>) -> Self {
        self.bookmark_properties = Some(props);
        self
    }
}

/// The [`Message::Record`] message contains the actual data being communicated
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Record {
    pub stream: String,
    pub record: serde_json::Value,
    pub version: Option<String>,
    #[serde(default = "current_time")]
    pub time_extracted: String,
}

impl Record {
    /// Create a record message with current time as time_extracted
    pub fn new<S: Into<String>>(stream: S, record: serde_json::Value) -> Self {
        Self { stream: stream.into(), record, ..Default::default() }
    }
}

/// The [`Message::Batch`] message contains paths to record
/// files dumped to disk by the tap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub stream: String,
}

/// The [`Message::State`] message keeps track of the progress of an extraction
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct State {
    pub(crate) value: serde_json::Value,
}

/// A  message enum with serdes implementation that uses the `type`
/// tag as a discriminator being one of:
/// [`Schema`]
/// [`Record`]
/// [`Batch`]
/// [`State`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all(deserialize = "UPPERCASE", serialize = "UPPERCASE"))]
pub enum Message {
    Record(Record),
    State(State),
    Schema(Schema),
    Batch(Batch),
}

impl Message {
    /// Returns true if message is of variant [`State`]
    pub fn is_state(&self) -> bool {
        match self {
            Message::State(_) => true,
            _ => false,
        }
    }

    /// Returns true if message is of variant [`Schema`]
    pub fn is_schema(&self) -> bool {
        match self {
            Message::Schema(_) => true,
            _ => false,
        }
    }

    /// Returns true if message is of variant [`Record`]
    pub fn is_record(&self) -> bool {
        match self {
            Message::Record(_) => true,
            _ => false,
        }
    }

    /// Returns true if message is of variant [`Batch`]
    pub fn is_batch(&self) -> bool {
        match self {
            Message::Batch(_) => true,
            _ => false,
        }
    }

    /// Return an option containing the [`State`] message if matched
    pub fn as_state(&self) -> Option<&State> {
        match self {
            Message::State(state) => Some(state),
            _ => None,
        }
    }

    /// Return an option containing the [`Schema`] message if matched
    pub fn as_schema(&self) -> Option<&Schema> {
        match self {
            Message::Schema(schema) => Some(schema),
            _ => None,
        }
    }

    /// Return an option containing the [`Record`] message if matched
    pub fn as_record(&self) -> Option<&Record> {
        match self {
            Message::Record(record) => Some(record),
            _ => None,
        }
    }

    /// Return an option containing the [`Batch`] message if matched
    pub fn as_batch(&self) -> Option<&Batch> {
        match self {
            Message::Batch(batch) => Some(batch),
            _ => None,
        }
    }

    pub fn ty(&self) -> &'static str {
        match self {
            Self::State { .. } => "status",
            Self::Schema { .. } => "schema",
            Self::Record { .. } => "record",
            Self::Batch { .. } => "batch",
        }
    }
}

impl From<State> for Message {
    fn from(state: State) -> Self {
        Self::State(state)
    }
}

impl TryFrom<Message> for State {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::State(state) => Ok(state),
            _ => Err(Error::InvalidConversion("state", m.ty())),
        }
    }
}

impl From<Schema> for Message {
    fn from(schema: Schema) -> Self {
        Self::Schema(schema)
    }
}

impl TryFrom<Message> for Schema {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::Schema(schema) => Ok(schema),
            _ => Err(Error::InvalidConversion("schema", m.ty())),
        }
    }
}

impl From<Record> for Message {
    fn from(record: Record) -> Self {
        Self::Record(record)
    }
}

impl TryFrom<Message> for Record {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::Record(record) => Ok(record),
            _ => Err(Error::InvalidConversion("record", m.ty())),
        }
    }
}

impl From<Batch> for Message {
    fn from(batch: Batch) -> Self {
        Self::Batch(batch)
    }
}

impl TryFrom<Message> for Batch {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::Batch(batch) => Ok(batch),
            _ => Err(Error::InvalidConversion("batch", m.ty())),
        }
    }
}

/// This provides a default for serde when serializing [`Message::Record`] messages
fn current_time() -> String {
    OffsetDateTime::now_utc().format(&SINGER_DATE_FORMAT).unwrap()
}

/// Add Singer Data Capture metadata to a [`Record`]
pub fn add_sdc_to_record(mut message: Record) -> Record {
    let time = json!(OffsetDateTime::now_utc().format(&SINGER_DATE_FORMAT).unwrap());
    message.record["_sdc_received_at"] = time.clone();
    if message.time_extracted.is_empty() {
        message.record["_sdc_extracted_at"] = time.clone();
    } else {
        message.record["_sdc_extracted_at"] = json!(message.time_extracted);
    }
    message.record["_sdc_sequence"] =
        json!(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    message
}

/// Remove Singer Data Capture metadata to a [`Record`]
pub fn remove_sdc_from_record(mut message: Record) -> Record {
    if let Value::Object(ref mut record) = message.record {
        record.remove("_sdc_extracted_at");
        record.remove("_sdc_received_at");
        record.remove("_sdc_sequence");
        record.remove("_sdc_batched_at");
        record.remove("_sdc_deleted_at");
        record.remove("_sdc_table_version");
    }
    message
}

/// Stamp a batch of Singer records with a timestamp
pub fn stamp_record_batch(mut messages: Vec<Record>) -> Vec<Record> {
    let time = json!(current_time());
    messages.iter_mut().for_each(|record_message| {
        record_message.record["_sdc_batched_at"] = json!(time);
    });
    messages
}

/// Convert any object implementing BufRead into an iterable of Singer Messages
pub fn into_singer_message_stream(
    io_input: Box<dyn BufRead>,
) -> StreamDeserializer<'static, IoRead<Box<dyn BufRead>>, Message> {
    Deserializer::from_reader(io_input).into_iter::<Message>()
}

pub type SingerReceiver = tokio::sync::mpsc::Receiver<Message>;
pub type SingerSender = tokio::sync::mpsc::Sender<Message>;
