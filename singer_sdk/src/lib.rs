//! The Singer Rust SDK provides types, interfaces, and an execution framework for Singer
//! taps and targets

/// Contains definitions of messages within the Singer specification
pub mod messages {
    use serde::{self, Deserialize, Serialize};
    use serde_json::{self, de::IoRead, json, Deserializer, Map, StreamDeserializer, Value};
    use time::macros::format_description;
    use time::{format_description::FormatItem, OffsetDateTime};

    pub const SINGER_DATE_FORMAT: &[FormatItem] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

    /// The [`SingerMessage::SCHEMA`] message defines the structure of the data
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerSchema {
        pub stream: String,
        #[serde(default)]
        pub key_properties: Vec<String>,
        pub schema: Value,
    }

    /// The [`SingerMessage::RECORD`] message contains the actual data being communicated
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerRecord {
        pub stream: String,
        pub record: Value,
        #[serde(default = "current_time")]
        pub time_extracted: String,
    }

    /// The [`SingerMessage::BATCH`] message contains paths to record
    /// files dumped to disk by the tap
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerBatch {
        pub stream: String,
    }

    /// The [`SingerMessage::STATE`] message keeps track of the progress of an extraction
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerState {
        pub value: Value,
    }

    /// A Singer message enum with serdes implementation that uses the `type`
    /// tag as a discriminator being one of:
    /// [`SingerSchema`]
    /// [`SingerRecord`]
    /// [`SingerBatch`]
    /// [`SingerState`]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub enum SingerMessage {
        RECORD(SingerRecord),
        STATE(SingerState),
        SCHEMA(SingerSchema),
        BATCH(SingerBatch),
    }

    /// This provides a default for serde json when serializing [`SingerMessage::RECORD`] messages
    fn current_time() -> String {
        OffsetDateTime::now_utc()
            .format(&SINGER_DATE_FORMAT)
            .unwrap()
    }

    /// Add Singer Data Capture metadata to a [`SingerRecord`]
    pub fn add_sdc_to_record(mut record_message: SingerRecord) -> SingerRecord {
        let time = json!(OffsetDateTime::now_utc()
            .format(&SINGER_DATE_FORMAT)
            .unwrap());
        record_message.record["_sdc_received_at"] = time.clone();
        if record_message.time_extracted.is_empty() {
            record_message.record["_sdc_extracted_at"] = time.clone();
        } else {
            record_message.record["_sdc_extracted_at"] = json!(record_message.time_extracted);
        }
        record_message.record["_sdc_sequence"] = json!(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        record_message
    }

    /// Remove Singer Data Capture metadata to a [`SingerRecord`]
    pub fn remove_sdc_from_record(mut record_message: SingerRecord) -> SingerRecord {
        if let Value::Object(ref mut record) = record_message.record {
            record.remove("_sdc_extracted_at");
            record.remove("_sdc_received_at");
            record.remove("_sdc_sequence");
            record.remove("_sdc_batched_at");
            record.remove("_sdc_deleted_at");
            record.remove("_sdc_table_version");
        }
        record_message
    }

    /// Stamp a batch of Singer records with a timestamp
    pub fn stamp_record_batch(mut record_messages: Vec<SingerRecord>) -> Vec<SingerRecord> {
        let time = json!(current_time());
        record_messages.iter_mut().for_each(|record_message| {
            record_message.record["_sdc_batched_at"] = json!(time);
        });
        record_messages
    }

    pub fn singer_parser(
        io_input: Box<dyn std::io::BufRead>,
    ) -> StreamDeserializer<'static, IoRead<Box<dyn std::io::BufRead>>, SingerMessage> {
        Deserializer::from_reader(io_input).into_iter::<SingerMessage>()
    }
}

/// Contains Singer CLI implementation
mod interface {
    use clap::{self, Parser};
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::path::PathBuf;
    use std::str::FromStr;

    #[derive(Debug, Clone)]
    pub enum AboutFormat {
        Json,
        Markdown,
    }

    impl Display for AboutFormat {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                AboutFormat::Json => write!(formatter, "json"),
                AboutFormat::Markdown => write!(formatter, "markdown"),
            }
        }
    }

    #[derive(Debug)]
    pub struct AboutFormatError;

    impl Display for AboutFormatError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "{:?}", self)
        }
    }

    impl Error for AboutFormatError {
        fn description(&self) -> &str {
            "Invalid format option. Must be one of `json` or `markdown`."
        }
    }

    impl FromStr for AboutFormat {
        type Err = AboutFormatError;

        fn from_str(string: &str) -> Result<Self, Self::Err> {
            match string {
                "json" => Ok(Self::Json),
                "markdown" => Ok(Self::Markdown),
                _ => Err(AboutFormatError),
            }
        }
    }

    /* Target CLI Spec
    Options:
      --input FILENAME          A path to read messages from instead of from
                              standard in.
      --config TEXT             Configuration file location or 'ENV' to use
                              environment variables.
      --format [json|markdown]  Specify output style for --about
      --about                   Display package metadata and settings.
      --version                 Display the package version.
      --help                    Show this message and exit.
    */

    /// This struct defines the Singer Target CLI interface
    #[derive(Parser, Debug)]
    #[clap(author="z3z1ma", version, about="Singer target built with the Singer Rust SDK", long_about = None)]
    pub struct SingerTarget {
        /// A path to read messages from instead of from standard in.
        #[clap(long, value_parser)]
        pub input: Option<PathBuf>,
        /// Configuration file location or 'ENV' to use environment variables.
        #[clap(long, value_parser)]
        pub config: PathBuf,
        /// Specify output style for --about
        #[clap(long, value_parser)]
        pub format: Option<AboutFormat>,
        /// Display package metadata and settings.
        #[clap(long, value_parser, default_value_t = false)]
        pub about: bool,
    }

    impl SingerTarget {
        /// Parse CLI arguments passed to the program
        pub fn parse_args() -> Self {
            SingerTarget::parse()
        }
    }

    /* Tap CLI Spec
    Options:
      --state PATH              Use a bookmarks file for incremental replication.
      --catalog PATH            Use a Singer catalog file with the tap.
      --test TEXT               Use --test to sync a single record for each
                                stream. Use --test=schema to test schema output
                                without syncing records.
      --discover                Run the tap in discovery mode.
      --config TEXT             Configuration file location or 'ENV' to use
                                environment variables.
      --format [json|markdown]  Specify output style for --about
      --about                   Display package metadata and settings.
      --version                 Display the package version.
      --help                    Show this message and exit.
    */

    /// This struct defines the Singer Target CLI interface
    #[derive(Parser, Debug)]
    #[clap(author="z3z1ma", version, about="Singer tap built with the Singer Rust SDK", long_about = None)]
    pub struct SingerTap {
        /// Use a bookmarks file for incremental replication.
        #[clap(long, value_parser)]
        pub state: Option<PathBuf>,
        /// Use a Singer catalog file with the tap.
        #[clap(long, value_parser)]
        pub catalog: Option<PathBuf>,
        /// Use --test to sync a single record for each stream. Use --test=schema
        /// to test schema output without syncing records.
        #[clap(long, value_parser)]
        pub test: Option<String>,
        /// Run the tap in discovery mode.
        #[clap(long, value_parser, default_value_t = false, conflicts_with = "about")]
        pub discover: bool,
        /// Configuration file location or 'ENV' to use environment variables.
        #[clap(long, value_parser)]
        pub config: PathBuf,
        /// Display package metadata and settings.
        #[clap(long, value_parser, default_value_t = false)]
        pub about: bool,
    }

    impl SingerTap {
        /// Parse CLI arguments passed to the program
        pub fn parse_args() -> Self {
            SingerTap::parse()
        }
    }
}

pub mod target {
    use crate::interface::*;
    use crate::messages::*;

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::{fs, io, mem, process};

    use crossbeam::channel::{bounded, unbounded};
    use log::{self, debug, error, info};
    use pretty_env_logger;
    use rayon;
    use rayon::prelude::*;
    use serde_json::json;
    use serde_json::{self, from_reader as json_load, Value};

    fn parse_input(input: Option<fs::File>) -> Box<dyn io::BufRead> {
        match input {
            None => Box::new(io::stdin().lock()),
            Some(fobj) => Box::new(io::BufReader::new(fobj)),
        }
    }

    #[derive(Clone)]
    pub struct BaseConfiguration {
        pub buffer_size: usize,
        pub add_sdc_metadata: bool,
    }

    pub trait Processor {
        fn new(name: String, config: Value) -> Self;
        fn preprocess_record(&self, record: SingerRecord) -> SingerRecord {
            record
        }
        fn process_batch(&self, batch: Vec<SingerRecord>);
    }

    /// The primary entrypoint for a Singer Target
    /// This handles the main processing loop maximizing CPU utilization
    /// via leveraging a work-stealing pattern and global injector queue under the hood
    pub fn run<T>(base_config: BaseConfiguration) -> Result<(), Box<dyn std::error::Error>>
    where
        T: Processor + Sync,
        (Arc<T>, Vec<SingerRecord>): Send,
    {
        rayon::scope(
            |thread_pool| -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
                pretty_env_logger::init();
                let (tx, rx) = unbounded::<(Arc<T>, Vec<SingerRecord>)>();
                info!("Spawning batch processor iterator");
                thread_pool.spawn(|_| {
                    rx.into_iter()
                        .par_bridge()
                        .into_par_iter()
                        .for_each(|(processor, records)| processor.process_batch(records));
                });
                info!("Iterator spawned");
                debug!("Parsing CLI arguments");
                let args = SingerTarget::parse_args();
                debug!("Parsed, got {:?}", args);
                debug!("Checking if config file exists");
                let config_path = args.config;
                if !config_path.exists() || !config_path.is_file() {
                    error!("Invalid config file passed into --config");
                    process::exit(1);
                }
                let config_file = fs::File::open(config_path)?;
                let config: Value = json_load(config_file)?;
                info!("{}", config);
                let mut streams: HashMap<String, (Arc<T>, Vec<SingerRecord>)> = HashMap::new();
                let mut state = json!({ "value": null });
                let singer_input =
                    parse_input(args.input.map(|path| fs::File::open(path).unwrap()));
                for message in singer_parser(singer_input) {
                    match message? {
                        SingerMessage::SCHEMA(message) => {
                            info!("Creating sink for stream {}!", message.stream);
                            streams.insert(
                                message.stream.clone(),
                                (Arc::new(T::new(message.stream, config.clone())), Vec::new()),
                            );
                        }
                        SingerMessage::RECORD(mut message) => {
                            let (processor, stream) = streams
                                .get_mut(message.stream.as_str())
                                .expect(
                                "RECORD message encountered before corresponding SCHEMA message",
                            );
                            message = processor.preprocess_record(message);
                            if base_config.add_sdc_metadata {
                                message = add_sdc_to_record(message);
                            } else {
                                message = remove_sdc_from_record(message);
                            }
                            stream.push(message);
                            if stream.len() > base_config.buffer_size {
                                let batch = mem::take(stream);
                                tx.send((processor.clone(), batch)).unwrap();
                            }
                        }
                        SingerMessage::BATCH(message) => {
                            error!("BATCH spec not implemented!");
                            drop(message);
                        }
                        SingerMessage::STATE(message) => state["value"] = message.value,
                    }
                }
                for (name, (processor, ref mut stream)) in streams {
                    if stream.len() > 0 {
                        info!("Draining leftovers from {:?}", name);
                        let batch = mem::take(stream);
                        tx.send((processor.clone(), batch)).unwrap();
                    }
                }
                drop(tx);
                info!("STATE: {:?}", state);
                Ok(())
            },
        )
        .unwrap();
        Ok(())
    }
}
