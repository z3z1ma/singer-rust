/// Contains definitions of messages within the Singer specification
pub mod messages {
    use serde::{self, Deserialize, Serialize};
    use serde_json::{self, Value};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerSchema {
        pub stream: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerRecord {
        pub stream: String,
        pub record: Value,
        pub time_extracted: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerBatch {
        pub stream: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerState {
        pub value: Value,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub enum SingerMessage {
        RECORD(SingerRecord),
        STATE(SingerState),
        SCHEMA(SingerSchema),
        BATCH(SingerBatch),
    }
}

/// Contains Singer CLI implementation
mod interface {
    use clap::{self, Parser};
    use std::path::PathBuf;

    /// This struct defines all arguments which Singer CLI programs accept
    #[derive(Parser, Debug)]
    #[clap(author="z3z1ma", version, about="Singer target SDK", long_about = None)]
    pub struct SingerArgs {
        /// Configuration file location or 'ENV' to use environment variables
        #[clap(short, long, value_parser)]
        pub config: PathBuf,

        /// Run in discovery mode
        #[clap(
            short,
            long,
            value_parser,
            default_value_t = false,
            conflicts_with = "about"
        )]
        pub discover: bool,

        /// State file location
        #[clap(short, long, value_parser)]
        pub state: Option<PathBuf>,

        /// Dump plugin details to stdout
        #[clap(short, long, value_parser, default_value_t = false)]
        pub about: bool,
    }

    impl SingerArgs {
        pub fn parse_args() -> Self {
            SingerArgs::parse()
        }
    }
}

/// The SDK for creating targets with minimum boilerplate code
/// # Example Target
/// ```
/// #[base_sink_fields]
/// struct JsonLSink {
///     stream: String,
///     config: Box<Value>,
///     target: File,
///     counter: usize,
///     buffer: String,
/// }
///
/// impl SingerSink for JsonLSink {
///     fn new(stream: String, config: Box<Value>) -> JsonLSink {
///         let fh_path = config
///             .get("path")
///             .map(|v| v.to_string())
///             .unwrap_or(String::from("./"));
///         let mut target_path = PathBuf::from(fh_path);
///         target_path.push("output");
///         create_dir_all(&target_path).unwrap();
///         target_path.push(&stream);
///         target_path.set_extension("jsonl");
///         let fh = File::options()
///             .create(true)
///             .append(true)
///             .open(target_path)
///             .unwrap();
///         JsonLSink {
///             stream,
///             config,
///             target: fh,
///             counter: 0,
///             buffer: String::new(),
///         }
///     }
///     fn tally_record(&mut self) -> () {
///         self.counter += 1;
///     }
///     fn clear_tally(&mut self) -> () {
///         self.counter = 0;
///     }
///     fn buffer_size(&self) -> usize {
///         self.counter
///     }
///     fn write(&mut self, record_message: SingerRecord) {
///         debug!("{:?}: Got {:?}", self.stream, &record_message);
///         // The most efficient method is to write to a string buffer
///         // over a Vec<String> since we would need to reallocate
///         // memory in order to `join` the strings for JSONL
///         self.buffer.push_str(
///             &to_string(&record_message.record).expect("Invalid RECORD message received from tap"),
///         );
///         self.buffer.push_str("\n");
///         // Custom flush example which is enforced in *conjunction
///         // with default mechanism which is based on record count
///         if self.buffer.len() > MAX_BYTES {
///             self.safe_flush()
///         };
///     }
///     fn flush(&mut self) {
///         info!(
///             "Executing flush of {:?} records...",
///             self.buffer.lines().count()
///         );
///         // Write to file and clear the buffer, it will maintain allocated
///         // space without needing to be resized
///         self.target.borrow_mut().write(self.buffer.as_bytes()).unwrap();
///         self.buffer.clear();
///     }
/// }
///
/// fn main() {
///     run::<JsonLSink>()
/// }
/// ```
pub mod target {
    use serde_json::{self, json, Value};

    use log::{self, debug, error, info};
    use pretty_env_logger;

    use crate::interface::*;
    use crate::messages::*;
    use std::collections::HashMap;
    use std::fs::File;
    use std::process::exit;

    use time::{format_description, OffsetDateTime};

    use lazy_static::lazy_static;

    use crossbeam::thread;

    lazy_static! {
        static ref DATE_FMT: Vec<format_description::FormatItem<'static>> =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
        static ref BATCH_DATE: String = OffsetDateTime::now_utc().format(&DATE_FMT).unwrap();
    }

    /// This is the primary interface defining Singer Sink behavior
    /// Our `run<Sink>` fn only cares that a sink has write, flush,
    /// and constructor behaviours
    pub trait SingerSink {
        /// Instantiate sink and any custom data structures
        fn new(stream: String, config: Value) -> Self;

        /// The number of records buffered
        fn max_buffer_size(&self) -> usize {
            50_000
        }

        /// Get batch date string
        fn get_batch_date(&self) -> String {
            BATCH_DATE.to_string()
        }

        /// Set batch date string
        fn set_batch_date(&self) -> () {
            let _ = OffsetDateTime::now_utc().format(&DATE_FMT).unwrap();
        }

        /// A hook to preprocess a record message
        #[allow(unused_mut)]
        fn before_write(&self, mut record_message: SingerRecord) -> SingerRecord {
            record_message
        }

        /// Flag to detemrine if Singer Data Capture metadata should be added to record
        fn add_sdc_metadata(&self) -> bool {
            true
        }

        /// Add Singer Data Capture metadata to a record
        fn add_sdc_to_record(&self, mut record_message: SingerRecord) -> SingerRecord {
            // _sdc_extracted_at [Implemented]
            if record_message.time_extracted.is_empty() {
                record_message.record["_sdc_extracted_at"] =
                    json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap())
            } else {
                record_message.record["_sdc_extracted_at"] =
                    json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap())
            }
            // _sdc_received_at [Implemented]
            record_message.record["_sdc_received_at"] =
                json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap());
            // _sdc_batched_at [Implemented]
            record_message.record["_sdc_batched_at"] = json!(self.get_batch_date());
            // _sdc_deleted_at [Implemented by virtue of presence in record]
            // _sdc_sequence [Implemented]
            record_message.record["_sdc_sequence"] = json!(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs());
            // _sdc_table_version [Implemented by virtue of presence in record]
            // Output
            record_message
        }

        /// This method should process a buffer of records sending it to its destination
        fn flush(&mut self, batch: &mut Vec<SingerRecord>) -> usize {
            let flush_size = batch.len();
            batch.clear();
            flush_size
        }

        /// A hook to execute actions after the end of input
        fn endofpipe(&mut self) -> () {
            ()
        }
    }

    pub struct SingerRunner<T: SingerSink> {
        pub sink: T,
        pub records: Vec<SingerRecord>,
    }

    pub fn run<T: SingerSink + Send>() {
        pretty_env_logger::init();

        debug!("Parsing CLI arguments");
        let args = SingerArgs::parse_args();
        debug!("Parsed, got {:?}", args);

        debug!("Checking if config file exists");
        let config_path = args.config;
        if !config_path.exists()
            || !config_path.is_file()
            || config_path.extension().unwrap() != "json"
        {
            error!("Invalid config file passed into --config");
            exit(1);
        }

        let config_file = File::open(config_path).unwrap();
        let config: Value = serde_json::from_reader(config_file).unwrap();
        info!("{}", config);

        let mut live_state: Box<SingerState> = Box::new(SingerState { value: json!({}) });
        let mut streams: HashMap<String, SingerRunner<T>> = HashMap::new();

        let stdin = std::io::stdin();
        let stdin = stdin.lock();
        let singer_parser = serde_json::Deserializer::from_reader(stdin);
        let singer_messages = singer_parser.into_iter::<SingerMessage>();
        for message in singer_messages {
            match message {
                Ok(SingerMessage::SCHEMA(schema_message)) => {
                    let this = schema_message.stream.clone();
                    match streams.get(&this) {
                        Some(_) => (), // Stream Exists, mutate schema... (add trait fn for schema_change)
                        None => {
                            info!("Creating sink for stream {}!", schema_message.stream);
                            // Gather sink-specific config, stream name, catalog
                            let stream_config = config.clone();
                            let stream_name = schema_message.stream.clone();
                            // Run T::new(), this runs user specific instantiation code
                            let sink = T::new(stream_name, stream_config);
                            // Push to HashMap
                            streams.insert(
                                schema_message.stream,
                                SingerRunner {
                                    sink,
                                    records: Vec::with_capacity(5_000),
                                },
                            );
                        }
                    }
                }
                Ok(SingerMessage::RECORD(record_message)) => {
                    match streams.get_mut(&record_message.stream) {
                        Some(stream) => {
                            stream.records.push(stream.sink.before_write(
                                match stream.sink.add_sdc_metadata() {
                                    true => stream.sink.add_sdc_to_record(record_message),
                                    false => record_message,
                                },
                            ));
                        }
                        None => panic!(
                            "Record for stream {} seen before SCHEMA message",
                            record_message.stream
                        ),
                    }
                }
                Ok(SingerMessage::BATCH(batch_message)) => match streams.get(&batch_message.stream)
                {
                    Some(_) => (),
                    None => panic!(
                        "Record for stream {} seen before SCHEMA message",
                        batch_message.stream
                    ),
                },
                Ok(SingerMessage::STATE(state_message)) => live_state.value = state_message.value,
                Err(_) => {
                    debug!("Invalid Singer message received on stdin");
                    continue;
                }
            };
            // FLUSH
            thread::scope(|s| {
                for (stream, container) in streams.iter_mut() {
                    if container.records.len() > container.sink.max_buffer_size() {
                        info!("Beginning flush for stream {}!", stream);
                        s.spawn(move |_| {
                            let buf: &mut Vec<SingerRecord> = container.records.as_mut();
                            container.sink.flush(buf);
                            container.records.clear();
                        });
                    }
                }
            })
            .unwrap();
        }

        // FLUSH
        thread::scope(|s| {
            for (stream, container) in streams.iter_mut() {
                if container.records.len() > 0 {
                    info!("Stream {:?} completed, cleaning up resources...", stream);
                    s.spawn(move |_| {
                        let buf: &mut Vec<SingerRecord> = container.records.as_mut();
                        container.sink.flush(buf);
                        container.records.clear();
                    });
                }
            }
        })
        .unwrap();

        info!("Message Handler threads completed!");
        info!("{:?}", live_state)
    }
}
