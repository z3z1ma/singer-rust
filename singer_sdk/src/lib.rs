/// Contains definitions of messages within the Singer specification
pub mod messages {
    use serde::{self, Deserialize, Serialize};
    use serde_json::{self, json, Value};
    use time::{format_description, OffsetDateTime};

    use lazy_static::lazy_static;

    lazy_static! {
        static ref DATE_FMT: Vec<format_description::FormatItem<'static>> =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
        static ref BATCH_DATE: String = OffsetDateTime::now_utc().format(&DATE_FMT).unwrap();
    }

    /// This provides a default for serde json when serializing RECORD messages
    fn get_time() -> String {
        OffsetDateTime::now_utc().format(&DATE_FMT).unwrap()
    }

    /// Get batch date as static batch start time which can be used in conjunction
    /// with SDC sequence to work with streams post-load
    fn get_batch_date() -> String {
        BATCH_DATE.to_string()
    }

    /// Add Singer Data Capture metadata to a record
    pub fn add_sdc_to_record(mut record_message: SingerRecord) -> SingerRecord {
        // _sdc_received_at [Implemented]
        record_message.record["_sdc_received_at"] =
            json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap());
        // _sdc_extracted_at [Implemented]
        if record_message.time_extracted.is_empty() {
            record_message.record["_sdc_extracted_at"] =
                record_message.record["_sdc_received_at"].clone();
        } else {
            record_message.record["_sdc_extracted_at"] = json!(record_message.time_extracted);
        }
        // _sdc_batched_at [Implemented]
        record_message.record["_sdc_batched_at"] = json!(get_batch_date());
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerSchema {
        pub stream: String,
        #[serde(default)]
        pub key_properties: Vec<String>,
        pub schema: Value,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SingerRecord {
        pub stream: String,
        pub record: Value,
        #[serde(default = "get_time")]
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

        /// Catalog file location
        #[clap(long, value_parser)]
        pub catalog: Option<PathBuf>,

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
/// struct JsonLSink {
///     stream: String,
///     config: Value,
///     target: File,
/// }
///
/// impl SingerSink for JsonLSink {
///     fn new(stream: String, config: Value) -> JsonLSink {
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
///         }
///     }
///     fn flush(&mut self, batch: &mut Vec<SingerRecord>) -> usize {
///         let flush_size = batch.len();
///         let mut buf = String::with_capacity(flush_size);
///         for rec in batch {
///             buf.push_str(&rec.record.to_string());
///         }
///         let mut t = self.target.try_clone().unwrap();
///         t.write(buf.as_bytes()).unwrap();
///         buf.clear();
///         flush_size
///     }
/// }
///
/// fn main() {
///     run::<JsonLSink>()
/// }
/// ```
pub mod target {
    use serde_json::{self, json, Value};

    use futures::stream::StreamExt;
    use log::{self, debug, error, info};
    use pretty_env_logger;
    use tokio::time::interval;

    use crate::interface::*;
    use crate::messages::*;
    use std::collections::HashMap;
    use std::fs::File;
    use std::mem::swap;
    use std::process::exit;
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio;

    /// This is the primary interface defining Singer Sink behavior
    /// Our `run<Sink>` fn only cares that a sink has flush,
    /// and constructor behaviours
    #[async_trait]
    pub trait SingerSink: Clone {
        /// Instantiate sink and any custom data structures
        async fn new(stream: String, config: Value) -> Self;

        /// The number of records buffered
        fn max_buffer_size() -> usize {
            50_000
        }

        /// The max size of the channel before incoming messages yield to the
        /// runtime. This allow the developer to adjust backpressure
        fn flush_chan_size() -> usize {
            50
        }

        /// A hook to preprocess a record message
        #[allow(unused_mut)]
        fn preprocess_record(&self, mut record_message: SingerRecord) -> SingerRecord {
            record_message
        }

        /// Flag to detemrine if Singer Data Capture metadata should be added to record
        fn add_sdc_metadata(&self) -> bool {
            true
        }

        /// This method should process a buffer of records sending it to its destination
        async fn flush(&self, mut batch: Vec<SingerRecord>) -> usize {
            let flush_size = batch.len();
            batch.clear();
            flush_size
        }

        /// A hook to execute actions after the end of input
        async fn endofpipe(&self) -> () {
            () // noop by default
        }
    }

    /// A container which wraps a sink which is essentially stream-scoped immutable configuration,
    /// a mutable vector of records, and a usize count of that vector's elements
    pub struct SingerRunner<T: SingerSink> {
        pub sink: T,
        pub records: Vec<SingerRecord>,
        pub count: usize,
    }

    pub async fn run<T: SingerSink + Send + Sync + 'static>() {
        // Init logger
        pretty_env_logger::init();

        // Validate Singer Tap/Target Args
        debug!("Parsing CLI arguments");
        let args = SingerArgs::parse_args();
        debug!("Parsed, got {:?}", args);

        // Validate Config Arg
        debug!("Checking if config file exists");
        let config_path = args.config;
        if !config_path.exists()
            || !config_path.is_file()
            || config_path.extension().unwrap() != "json"
        {
            error!("Invalid config file passed into --config");
            exit(1);
        }

        // Config Parsing
        let config_file = File::open(config_path).unwrap();
        let config: Value = serde_json::from_reader(config_file).unwrap();
        info!("{}", config);

        // HashMap + State
        let mut live_state: Box<SingerState> = Box::new(SingerState { value: json!({}) });
        let mut streams: HashMap<String, SingerRunner<T>> = HashMap::new();

        // Singer message deserializer with lazy stdin iter
        let stdin = std::io::stdin();
        let stdin = stdin.lock();
        let singer_parser = serde_json::Deserializer::from_reader(stdin);
        let singer_messages = singer_parser.into_iter::<SingerMessage>();

        // Async runtime handler
        let handle = tokio::runtime::Handle::current();

        // Async channels for message passing
        let (tx, rx) = flume::bounded::<(T, Vec<SingerRecord>)>(T::flush_chan_size());

        // Efficient async dispatcher implementation
        let rx_dispatch = rx.clone();
        let dispatcher = handle.spawn(async move {
            rx_dispatch
                .stream()
                .for_each_concurrent(T::flush_chan_size(), |(sink, batch)| async move {
                    sink.flush(batch).await;
                })
                .await;
        });

        // Singer compliant stream handler
        for message in singer_messages {
            match message {
                Ok(SingerMessage::SCHEMA(schema_message)) => {
                    let this = schema_message.stream.clone();
                    match streams.get(&this) {
                        Some(_) => (), // Stream Exists, mutate schema... (add trait fn for schema_change)
                        None => {
                            info!("Creating sink for stream {}!", schema_message.stream);
                            let stream_config = config.clone();
                            let stream_name = schema_message.stream.clone();
                            let sink = T::new(stream_name, stream_config).await;
                            streams.insert(
                                schema_message.stream,
                                SingerRunner {
                                    sink,
                                    records: Vec::with_capacity(T::max_buffer_size()),
                                    count: 0,
                                },
                            );
                        }
                    }
                }
                Ok(SingerMessage::RECORD(record_message)) => {
                    match streams.get_mut(&record_message.stream) {
                        Some(stream) => {
                            stream.records.push(match stream.sink.add_sdc_metadata() {
                                true => {
                                    add_sdc_to_record(stream.sink.preprocess_record(record_message))
                                }
                                false => stream.sink.preprocess_record(record_message),
                            });
                            stream.count += 1;
                        }
                        None => panic!(
                            "Record for stream {} seen before SCHEMA message",
                            record_message.stream
                        ),
                    }
                }
                Ok(SingerMessage::BATCH(batch_message)) => {
                    match streams.get(&batch_message.stream) {
                        Some(_) => (),
                        None => panic!(
                            "Record for stream {} seen before SCHEMA message",
                            batch_message.stream
                        ),
                    }
                }
                Ok(SingerMessage::STATE(state_message)) => live_state.value = state_message.value,
                Err(_) => {
                    debug!("Invalid Singer message received on stdin");
                    continue;
                }
            };

            // Flush sinks based on usize'ed capacity
            for (stream, container) in streams.iter_mut() {
                if container.count > T::max_buffer_size() {
                    debug!("Beginning flush for stream {}!", stream);
                    let sink = container.sink.clone();
                    let mut buffer = Vec::new();
                    swap(&mut buffer, &mut container.records);
                    container.count = 0;
                    tx.send_async((sink, buffer)).await.unwrap();
                }
            }
        }

        // Clean up non-empty Vecs
        for (stream, container) in streams.iter_mut() {
            if container.count > 0 {
                info!("Stream {:?} completed, cleaning up resources...", stream);
                let sink = container.sink.clone();
                let mut buffer = Vec::new();
                swap(&mut buffer, &mut container.records);
                container.count = 0;
                tx.send_async((sink, buffer)).await.unwrap();
            }
            container.sink.endofpipe().await;
            debug!("FLUSHED + EOF");
        }

        // Await queue to be cleared
        debug!("WAITING FOR RX TO BE EMPTY");
        let mut delay = interval(Duration::from_secs(1));
        while !rx.is_empty() {
            delay.tick().await;
        }
        // Drop sender which will signal our dispatcher thread to wrap-up
        drop(tx);
        debug!("QUEUE DROPPED - WAITING FOR THREAD TO JOIN");
        // This wrap-up ensures all futures in flight complete
        dispatcher.await.unwrap();

        // Output success + state message
        info!("Message Handler threads completed!");
        info!("{:?}", live_state)
    }
}
