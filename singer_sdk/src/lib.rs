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

    use log::{self, debug, error, info};
    use pretty_env_logger;

    use crate::interface::*;
    use crate::messages::*;
    use std::collections::HashMap;
    use std::fs::File;
    use std::mem::swap;
    use std::process::exit;
    use std::thread::sleep;
    use std::time::Duration;

    use time::{format_description, OffsetDateTime};

    use lazy_static::lazy_static;

    use async_trait::async_trait;
    use crossbeam::{channel, thread};
    use tokio;

    lazy_static! {
        static ref DATE_FMT: Vec<format_description::FormatItem<'static>> =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
        static ref BATCH_DATE: String = OffsetDateTime::now_utc().format(&DATE_FMT).unwrap();
    }

    /// This is the primary interface defining Singer Sink behavior
    /// Our `run<Sink>` fn only cares that a sink has write, flush,
    /// and constructor behaviours
    #[async_trait]
    pub trait SingerSink: Clone {
        /// Instantiate sink and any custom data structures
        fn new(stream: String, config: Value) -> Self;

        /// The number of records buffered
        fn max_buffer_size(&self) -> usize {
            50_000
        }

        /// The number of records buffered
        fn flusher_threads() -> usize {
            10
        }

        /// Determines if we should use async flush calls
        fn is_async() -> bool {
            false
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
        fn flush(&self, mut batch: Vec<SingerRecord>) -> usize {
            let flush_size = batch.len();
            batch.clear();
            flush_size
        }

        /// This method should process a buffer of records sending it to its destination
        async fn flush_async(&self, mut batch: Vec<SingerRecord>) -> usize {
            let flush_size = batch.len();
            batch.clear();
            flush_size
        }

        /// A hook to execute actions after the end of input
        fn endofpipe(&self) -> () {
            ()
        }
    }

    pub struct SingerRunner<T: SingerSink> {
        pub sink: T,
        pub records: Vec<SingerRecord>,
        pub count: usize,
    }

    pub fn run<T: SingerSink + Send + Sync + 'static>() {
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
        let handle = tokio::runtime::Handle::current();
        thread::scope(|s| {
            // WORKER TO HANDLE FLUSHES SEQUENTIALLY
            let (tx, rx) = channel::bounded::<(T, Vec<SingerRecord>)>(T::flusher_threads());
            if !T::is_async() {
                for id in 0..T::flusher_threads() {
                    let rx_worker = rx.clone();
                    s.builder()
                        .name(format!("singer-rs-{}", id))
                        .spawn(move |_| {
                            loop {
                                if let Ok((sink, batch)) = rx_worker.try_recv() {
                                    debug!("Worker {} picked up flush!", id.clone());
                                    sink.flush(batch);
                                } else {
                                    sleep(Duration::from_millis(50));
                                }
                            }
                            // for (sink, batch) in rx_worker {}
                        })
                        .unwrap();
                    debug!("Worker {} Spawned!", id.clone());
                }
            } else {
                let rx_worker = rx.clone();
                s.spawn(|_| {
                    rx_worker
                        .into_iter()
                        .map(move |(sink, batch)| {
                            handle.spawn(async move {
                                sink.flush_async(batch).await
                                // A retry strat requires COPYing buffer in mem, not ideal
                                // let res = sink.flush_async(batch).await;
                                // if res.is_err() {
                                //     sink.flush_async(batch).await;
                                // }
                            })
                        })
                        .for_each(drop);
                });
            }
            // HANDLE STREAM
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
                                let sink = T::new(stream_name, stream_config);
                                streams.insert(
                                    schema_message.stream,
                                    SingerRunner {
                                        sink,
                                        records: Vec::with_capacity(5_000),
                                        count: 0,
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
                    Ok(SingerMessage::STATE(state_message)) => {
                        live_state.value = state_message.value
                    }
                    Err(_) => {
                        debug!("Invalid Singer message received on stdin");
                        continue;
                    }
                };
                // FLUSH
                for (stream, container) in streams.iter_mut() {
                    if container.count > container.sink.max_buffer_size() {
                        info!("Beginning flush for stream {}!", stream);
                        let sink = container.sink.clone();
                        let mut buffer = Vec::new();
                        swap(&mut buffer, &mut container.records);
                        tx.send_timeout((sink, buffer), Duration::from_secs(15))
                            .expect("CHANNEL BLOCKED");
                        container.count = 0;
                    }
                }
            }
            // CLEAN UP
            for (stream, container) in streams.iter_mut() {
                if container.count > 0 {
                    info!("Stream {:?} completed, cleaning up resources...", stream);
                    let sink = container.sink.clone();
                    let mut buffer = Vec::new();
                    swap(&mut buffer, &mut container.records);
                    tx.send((sink, buffer)).unwrap();
                    container.count = 0;
                }
            }
            drop(tx);
        })
        .unwrap();

        info!("Message Handler threads completed!");
        info!("{:?}", live_state)
    }
}
