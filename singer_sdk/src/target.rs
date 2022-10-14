//! Contains the methods, traits, and the entrypoint
//! for the Singer targets built by the SDK

use crate::interface::*;
use crate::messages::*;

use std::collections::HashMap;
use std::{fs, mem};

use log;
use pretty_env_logger;
use serde_json::json;
use serde_json::{self, Value};

#[derive(Clone)]
pub struct BaseConfiguration {
    pub buffer_size: usize,
    pub add_sdc_metadata: bool,
}

pub trait Processor {
    fn new(name: String, config: Value) -> Self;
    fn preprocess_record(&self, record: Record) -> Record {
        record
    }
    // No interior mutation for a processor, therefore we can wrap it
    // in an Arc and parallelize the process_batch method BUT is this really the way?
    fn process_batch(&mut self, batch: Vec<Record>);
}

/// Alias type for a batch job
type Sink<T> = (T, Vec<Record>);

/// The primary entrypoint for a Singer Target
/// This handles the main processing loop maximizing CPU utilization
/// via leveraging a work-stealing pattern and global injector queue under the hood
pub fn run<T>(base_config: BaseConfiguration) -> Result<(), Box<dyn std::error::Error>>
where
    T: Processor + Sync,
    Sink<T>: Send,
{
    pretty_env_logger::init();

    let args: SingerTarget = parse_cli_args();
    let config = parse_json_file(args.config, JsonFiles::Config)?;

    let mut streams: HashMap<String, Sink<T>> = HashMap::new();
    let mut state = json!({ "value": null });

    let singer_input = parse_singer_input(args.input.map(|path| fs::File::open(path).unwrap()));
    for message in into_singer_message_stream(singer_input) {
        match message? {
            Message::Schema(message) => {
                log::info!("Creating sink for stream {}!", &message.stream);
                let dispatcher = T::new(message.stream.clone(), config.clone());
                streams.insert(message.stream, (dispatcher, vec![]));
            }
            Message::Record(mut message) => {
                let (processor, stream) = streams
                    .get_mut(message.stream.as_str())
                    .expect("RECORD message encountered before corresponding SCHEMA message");
                message = processor.preprocess_record(message);
                if base_config.add_sdc_metadata {
                    message = add_sdc_to_record(message);
                } else {
                    message = remove_sdc_from_record(message);
                }
                stream.push(message);
                if stream.len() > base_config.buffer_size {
                    let mut batch = mem::take(stream);
                    batch = stamp_record_batch(batch);
                    processor.process_batch(batch);
                }
            }
            Message::Batch(message) => {
                log::error!("BATCH spec not implemented!");
                drop(message);
            }
            Message::State(message) => state["value"] = message.value,
        }
    }
    for (name, (mut processor, ref mut stream)) in streams {
        if stream.len() > 0 {
            log::info!("Draining leftovers from {:?}", name);
            let batch = mem::take(stream);
            processor.process_batch(batch);
        }
    }
    log::info!("STATE: {:?}", state);
    Ok(())
}
