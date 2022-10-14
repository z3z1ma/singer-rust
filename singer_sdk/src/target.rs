//! Contains the methods, traits, and the entrypoint
//! for the Singer targets built by the SDK

use crate::interface::*;
use crate::messages::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::{fs, mem};

use crossbeam::channel;
use log;
use pretty_env_logger;
use rayon;
use rayon::prelude::*;
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
    fn process_batch(&self, batch: Vec<Record>);
}

/// Alias type for a batch job
type BatchJob<T> = (Arc<T>, Vec<Record>);

/// The primary entrypoint for a Singer Target
/// This handles the main processing loop maximizing CPU utilization
/// via leveraging a work-stealing pattern and global injector queue under the hood
pub fn run<T>(base_config: BaseConfiguration) -> Result<(), Box<dyn std::error::Error>>
where
    T: Processor + Sync,
    BatchJob<T>: Send,
{
    rayon::scope(|thread_pool| -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        pretty_env_logger::init();

        // Ingest System IDK
        let (tx, rx) = channel::bounded::<BatchJob<T>>(500);
        log::info!("Spawning batch processor iterator");
        thread_pool.spawn(|_| {
            rx.into_iter()
                .par_bridge()
                .into_par_iter()
                .for_each(|(processor, records)| processor.process_batch(records));
        });
        log::info!("Iterator spawned");

        let args: SingerTarget = parse_cli_args();
        let config = parse_json_file(args.config, JsonFiles::Config)?;

        // How and WHY do we manage ownership with regards to a sink
        // I don't think it should own anything other than supplying
        // functionality to batch a Vec of messages which it takes ownership over?
        let mut streams: HashMap<String, BatchJob<T>> = HashMap::new();
        let mut state = json!({ "value": null });

        let singer_input = parse_singer_input(args.input.map(|path| fs::File::open(path).unwrap()));
        for message in into_singer_message_stream(singer_input) {
            match message? {
                Message::Schema(message) => {
                    log::info!("Creating sink for stream {}!", message.stream);
                    streams.insert(
                        message.stream.clone(),
                        (Arc::new(T::new(message.stream, config.clone())), Vec::new()),
                    );
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
                        tx.send((processor.clone(), batch)).unwrap();
                    }
                }
                Message::Batch(message) => {
                    log::error!("BATCH spec not implemented!");
                    drop(message);
                }
                Message::State(message) => state["value"] = message.value,
            }
        }
        for (name, (processor, ref mut stream)) in streams {
            if stream.len() > 0 {
                log::info!("Draining leftovers from {:?}", name);
                let batch = mem::take(stream);
                tx.send((processor.clone(), batch)).unwrap();
            }
        }
        drop(tx);
        log::info!("STATE: {:?}", state);
        Ok(())
    })
    .unwrap();
    Ok(())
}
