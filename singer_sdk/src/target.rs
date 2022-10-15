//! Contains the methods, traits, and the entrypoint
//! for the Singer targets built by the SDK

use crate::error::Error;
use crate::interface::*;
use crate::messages::*;

use log;
use pretty_env_logger;

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use serde_json;
use tokio::{
    sync::mpsc::{channel, unbounded_channel, UnboundedSender},
    task,
};

/// This configuration applies to all sinks within
/// the taps execution and typically involves
/// blanket config such as buffer size and sdc data
#[derive(Clone)]
pub struct BaseConfiguration {
    pub buffer_size: usize,
    pub add_sdc_metadata: bool,
}

/// The target trait must be implemented for the
/// SDK to manage creating new instances of your struct
/// and running the listen method in its own green thread
#[async_trait::async_trait]
pub trait SdkManagedTarget {
    /// This method serves as the constructor and
    /// should instatiate an instance of the struct
    /// you implement the Target trait for
    async fn new(stream_name: String, target_config: serde_json::Value) -> Result<Self, Error>
    where
        Self: Sized;
    /// The primary trait for a target to implement. The
    /// runtime handles processing a stream, creating the target,
    /// and invoking the listen method
    async fn listen(&mut self, rx: SingerReceiver) -> Result<(), Error>;
    /// This hook offers a place for developers to instrument any
    /// logic around resource clean up and termination
    async fn endofpipe() -> Result<(), Error> {
        Ok(())
    }
}

/// A stream map as we understand it is a map of a
/// stream name to a particular channel that can
/// receive Singer messages
type StreamMap = HashMap<String, SingerSender>;

/// Blocking stdin reader whose responsibility
/// is to deserialize, validate, and proxy data
/// into application memory to be used in an
/// asynchronous context.
fn process_input(input: Option<PathBuf>, stdin_tx: UnboundedSender<Message>) -> Result<(), Error> {
    let singer_input = parse_singer_input(input.map(|path| File::open(path).unwrap()));
    for message in into_singer_message_stream(singer_input) {
        stdin_tx.send(message?).unwrap();
    }
    Ok(drop(stdin_tx))
}

pub async fn run<Target>(base_config: BaseConfiguration) -> Result<(), Error>
where
    Target: SdkManagedTarget + Send,
{
    // Logging
    pretty_env_logger::init();

    // Inputs
    let args: SingerTarget = parse_cli_args();
    let config = parse_json_file(args.config, JsonFiles::Config)?;

    // IO Processor
    let (stdin_tx, mut stdin_rx) = unbounded_channel();
    let io_loop = task::spawn_blocking(move || process_input(args.input, stdin_tx));

    // Mut Vars
    let mut stream_map: StreamMap = HashMap::new();
    let mut syncing_streams: Vec<task::JoinHandle<Result<(), Error>>> = Vec::new();
    let mut state = State { value: serde_json::json!({ "value": null }) };

    // DeMUX Processor
    log::debug!("Start demux io stream");
    while let Some(message) = stdin_rx.recv().await {
        match message {
            Message::Schema(message) => {
                // Sink Creation
                match stream_map.get_mut(&message.stream) {
                    Some(stream) => {
                        log::info!("Schema change detected for stream {}!", &message.stream);
                        stream.send(Message::from(message)).await?;
                    }
                    None => {
                        log::info!("Creating sink for stream {}!", &message.stream);
                        let (sink_tx, sink_rx) = channel(500);
                        let stream_name = message.stream.clone();
                        let stream_config = config.clone();
                        let stream: task::JoinHandle<Result<(), Error>> = task::spawn(async move {
                            Target::new(stream_name, stream_config).await?.listen(sink_rx).await?;
                            Ok(())
                        });
                        // HashMap Insert
                        stream_map.insert(message.stream, sink_tx);
                        // Track Fut
                        syncing_streams.push(stream)
                    }
                }
            }
            Message::Record(mut message) => {
                // HashMap Lookup
                let stream = stream_map
                    .get_mut(&message.stream)
                    .expect("RECORD message encountered before corresponding SCHEMA message");
                // Metadata
                if base_config.add_sdc_metadata {
                    message = add_sdc_to_record(message);
                } else {
                    message = remove_sdc_from_record(message);
                }
                // Process
                stream.send(Message::from(message)).await?;
            }
            Message::Batch(message) => {
                // Awaiting Strategy
                log::error!("BATCH spec not implemented!");
                drop(message);
            }
            Message::State(message) => {
                // Update State
                state = message;
            }
        }
    }

    // End of Pipe
    log::debug!("End demux io stream, end of pipe");
    io_loop.await.unwrap()?;
    for (ref stream, tx) in stream_map {
        log::debug!("Closing stream {stream}");
        drop(tx);
    }
    for stream in syncing_streams {
        stream.await??;
    }
    Target::endofpipe().await?;

    // State
    log::info!("STATE: {:?}", state);
    Ok(())
}
