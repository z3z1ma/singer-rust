use serde_json::Value;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use singer::messages::SingerRecord;
use singer::target::{run, SingerSink};

use async_trait::async_trait;

#[derive(Clone)]
struct JsonLSink {
    #[allow(unused)]
    stream: String,
    #[allow(unused)]
    config: Value,
    target: Arc<File>,
}

#[async_trait]
impl SingerSink for JsonLSink {
    // CONSTRUCTOR
    async fn new(stream: String, config: Value) -> JsonLSink {
        // Do custom stuff
        let fh_path = config
            .get("path")
            .map(|v| v.to_string())
            .unwrap_or(String::from("./"));
        let mut target_path = PathBuf::from(fh_path);
        target_path.push("output");
        create_dir_all(&target_path).unwrap();
        target_path.push(&stream);
        target_path.set_extension("jsonl");
        let fh = File::options()
            .create(true)
            .append(true)
            .open(target_path)
            .unwrap();

        // Return your sink
        JsonLSink {
            stream,
            config,
            target: Arc::new(fh),
        }
    }

    // CUSTOM BUFFER SIZE
    fn max_buffer_size() -> usize {
        250_000
    }

    // 2 THREADS SINCE SYSCALLS HERE ARE FAST
    fn flush_chan_size() -> usize {
        2
    }

    // MAIN DEVELOPER IMPL
    async fn flush(&self, mut batch: Vec<SingerRecord>) -> usize {
        let flush_size = batch.len();
        let mut t = self.target.try_clone().unwrap();
        t.write(
            batch
                .iter_mut()
                .map(|msg| msg.record.to_string())
                .collect::<Vec<String>>()
                .join("\n")
                .as_bytes(),
        )
        .unwrap();
        batch.clear();
        flush_size
    }
}

#[tokio::main]
async fn main() {
    run::<JsonLSink>().await;
}
