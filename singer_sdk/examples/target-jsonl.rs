use serde_json;
use tokio::io::AsyncWriteExt;

use std::fs::{create_dir_all, File};
use std::path::PathBuf;

use singer::error::Error;
use singer::messages::{Message, Record, SingerReceiver};
use singer::target::{run, BaseConfiguration, SdkManagedTarget};

use tokio;

struct JsonLSink {
    fh: tokio::fs::File,
    buf: Vec<u8>,
}

impl JsonLSink {
    fn process_record(&mut self, record: Record) {
        serde_json::to_writer(&mut self.buf, &record.record).unwrap();
        self.buf.push(b'\n');
        drop(record);
    }
    fn ready_to_drain(&self) -> bool {
        self.buf.len() > 1024 * 1024 * 500
    }
    fn is_empty(&self) -> bool {
        self.buf.len() == 0
    }
}

#[async_trait::async_trait]
impl SdkManagedTarget for JsonLSink {
    async fn new(stream_name: String, target_config: serde_json::Value) -> Result<Self, Error>
    where
        Self: Sized,
    {
        // Get path
        let fh_path =
            target_config.get("path").map(|v| v.to_string()).unwrap_or(String::from("./"));
        // Append /output
        let mut target_path = PathBuf::from(fh_path);
        target_path.push("output");
        // Make dir
        create_dir_all(&target_path).unwrap();
        // Name {stream}.jsonl
        target_path.push(&stream_name);
        target_path.set_extension("jsonl");
        // Make file handle
        let fh = File::options().create(true).append(true).open(target_path).unwrap();
        // Return sink
        Ok(JsonLSink {
            fh: tokio::fs::File::from_std(fh),
            buf: Vec::with_capacity(1024 * 1024 * 515),
        })
    }
    async fn listen(&mut self, mut rx: SingerReceiver) -> Result<(), Error> {
        while let Some(message) = rx.recv().await {
            match message {
                Message::Schema(message) => {
                    // Don't support schema messages on a 'no schema' target
                    drop(message);
                }
                Message::Record(message) => {
                    self.process_record(message);
                }
                _ => (),
            }
            if self.ready_to_drain() {
                ingest_batch(&mut self.fh, &mut self.buf).await;
            }
        }
        log::debug!("End of SingerInput");
        if !self.is_empty() {
            ingest_batch(&mut self.fh, &mut self.buf).await;
        }
        Ok(())
    }
}

async fn ingest_batch(fh: &mut tokio::fs::File, buf: &mut Vec<u8>) {
    fh.write_all(buf).await.unwrap();
    buf.clear();
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = BaseConfiguration { buffer_size: 150000, add_sdc_metadata: true };
    run::<JsonLSink>(config).await?;
    Ok(())
}
