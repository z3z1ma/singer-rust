# Singer Rust

🚧 This is a WIP and does not fully support all possible singer components yet (properties, catalog)

This is a rust SDK for the Singer ecosystem.  It is designed to minimize boilerplate required to make a fast, efficient tap or target deployable anywhere with minimal binary sizes.

Here is the complete working `target-jsonl` example, this compiles to ~1.7Mb on Mac and the throughput is significant but pending more reliable benchamrks:

```rust
use std::fs::{create_dir_all, File};
use std::path::PathBuf;

use serde_json;
use tokio;
use tokio::io::AsyncWriteExt;

use singer::error::Error;
use singer::messages::{Message, Record, SingerReceiver};
use singer::target::{run, BaseConfiguration, SdkManagedTarget};


struct TargetJsonL {
    fh: tokio::fs::File,
    buf: Vec<u8>,
}

impl TargetJsonL {
    fn process_record(&mut self, record: Record) {
        serde_json::to_writer(&mut self.buf, &record.record).unwrap();
        self.buf.push(b'\n');
    }
    fn ready_to_drain(&self) -> bool {
        self.buf.len() > 1024 * 1024 * 255
    }
    fn is_empty(&self) -> bool {
        self.buf.len() == 0
    }
}

#[async_trait::async_trait]
impl SdkManagedTarget for TargetJsonL {
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
        Ok(TargetJsonL {
            fh: tokio::fs::File::from_std(fh),
            buf: Vec::with_capacity(1024 * 1024 * 265),
        })
    }
    async fn listen(&mut self, mut rx: SingerReceiver) -> Result<(), Error> {
        while let Some(message) = rx.recv().await {
            match message {
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
    let config = BaseConfiguration { buffer_size: 1, add_sdc_metadata: true };
    run::<TargetJsonL>(config).await?;
    Ok(())
}
```

## Usage

All that is required of the developer if to create a struct which minimally implements `SdkManagedTarget` and to execute the `run::<TargetSalesforce>(config: BaseConfiguration)` method somewhere in the `main` call. 

The `new` method serves as a constructor on first schema message received for a particular stream, the `listen` method receives a stream of singer messages. You can implement logic directly in the listen method and generate a complete solution or you can break out different function calls, lazy statics, etc. The run function effectively demultiplexes the input stream, parses args + config, runs constructors, and offers a simpler interface. The end goal is to supply the developer with all the tools to stand up a tap or target in rust in a day.
