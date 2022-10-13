# Singer Rust

🚧 This is a WIP and does not fully support all possible singer components yet (properties, catalog)

This is a rust SDK for the Singer ecosystem.  It is designed to minimize boilerplate required to make a fast, efficient tap or target deployable anywhere with minimal binary sizes.

Here is the complete working `target-jsonl` example, this compiles to ~1.7Mb on Mac and the throughput is significant but pending more reliable benchamrks:

```rust
use serde_json::Value;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use singer::messages::SingerRecord;
use singer::target::{run, BaseConfiguration, Processor};

struct JsonLSink {
    fh: File,
}

impl Processor for JsonLSink {
    fn new(stream_name: String, stream_config: Value) -> Self {
        // Get path
        let fh_path = stream_config
            .get("path")
            .map(|v| v.to_string())
            .unwrap_or(String::from("./"));
        // Append /output
        let mut target_path = PathBuf::from(fh_path);
        target_path.push("output");
        // Make dir
        create_dir_all(&target_path).unwrap();
        // Name {stream}.jsonl
        target_path.push(&stream_name);
        target_path.set_extension("jsonl");
        // Make file handle
        let fh = File::options()
            .create(true)
            .append(true)
            .open(target_path)
            .unwrap();
        // Return sink
        JsonLSink { fh }
    }
    fn process_batch(&self, mut payload: Vec<SingerRecord>) -> () {
        let mut t = self.fh.try_clone().unwrap();
        t.write(
            payload
                .iter_mut()
                .map(|msg| msg.record.to_string())
                .collect::<Vec<String>>()
                .join("\n")
                .as_bytes(),
        )
        .unwrap();
        drop(payload);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BaseConfiguration {
        buffer_size: 50000,
        add_sdc_metadata: true,
    };
    run::<JsonLSink>(config)?;
    Ok(())
}
```

## Usage

All that is required of the developer if to create a struct which implements `Processor` and to execute the `run::<YourSink>(config: BaseConfiguration)` method somewhere in the `main` call. The struct must minimally implement these fields:

    stream: String,
    config: serde_json::Value,
