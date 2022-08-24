# Singer Rust

🚧 This is a WIP and does not fully support all possible singer components yet (properties, catalog)

This is a rust SDK for the Singer ecosystem.  It is designed to minimize boilerplate required to make a fast, efficient tap or target deployable anywhere with minimal binary sizes.

Here is the complete working `target-jsonl` example, this compiles to ~1.7Mb on Mac and the throughput is significant but pending more reliable benchamrks:

```rust
use serde_json::{to_string, Value};
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use singer::messages::SingerRecord;
use singer::target::{run, SingerSink};

struct JsonLSink {
    stream: String,
    config: Value,
    target: File,
}

impl SingerSink for JsonLSink {
    // CONSTRUCTOR
    fn new(stream: String, config: Value) -> JsonLSink {
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
            target: fh,
        }
    }
    // MAIN DEVELOPER IMPL
    fn flush(&mut self, batch: &mut Vec<SingerRecord>) -> usize {
        let flush_size = batch.len();
        let mut buf = String::with_capacity(flush_size);
        for rec in batch {
            buf.push_str(&rec.record.to_string());
        }
        let mut t = self.target.try_clone().unwrap();
        t.write(buf.as_bytes()).unwrap();
        buf.clear();
        flush_size
    }
}

fn main() {
    run::<JsonLSink>()
}
```

## Usage

All that is required of the developer if to create a struct which implements `SingerSink` and to execute the `run::<YourSink>()` method somewhere in the `main` call. The struct must minimally implement these fields:

    stream: String,
    config: serde_json::Value,
