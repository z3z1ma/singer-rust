use serde_json::Value;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use singer::messages::Record;
use singer::target::{run, BaseConfiguration, Processor};

struct JsonLSink {
    fh: File,
}

impl Processor for JsonLSink {
    fn new(stream_name: String, stream_config: Value) -> Self {
        // Get path
        let fh_path =
            stream_config.get("path").map(|v| v.to_string()).unwrap_or(String::from("./"));
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
        JsonLSink { fh }
    }
    fn process_batch(&self, mut payload: Vec<Record>) -> () {
        let mut t = &self.fh;
        t.write_all(
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
    rayon::ThreadPoolBuilder::new().num_threads(2).build_global().unwrap();
    let config = BaseConfiguration { buffer_size: 100, add_sdc_metadata: true };
    run::<JsonLSink>(config)?;
    Ok(())
}
