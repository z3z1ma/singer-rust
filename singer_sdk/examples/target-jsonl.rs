use serde_json::{to_string, Value};
use std::borrow::BorrowMut;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use log::{debug, info};
use singer::messages::SingerRecord;
use singer::target::{run, SingerSink};
use singer_scaffold::base_sink_fields;

// 50mb Memory Limit per sink
static MEGABYTE: usize = 1000000;
static MAX_BYTES: usize = 50 * MEGABYTE;

#[base_sink_fields]
struct JsonLSink {
    #[allow(unused)]
    stream: String,
    #[allow(unused)]
    config: Value,
    target: File,
    counter: usize,
    buffer: String,
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
            counter: 0,
            buffer: String::new(),
        }
    }
    // COUNTER
    fn tally_record(&mut self) -> () {
        self.counter += 1;
    }
    fn clear_tally(&mut self) -> () {
        self.counter = 0;
    }
    fn buffer_size(&self) -> usize {
        self.counter
    }
    // WRITER
    fn write(&mut self, record_message: SingerRecord) {
        debug!("{:?}: Got {:?}", self.stream, &record_message);
        // The most efficient method is to write to a string buffer
        // over a Vec<String> since we would need to reallocate
        // memory in order to `join` the strings for JSONL
        self.buffer.push_str(
            &to_string(&record_message.record).expect("Invalid RECORD message received from tap"),
        );
        self.buffer.push_str("\n");
        // Custom flush example which is enforced in *conjunction
        // with default mechanism which is based on record count
        if self.buffer.len() > MAX_BYTES {
            self.safe_flush()
        };
    }
    fn flush(&mut self) {
        info!(
            "Executing flush of {:?} records...",
            self.buffer.lines().count()
        );
        // Write to file and clear the buffer, it will maintain allocated
        // space without needing to be resized
        self.target
            .borrow_mut()
            .write(self.buffer.as_bytes())
            .unwrap();
        self.buffer.clear();
    }
}

fn main() {
    run::<JsonLSink>()
}
