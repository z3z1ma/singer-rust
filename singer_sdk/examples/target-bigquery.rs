use serde::Serialize;
use serde_json::{json, Value};

// use std::env;

use log::{self, debug, info};
use singer::messages::SingerRecord;
use singer::target::{run, SingerSink};
use singer_scaffold::base_sink_fields;

use gcp_bigquery_client::model::{
    clustering::Clustering, dataset::Dataset, table::Table,
    table_data_insert_all_request::TableDataInsertAllRequest, table_field_schema::TableFieldSchema,
    table_schema::TableSchema, time_partitioning::TimePartitioning,
};
use gcp_bigquery_client::Client;

use lazy_static::lazy_static;
use std::time::{Duration, SystemTime};
use time::{format_description, OffsetDateTime};

use crossbeam::{channel, thread};
use tokio;

#[derive(Serialize)]
struct SinkRow {
    /// Json serialized string data
    data: String,
    /// message.get("time_extracted") or now
    #[serde(skip_serializing_if = "Option::is_none")]
    _sdc_extracted_at: Option<String>,
    /// dat&etime.datetime.now().isoformat()
    _sdc_received_at: Option<String>,
    /// context.get("batch_start_time", None) or datetime.datetime.now()
    _sdc_batched_at: Option<String>,
    /// record.get("_sdc_deleted_at")
    #[serde(skip_serializing_if = "Option::is_none")]
    _sdc_deleted_at: Option<String>,
    /// int(round(time.time() * 1000))
    _sdc_sequence: Option<u64>,
    /// message.get("version")
    #[serde(skip_serializing_if = "Option::is_none")]
    _sdc_table_version: Option<u64>,
}

lazy_static! {
    static ref DATE_FMT: Vec<format_description::FormatItem<'static>> =
        format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
    static ref BATCH_DATE: String = OffsetDateTime::now_utc().format(&DATE_FMT).unwrap();
}

#[base_sink_fields]
struct BigQuerySink {
    stream: String,
    #[allow(unused)]
    config: Box<Value>,
    client: Client,
    counter: usize,
    buffer: TableDataInsertAllRequest,
    rt: tokio::runtime::Runtime,
}

impl SingerSink for BigQuerySink {
    // CONSTUCTOR
    fn new(stream: String, config: Box<Value>) -> BigQuerySink {
        // Do custom stuff
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let gcp_sa_key = config.get("credentials_path").map(|v| v.as_str().unwrap());
        let client = rt.block_on(gcp_bigquery_client::Client::from_service_account_key_file(
            gcp_sa_key.unwrap(),
        ));
        let target = &stream.to_lowercase();

        // Ensure Target Dataset Exists
        let dataset = match rt.block_on(client.dataset().exists(
            config["project_id"].as_str().unwrap(),
            config["dataset_id"].as_str().unwrap(),
        )) {
            Ok(true) => rt
                .block_on(client.dataset().get(
                    config["project_id"].as_str().unwrap(),
                    config["dataset_id"].as_str().unwrap(),
                ))
                .unwrap(),
            Err(_) | Ok(false) => rt
                .block_on(
                    client.dataset().create(
                        Dataset::new(
                            config["project_id"].as_str().unwrap(),
                            config["dataset_id"].as_str().unwrap(),
                        )
                        .location("US")
                        .friendly_name("Dataset created by Singer-rust"),
                    ),
                )
                .unwrap(),
        };

        // Ensure Target Table Exists
        match rt.block_on(client.table().exists(
            config["project_id"].as_str().unwrap(),
            config["dataset_id"].as_str().unwrap(),
            target,
        )) {
            Ok(true) => rt
                .block_on(client.table().get(
                    config["project_id"].as_str().unwrap(),
                    config["dataset_id"].as_str().unwrap(),
                    target,
                    None,
                ))
                .unwrap(),
            Err(_) | Ok(false) => rt
                .block_on(
                    dataset.create_table(
                        &client,
                        Table::from_dataset(
                            &dataset,
                            target,
                            TableSchema::new(vec![
                                TableFieldSchema::json("data"),
                                TableFieldSchema::timestamp("_sdc_extracted_at"),
                                TableFieldSchema::timestamp("_sdc_received_at"),
                                TableFieldSchema::timestamp("_sdc_batched_at"),
                                TableFieldSchema::timestamp("_sdc_deleted_at"),
                                TableFieldSchema::integer("_sdc_sequence"),
                                TableFieldSchema::integer("_sdc_table_version"),
                            ]),
                        )
                        .friendly_name(&stream)
                        .clustering(Clustering {
                            fields: Some(vec![
                                "_sdc_extracted_at".into(),
                                "_sdc_received_at".into(),
                                "_sdc_batched_at".into(),
                            ]),
                        })
                        .description(&format!(
                            "
This table is loaded via target-bigquery which is a 
Singer target that uses an unstructured load approach. 
The originating stream name is `{}`. 
This table is partitioned by _sdc_batched_at and 
clustered by related _sdc timestamp fields.",
                            target
                        ))
                        .expiration_time(SystemTime::now() + Duration::from_secs(3600))
                        .time_partitioning(
                            TimePartitioning::per_day()
                                .expiration_ms(Duration::from_secs(3600 * 24 * 7))
                                .field("_sdc_batched_at"),
                        ),
                    ),
                )
                .unwrap(),
        };

        // Prep buffer
        let buffer = TableDataInsertAllRequest::new();

        // Return your sink
        return BigQuerySink {
            stream,
            config,
            client,
            counter: 0,
            buffer,
            rt,
        };
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
    fn max_buffer_size(&self) -> usize {
        50
    }

    // WRITER
    fn add_sdc_to_record(&self, mut record_message: SingerRecord) -> SingerRecord {
        let nested = json!({"data": record_message.record});
        record_message.record = nested;
        if record_message.time_extracted.is_empty() {
            record_message.record["_sdc_extracted_at"] =
                json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap())
        } else {
            record_message.record["_sdc_extracted_at"] =
                json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap())
        }
        record_message.record["_sdc_received_at"] =
            json!(OffsetDateTime::now_utc().format(&DATE_FMT).unwrap());
        record_message.record["_sdc_batched_at"] = json!(self.get_batch_date());
        record_message.record["_sdc_sequence"] = json!(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        record_message
    }

    // Explore optimization
    // We can write every record as an API request...
    // Or we can figure out how to thread it
    // probably with mpsc? or a more basic channel? is Single-consumer a bottleneck?
    // MAYBE we spin up 4-8 threads and they all listen to one channel :) 🚀
    fn write(&mut self, record_message: SingerRecord) {
        debug!("{:?}: Got {:?}", self.stream, &record_message);
        let _ = self
            .buffer
            .add_row(
                None,
                SinkRow {
                    data: record_message.record.to_string(),
                    _sdc_extracted_at: record_message
                        .record
                        .get("_sdc_extracted_at")
                        .map(|v| v.as_str().unwrap().to_string()),
                    _sdc_received_at: record_message
                        .record
                        .get("_sdc_received_at")
                        .map(|v| v.as_str().unwrap().to_string()),
                    _sdc_batched_at: record_message
                        .record
                        .get("_sdc_batched_at")
                        .map(|v| v.as_str().unwrap().to_string()),
                    _sdc_deleted_at: record_message
                        .record
                        .get("_sdc_deleted_at")
                        .map(|v| v.as_str().unwrap().to_string()),
                    _sdc_sequence: record_message
                        .record
                        .get("_sdc_sequence")
                        .map(|v| v.as_u64().unwrap()),
                    _sdc_table_version: record_message
                        .record
                        .get("_sdc_table_version")
                        .map(|v| v.as_u64().unwrap()),
                },
            )
            .unwrap();
        // info!("{:?}", resp)
    }

    fn flush(&mut self) {
        info!("Executing flush of {:?} records...", self.buffer.len());
        // Write to BQ
        let resp = self
            .rt
            .block_on(self.client.tabledata().insert_all(
                self.config["project_id"].as_str().unwrap(),
                self.config["dataset_id"].as_str().unwrap(),
                &self.stream.to_lowercase(),
                self.buffer.clone(),
            ))
            .unwrap();
        // TODO: check for errors, move without clone
        if let Some(err) = resp.insert_errors {
            panic!("{:?}", err)
        }
        self.buffer.clear();
    }
}

fn main() {
    run::<BigQuerySink>()
}
