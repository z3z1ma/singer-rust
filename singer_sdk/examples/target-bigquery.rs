use serde::Serialize;
use serde_json::{json, Value};

// use std::env;
use tokio;

use log::{self, info};
use singer::messages::SingerRecord;
use singer::target::{run, SingerSink};

use gcp_bigquery_client::model::{
    clustering::Clustering, dataset::Dataset, table::Table,
    table_data_insert_all_request::TableDataInsertAllRequest, table_field_schema::TableFieldSchema,
    table_schema::TableSchema, time_partitioning::TimePartitioning,
};
use gcp_bigquery_client::Client;

use std::borrow::Borrow;
use std::time::{Duration, SystemTime};
use time::{format_description, OffsetDateTime};

use lazy_static::lazy_static;

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

struct BigQuerySink {
    stream: String,
    #[allow(unused)]
    config: Value,
    client: Client,
    buffer: TableDataInsertAllRequest,
}

impl SingerSink for BigQuerySink {
    // CONSTUCTOR
    fn new(stream: String, config: Value) -> BigQuerySink {
        // Do custom stuff
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let gcp_sa_key = config.get("credentials_path").map(|v| v.as_str().unwrap());
                let client =
                    gcp_bigquery_client::Client::from_service_account_key_file(gcp_sa_key.unwrap())
                        .await;
                let target = &stream.to_lowercase();

                // Ensure Target Dataset Exists
                let dataset = match client
                    .dataset()
                    .exists(
                        config["project_id"].as_str().unwrap(),
                        config["dataset_id"].as_str().unwrap(),
                    )
                    .await
                {
                    Ok(true) => client
                        .dataset()
                        .get(
                            config["project_id"].as_str().unwrap(),
                            config["dataset_id"].as_str().unwrap(),
                        )
                        .await
                        .unwrap(),
                    Err(_) | Ok(false) => client
                        .dataset()
                        .create(
                            Dataset::new(
                                config["project_id"].as_str().unwrap(),
                                config["dataset_id"].as_str().unwrap(),
                            )
                            .location("US")
                            .friendly_name("Dataset created by Singer-rust"),
                        )
                        .await
                        .unwrap(),
                };

                // Ensure Target Table Exists
                match client
                    .table()
                    .exists(
                        config["project_id"].as_str().unwrap(),
                        config["dataset_id"].as_str().unwrap(),
                        target,
                    )
                    .await
                {
                    Ok(true) => client
                        .table()
                        .get(
                            config["project_id"].as_str().unwrap(),
                            config["dataset_id"].as_str().unwrap(),
                            target,
                            None,
                        )
                        .await
                        .unwrap(),
                    Err(_) | Ok(false) => dataset
                        .create_table(
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
                        )
                        .await
                        .unwrap(),
                };

                // Prep buffer
                let buffer = TableDataInsertAllRequest::new();

                // Return your sink
                return BigQuerySink {
                    stream,
                    config,
                    client,
                    buffer,
                };
            })
    }

    fn max_buffer_size(&self) -> usize {
        500
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

    fn flush(&mut self, batch: &mut Vec<SingerRecord>) -> usize {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let flush_size = batch.len();
                info!("Executing flush of {:?} records...", flush_size);
                // Write to BQ
                let project_id = self.config.get("project_id").unwrap().clone();
                let dataset_id = self.config.get("dataset_id").unwrap().clone();
                let stream = self.stream.to_lowercase();
                let mut buffer = self.buffer.clone();
                for row in batch {
                    buffer
                        .add_row(
                            None,
                            SinkRow {
                                data: row.record.to_string(),
                                _sdc_extracted_at: row
                                    .record
                                    .get("_sdc_extracted_at")
                                    .map(|v| v.as_str().unwrap().to_string()),
                                _sdc_received_at: row
                                    .record
                                    .get("_sdc_received_at")
                                    .map(|v| v.as_str().unwrap().to_string()),
                                _sdc_batched_at: row
                                    .record
                                    .get("_sdc_batched_at")
                                    .map(|v| v.as_str().unwrap().to_string()),
                                _sdc_deleted_at: row
                                    .record
                                    .get("_sdc_deleted_at")
                                    .map(|v| v.as_str().unwrap().to_string()),
                                _sdc_sequence: row
                                    .record
                                    .get("_sdc_sequence")
                                    .map(|v| v.as_u64().unwrap()),
                                _sdc_table_version: row
                                    .record
                                    .get("_sdc_table_version")
                                    .map(|v| v.as_u64().unwrap()),
                            },
                        )
                        .unwrap()
                }
                let resp = self
                    .client
                    .tabledata()
                    .insert_all(
                        project_id.as_str().unwrap(),
                        dataset_id.as_str().unwrap(),
                        stream.borrow(),
                        buffer,
                    )
                    .await
                    .unwrap();
                // TODO: check for errors, move without clone
                if let Some(err) = resp.insert_errors {
                    panic!("{:?}", err)
                };
                flush_size
            })
    }
}

fn main() {
    run::<BigQuerySink>()
}
