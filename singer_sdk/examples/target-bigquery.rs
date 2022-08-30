use gcp_bigquery_client::error::BQError;
use serde_json::{json, Value};

use log::{self, debug, info};
use singer::messages::SingerRecord;
use singer::target::{run, SingerSink};

use gcp_bigquery_client::model::{
    clustering::Clustering, dataset::Dataset, table::Table,
    table_data_insert_all_request::TableDataInsertAllRequest,
    table_data_insert_all_request_rows::TableDataInsertAllRequestRows,
    table_field_schema::TableFieldSchema, table_schema::TableSchema,
    time_partitioning::TimePartitioning,
};
use gcp_bigquery_client::Client;

use std::env;
use std::time::Duration;

use async_trait::async_trait;
use lazy_static::lazy_static;
use tokio;
use tokio::sync::OnceCell;

lazy_static! {
    static ref GOOGLE_APPLICATION_CREDENTIALS: String =
        env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap();
    static ref ONCE: OnceCell<Client> = OnceCell::new();
}

async fn get_client() -> &'static Client {
    ONCE.get_or_init(|| async {
        Client::from_service_account_key_file(&GOOGLE_APPLICATION_CREDENTIALS).await
    })
    .await
}

#[derive(Clone)]
struct BigQuerySink {
    stream: String,
    config: Value,
}

#[async_trait]
impl SingerSink for BigQuerySink {
    // CONSTRUCTOR
    async fn new(stream: String, config: Value) -> BigQuerySink {
        // Do custom stuff
        let target = &stream.to_lowercase();
        let client = get_client().await;
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
                    // .expiration_time(SystemTime::now() + Duration::from_secs(3600))
                    .time_partitioning(
                        TimePartitioning::per_day()
                            .expiration_ms(Duration::from_secs(3600 * 24 * 7))
                            .field("_sdc_batched_at"),
                    ),
                )
                .await
                .unwrap(),
        };

        // Return your sink
        return BigQuerySink { stream, config };
    }

    // OPTIMUM BUFFER FOR STREAMING API
    fn max_buffer_size() -> usize {
        500
    }

    fn flush_chan_size() -> usize {
        10
    }

    // SCHEMALESS SINK
    fn preprocess_record(&self, mut record_message: SingerRecord) -> SingerRecord {
        let nested = json!({"data": record_message.record.to_string()});
        record_message.record = nested;
        record_message
    }

    // MAIN DEVELOPER IMPL
    async fn flush(&self, batch: Vec<SingerRecord>) -> usize {
        let flush_size = batch.len();
        debug!("Executing flush of {:?} records...", flush_size);
        // Write to BQ
        let project_id = self.config.get("project_id").unwrap();
        let dataset_id = self.config.get("dataset_id").unwrap();
        let stream = self.stream.to_lowercase();
        let mut attempts = 3;
        let mut buffer = TableDataInsertAllRequest::new();
        buffer
            .add_rows(
                batch
                    .iter()
                    .map(|row| TableDataInsertAllRequestRows {
                        insert_id: None,
                        json: row.record.clone(),
                    })
                    .collect(),
            )
            .unwrap();
        drop(batch);
        loop {
            let resp = get_client()
                .await
                .tabledata()
                .insert_all(
                    &project_id.as_str().unwrap().to_owned(),
                    &dataset_id.as_str().unwrap().to_owned(),
                    &stream,
                    &buffer,
                )
                .await;
            match resp {
                Ok(r) => {
                    if r.insert_errors.is_some() {
                        debug!("ERROR ON INSERT");
                        attempts -= 1;
                        if attempts > 0 {
                            debug!("NEED TO RETRY");
                            continue;
                        } else {
                            panic!("{:?}", r.insert_errors)
                        }
                    } else {
                        info!("> SUCCESS");
                        break;
                    }
                }
                Err(BQError::RequestError(err)) => {
                    attempts -= 1;
                    if attempts > 0 {
                        debug!("NEED TO RETRY");
                        continue;
                    } else {
                        panic!("Panicking after {} attempts. {:?}", attempts, err);
                    }
                }
                // TODO: more granular error handling scenarios?
                Err(err) => {
                    attempts -= 1;
                    debug!("GENERIC FAILURE");
                    if attempts > 0 {
                        debug!("NEED TO RETRY");
                        continue;
                    } else {
                        panic!("{:?}", err)
                    }
                }
            }
        }
        flush_size
    }
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { run::<BigQuerySink>().await })
}
