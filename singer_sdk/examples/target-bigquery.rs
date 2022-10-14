use futures::executor;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::{
    clustering::Clustering, dataset::Dataset, table::Table,
    table_data_insert_all_request::TableDataInsertAllRequest,
    table_data_insert_all_request_rows::TableDataInsertAllRequestRows,
    table_field_schema::TableFieldSchema, table_schema::TableSchema,
    time_partitioning::TimePartitioning,
};
use gcp_bigquery_client::Client;
use log::{self, debug, info};
use serde_json::{json, Value};
use singer::messages::Record;
use singer::target::{run, BaseConfiguration, Processor};
use std::env;
use std::time::Duration;

use async_compat::Compat;

struct BigQuerySink {
    stream: String,
    config: Value,
    client: Client,
}

impl Processor for BigQuerySink {
    fn new(stream: String, config: Value) -> BigQuerySink {
        let sa_json = env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap();
        let client =
            executor::block_on(Compat::new(Client::from_service_account_key_file(&sa_json)));

        let target = &stream.to_lowercase();
        let dataset = match executor::block_on(Compat::new(client.dataset().exists(
            config["project_id"].as_str().unwrap(),
            config["dataset_id"].as_str().unwrap(),
        ))) {
            Ok(true) => executor::block_on(Compat::new(client.dataset().get(
                config["project_id"].as_str().unwrap(),
                config["dataset_id"].as_str().unwrap(),
            )))
            .expect("Failed to get ref to dataset?"),
            Err(_) | Ok(false) => executor::block_on(Compat::new(
                client.dataset().create(
                    Dataset::new(
                        config["project_id"].as_str().unwrap(),
                        config["dataset_id"].as_str().unwrap(),
                    )
                    .location("US")
                    .friendly_name("Dataset created by Singer-rust"),
                ),
            ))
            .expect("Failed to make dataset?"),
        };

        match executor::block_on(Compat::new(client.table().exists(
            config["project_id"].as_str().unwrap(),
            config["dataset_id"].as_str().unwrap(),
            target,
        ))) {
            Ok(true) => executor::block_on(client.table().get(
                config["project_id"].as_str().unwrap(),
                config["dataset_id"].as_str().unwrap(),
                target,
                None,
            ))
            .unwrap(),
            Err(_) | Ok(false) => executor::block_on(Compat::new(
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
                    .time_partitioning(
                        TimePartitioning::per_day()
                            .expiration_ms(Duration::from_secs(3600 * 24 * 7))
                            .field("_sdc_batched_at"),
                    ),
                ),
            ))
            .unwrap(),
        };

        return BigQuerySink { stream, config, client };
    }

    // SCHEMALESS SINK
    fn preprocess_record(&self, mut record_message: Record) -> Record {
        let nested = json!({"data": record_message.record.to_string()});
        record_message.record = nested;
        record_message
    }

    fn process_batch(&mut self, batch: Vec<Record>) {
        let flush_size = batch.len();
        debug!("Executing flush of {:?} records...", flush_size);
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
            let resp = executor::block_on(Compat::new(self.client.tabledata().insert_all(
                &project_id.as_str().unwrap().to_owned(),
                &dataset_id.as_str().unwrap().to_owned(),
                &stream,
                &buffer,
            )));
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
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    rayon::ThreadPoolBuilder::new().num_threads(42).build_global().unwrap();
    run::<BigQuerySink>(BaseConfiguration { buffer_size: 500, add_sdc_metadata: true })?;
    Ok(())
}
