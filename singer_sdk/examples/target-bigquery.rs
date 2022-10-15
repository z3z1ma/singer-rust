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
use singer::error::Error;
use singer::messages::{Message, SingerReceiver};
use singer::target::{run, BaseConfiguration, SdkManagedTarget};
use std::env;
use std::time::Duration;

struct TargetBigquery {
    stream: String,
    config: Value,
    client: Client,
    rows: Vec<TableDataInsertAllRequestRows>,
}

#[async_trait::async_trait]
impl SdkManagedTarget for TargetBigquery {
    async fn new(stream: String, config: Value) -> Result<Self, Error> {
        let sa_json = env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap();
        let client = Client::from_service_account_key_file(&sa_json).await;

        let target = &stream.to_lowercase();
        let dataset = match client
            .dataset()
            .exists(config["project_id"].as_str().unwrap(), config["dataset_id"].as_str().unwrap())
            .await
        {
            Ok(true) => client
                .dataset()
                .get(config["project_id"].as_str().unwrap(), config["dataset_id"].as_str().unwrap())
                .await
                .expect("Failed to get ref to dataset?"),
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
                .expect("Failed to make dataset?"),
        };

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
                    .time_partitioning(
                        TimePartitioning::per_day()
                            .expiration_ms(Duration::from_secs(3600 * 24 * 7))
                            .field("_sdc_batched_at"),
                    ),
                )
                .await
                .unwrap(),
        };

        return Ok(TargetBigquery { stream, config, client, rows: Vec::with_capacity(500) });
    }

    async fn listen(&mut self, mut rx: SingerReceiver) -> Result<(), Error> {
        let mut futs = Vec::new();
        while let Some(message) = rx.recv().await {
            match message {
                Message::Record(mut record) => {
                    let nested = json!({"data": record.record.to_string()});
                    record.record = nested;
                    self.rows.push(TableDataInsertAllRequestRows {
                        insert_id: None,
                        json: record.record,
                    });
                }
                _ => continue,
            }
            if self.rows.len() >= 500 {
                let batch = std::mem::take(&mut self.rows);
                let stream = self.stream.to_lowercase();
                let project_id = self.config["project_id"].as_str().unwrap().to_owned();
                let dataset_id = self.config["dataset_id"].as_str().unwrap().to_owned();
                let c = self.client.clone();
                futs.push(tokio::spawn(async {
                    process_batch(batch, project_id, dataset_id, stream, c).await;
                }));
            }
        }
        log::debug!("End of SingerInput");
        if !self.rows.is_empty() {
            let batch = std::mem::take(&mut self.rows);
            let stream = self.stream.to_lowercase();
            let project_id = self.config["project_id"].as_str().unwrap().to_owned();
            let dataset_id = self.config["dataset_id"].as_str().unwrap().to_owned();
            let c = self.client.clone();
            futs.push(tokio::spawn(async {
                process_batch(batch, project_id, dataset_id, stream, c).await;
            }));
        }
        for fut in futs {
            fut.await.unwrap();
        }
        Ok(())
    }
}

async fn process_batch(
    rows: Vec<TableDataInsertAllRequestRows>,
    project_id: String,
    dataset_id: String,
    stream: String,
    client: Client,
) {
    let flush_size = rows.len();
    debug!("Executing flush of {:?} records...", flush_size);
    let mut attempts = 3;
    let mut buffer = TableDataInsertAllRequest::new();
    buffer.add_rows(rows).unwrap();
    loop {
        let resp = client.tabledata().insert_all(&project_id, &dataset_id, &stream, &buffer).await;
        match resp {
            Ok(r) => {
                if r.insert_errors.is_some() {
                    debug!("Error on insert, retrying");
                    attempts -= 1;
                    if attempts > 0 {
                        continue;
                    } else {
                        panic!("{:?}", r.insert_errors)
                    }
                } else {
                    break;
                }
            }
            Err(BQError::RequestError(err)) => {
                attempts -= 1;
                if attempts > 0 {
                    continue;
                } else {
                    panic!("Panicking after {} attempts. {:?}", attempts, err);
                }
            }
            Err(err) => {
                attempts -= 1;
                if attempts > 0 {
                    continue;
                } else {
                    panic!("{:?}", err)
                }
            }
        }
    }
    info!("Inserted rows for {project_id}.{dataset_id}.{stream}");
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = BaseConfiguration { buffer_size: 1, add_sdc_metadata: true };
    run::<TargetBigquery>(config).await?;
    Ok(())
}
