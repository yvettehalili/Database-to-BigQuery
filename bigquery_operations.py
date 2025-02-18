#This script handles loading the data into BigQuery.

from google.cloud import bigquery
import logging
from config import schema_config, project_id, dataset_id

bq_client = bigquery.Client()

def get_schema_from_config(table_name):
    """Get BigQuery schema from JSON file"""
    if table_name not in schema_config:
        raise ValueError(f"No schema defined for table: {table_name}")

    schema = [
        bigquery.SchemaField(field["name"], field["type"])
        for field in schema_config[table_name]
    ]

    return schema

def load_to_bigquery(df, table_name, is_daily=False):
    """Load data into BigQuery"""
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig()
        job_config.schema = get_schema_from_config(table_name)
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        if table_name == 'daily_log':
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="BackupDate"
            )

        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if is_daily else bigquery.WriteDisposition.WRITE_TRUNCATE

        # Load data to BigQuery directly from DataFrame
        job = bq_client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )
        job.result()  # Wait for the job to complete

        table = bq_client.get_table(table_ref)
        logging.info(f"Successfully loaded {len(df)} rows into BigQuery table: {table_name}")
        logging.info(f"Total rows in table after load: {table.num_rows}")

    except Exception as e:
        logging.error(f"Error loading data into BigQuery table {table_name}: {e}")
        raise
