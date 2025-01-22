import os
import json
from google.cloud import bigquery

# Set up variables
KEY_FILE = '/root/jsonfiles/ti-dba-prod-01.json'
BIGQUERY_PROJECT = 'ti-dba-prod-01'
BIGQUERY_DATASET = 'ti_db_inventory'
BIGQUERY_TABLE = 'daily_log'  # Name of the BigQuery table
JSON_FILE_PATH = "/backup/dumps/ti_db_inventory_daily_log_01222025.json"

def rename_fields(row):
    return {
        "ID": row["ID"],
        "BackupDate": row["backup_date"],
        "Server": row["server"],
        "Database": row["database"],
        "Size": row["size"],
        "State": row["state"],
        "LastUpdate": row["last_update"]
    }

def load_json_to_bigquery():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE
    client = bigquery.Client()

    # Define BigQuery table structure (schema)
    table_id = f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}'
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ID", "INTEGER"),
            bigquery.SchemaField("BackupDate", "TIMESTAMP"),
            bigquery.SchemaField("Server", "STRING"),
            bigquery.SchemaField("Database", "STRING"),
            bigquery.SchemaField("Size", "INTEGER"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("LastUpdate", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Load data into BigQuery
    with open(JSON_FILE_PATH, 'r') as json_file:
        rows_to_insert = []
        for line in json_file:
            row = json.loads(line)
            renamed_row = rename_fields(row)
            rows_to_insert.append(renamed_row)
        
        # Load data into BigQuery
        errors = client.insert_rows_json(
            table_id, 
            rows_to_insert, 
            row_ids=[None] * len(rows_to_insert), 
            retry=None
        )
        
        if errors:
            print("Encountered errors while inserting rows: {}".format(errors))
        else:
            print("Data inserted successfully.")

def main():
    try:
        load_json_to_bigquery()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
