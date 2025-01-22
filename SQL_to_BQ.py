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
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Load data into BigQuery
    with open(JSON_FILE_PATH, 'r') as json_file:
        rows_to_insert = []
        for line in json_file:
            row = json.loads(line)
            renamed_row = rename_fields(row)
            rows_to_insert.append(renamed_row)
        
        # Write the list of dictionaries to a temporary JSON file
        temp_json_file_path = "/backup/dumps/temp_ti_db_inventory_daily_log.json"
        with open(temp_json_file_path, 'w') as temp_json_file:
            for row in rows_to_insert:
                json.dump(row, temp_json_file)
                temp_json_file.write('\n')  # Newline delimiter
        
        # Load data into BigQuery
        with open(temp_json_file_path, 'rb') as temp_json_file:
            job = client.load_table_from_file(
                temp_json_file, 
                table_id, 
                job_config=job_config
            )
            job.result()  # Wait for the job to complete

    if job.errors:
        print("Encountered errors while inserting rows: {}".format(job.errors))
    else:
        print("Data inserted successfully.")

def main():
    try:
        load_json_to_bigquery()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
