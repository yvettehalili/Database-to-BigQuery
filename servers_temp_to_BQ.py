import mysql.connector
import json
import os
from google.cloud import bigquery
from datetime import datetime

# Set up variables
KEY_FILE = '/root/jsonfiles/ti-dba-prod-01.json'
BIGQUERY_PROJECT = 'ti-dba-prod-01'
BIGQUERY_DATASET = 'ti_db_inventory'
BIGQUERY_TABLE = 'servers_temp'  # Name of the BigQuery table
JSON_FILE_PATH = f"/backup/dumps/servers_temp_{datetime.now().strftime('%m%d%Y')}.json"

# MySQL connection details
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'uRbB5Y7xfHEK*'
MYSQL_DB = 'ti_db_inventory'

def backup_table_to_json():
    # Connect to MySQL database
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = conn.cursor(dictionary=True)

    # Query to retrieve data from servers_temp table
    query = "SELECT * FROM servers_temp"
    cursor.execute(query)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Write data to JSON file
    with open(JSON_FILE_PATH, 'w') as json_file:
        json.dump(rows, json_file, indent=4)
    
    print(f"Data backed up to JSON file: {JSON_FILE_PATH}")

def load_json_to_bigquery():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE
    client = bigquery.Client()

    # Define BigQuery table structure (schema)
    table_id = f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}'
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("ip", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("os", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("version", "STRING"),
            bigquery.SchemaField("frequency", "STRING"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("project", "STRING"),
            bigquery.SchemaField("encryption_type", "STRING"),
            bigquery.SchemaField("sun", "BOOLEAN"),
            bigquery.SchemaField("mon", "BOOLEAN"),
            bigquery.SchemaField("tue", "BOOLEAN"),
            bigquery.SchemaField("wed", "BOOLEAN"),
            bigquery.SchemaField("thu", "BOOLEAN"),
            bigquery.SchemaField("fri", "BOOLEAN"),
            bigquery.SchemaField("sat", "BOOLEAN"),
            bigquery.SchemaField("encrypted", "BOOLEAN"),
            bigquery.SchemaField("ssl", "BOOLEAN"),
            bigquery.SchemaField("user", "STRING"),
            bigquery.SchemaField("pwd", "STRING"),
            bigquery.SchemaField("backup", "BOOLEAN"),
            bigquery.SchemaField("load", "BOOLEAN"),
            bigquery.SchemaField("size", "BOOLEAN"),
            bigquery.SchemaField("bucket", "STRING"),
            bigquery.SchemaField("authfile", "STRING"),
            bigquery.SchemaField("save_path", "STRING"),
            bigquery.SchemaField("active", "BOOLEAN"),
            bigquery.SchemaField("owner", "STRING"),
            bigquery.SchemaField("environment", "STRING"),
            bigquery.SchemaField("database_name", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Load data into BigQuery
    with open(JSON_FILE_PATH, 'r') as json_file:
        rows_to_insert = json.load(json_file)
        
        # Write the list of dictionaries to a temporary JSON file
        temp_json_file_path = "/backup/dumps/temp_servers_temp.json"
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
        backup_table_to_json()
        load_json_to_bigquery()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
