import os
import sqlparse
from google.cloud import bigquery
from datetime import datetime

# Set up variables
SQL_FILE_PATH = '/backup/dumps/ti_db_inventory_daily_log_01222025.sql'
KEY_FILE = '/root/jsonfiles/ti-dba-prod-01.json'
BIGQUERY_PROJECT = 'ti-dba-prod-01'
BIGQUERY_DATASET = 'ti_db_inventory'
BIGQUERY_TABLE = 'daily_log'  # Name of the BigQuery table

def load_data_from_sql():
    # Read SQL file
    with open(SQL_FILE_PATH, 'r') as file:
        sql_data = file.read()

    # Parse SQL file
    parsed_data = sqlparse.split(sql_data)
    
    # Extract the insert data from SQL file, assuming standard mysqldump INSERT syntax
    data = []
    for statement in parsed_data:
        if statement.startswith('INSERT'):
            # Format: INSERT INTO `table` VALUES (values), (values), ...;
            values_part = statement.partition('VALUES')[2].strip().strip(';')
            values_list = values_part.split('), (')
            for values in values_list:
                values = values.strip('(').strip(')')
                row = values.split(',')
                data.append([value.strip().strip("'\"") for value in row])
                
    return data

def convert_to_bigquery_format(data):
    rows_to_insert = []
    for row in data:
        rows_to_insert.append({
            "ID": int(row[0]),
            "BackupDate": datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S.%f'),
            "Server": row[2],
            "Database": row[3],
            "Size": int(row[4]),
            "State": row[6],
            "LastUpdate": datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S.%f')
        })
    return rows_to_insert

def insert_data_to_bigquery(data):
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
    errors = client.insert_rows_json(table_id, data, row_ids=[None]*len(data), job_config=job_config)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Data inserted successfully.")

def main():
    try:
        data = load_data_from_sql()
        formatted_data = convert_to_bigquery_format(data)
        insert_data_to_bigquery(formatted_data)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
