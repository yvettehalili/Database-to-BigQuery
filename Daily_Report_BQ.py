import os
import json
import mysql.connector
from google.cloud import bigquery
from datetime import datetime

# Set up variables
KEY_FILE = '/root/jsonfiles/ti-dba-prod-01.json'
BIGQUERY_PROJECT = 'ti-dba-prod-01'
BIGQUERY_DATASET = 'ti_db_inventory'
BIGQUERY_TABLE = 'daily_report'
JSON_FILE_PATH = "/backup/dumps/daily_report.json"
DB_USER = "trtel.backup"
DB_PASS = "Telus2017#"
DB_NAME = "ti_db_inventory"
REPORT_DATE = datetime.utcnow().strftime('%Y-%m-%d')

# MySQL connection
conn = mysql.connector.connect(
    user=DB_USER,
    password=DB_PASS,
    host='localhost',
    database=DB_NAME
)
cursor = conn.cursor()

# Define the function to generate queries
def generate_query(server_type, location_constraint):
    query = f"""
    SELECT 
        b.server AS Server, 
        (CASE 
            WHEN (TRUNCATE((SUM(b.size) / 1024), 0) > 0) THEN 
                (CASE 
                    WHEN (TRUNCATE(((SUM(b.size) / 1024) / 1024), 0) > 0) THEN 
                        TRUNCATE(((SUM(b.size) / 1024) / 1024), 2) 
                    ELSE TRUNCATE((SUM(b.size) / 1024), 2) 
                END) 
            ELSE SUM(b.size) 
        END) AS Size, 
        (CASE 
            WHEN (TRUNCATE((SUM(b.size) / 1024), 0) > 0) THEN 
                (CASE 
                    WHEN (TRUNCATE(((SUM(b.size) / 1024) / 1024), 0) > 0) 
                    THEN 'MB' ELSE 'KB' 
                END) ELSE 'B' 
        END) AS SizeName, 
        s.location AS Location, 
        s.type AS DBEngine, 
        s.os AS OS 
    FROM daily_log b 
    JOIN servers s ON s.name = b.server 
    WHERE b.backup_date = '{REPORT_DATE}' {location_constraint} AND s.type='{server_type}' 
    GROUP BY b.server, s.location, s.type, s.os;
    """
    return query

# Generate Queries
queries = [
    generate_query("MYSQL", "AND s.location='GCP'"),
    generate_query("PGSQL", "AND s.location='GCP'"),
    generate_query("MSSQL", "AND s.location='GCP'")
]

# Extract data and write to JSON file
rows_to_insert = []
for query in queries:
    print(f"Executing query:\n{query}\n")  # Debugging: Print the query
    cursor.execute(query)
    result = cursor.fetchall()
    print(f"Query result: {result}\n")  # Debugging: Print the query result
    for row in result:
        Server, Size, SizeName, Location, DBEngine, OS = row
        Error = "Yes" if Size == 0.00 and SizeName == "B" else "No"
        rows_to_insert.append({
            "Server": Server,
            "Size": Size,
            "SizeName": SizeName,
            "Location": Location,
            "DBEngine": DBEngine,
            "OS": OS,
            "Error": Error,
            "ReportDate": REPORT_DATE
        })

# Debugging: Print the extracted data
print("Extracted Data:")
for row in rows_to_insert:
    print(row)

cursor.close()
conn.close()

# Write data to JSON file
with open(JSON_FILE_PATH, 'w') as json_file:
    json.dump(rows_to_insert, json_file, indent=4)

# Verify the JSON file content
print(f"Data written to JSON file: {JSON_FILE_PATH}")
with open(JSON_FILE_PATH, 'r') as json_file:
    data_from_file = json.load(json_file)
    print("Data in JSON file:")
    for row in data_from_file:
        print(row)

# Load JSON data into BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE
client = bigquery.Client()

table_id = f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}'
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)

# Convert list of dictionaries to newline-delimited JSON format
ndjson_data = '\n'.join([json.dumps(row) for row in rows_to_insert])

# Use io.BytesIO to create a file-like object from the string
import io
ndjson_bytes = io.BytesIO(ndjson_data.encode('utf-8'))

# Load data into BigQuery
job = client.load_table_from_file(
    file_obj=ndjson_bytes, 
    destination=table_id, 
    job_config=job_config
)
job.result()  # Wait for the job to complete

# Check for errors in the job
if job.errors:
    print("Encountered errors while inserting rows: {}".format(job.errors))
else:
    print("Data inserted successfully.")
