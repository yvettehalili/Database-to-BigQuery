import mysql.connector
import json

# MySQL connection details
mysql_config = {
    'user': 'root',
    'password': 'uRbB5Y7xfHEK*',
    'host': 'localhost',
    'database': 'ti_db_inventory'
}

# File path to save the JSON backup
json_file_path = "/backup/dumps/ti_db_inventory_daily_log_01222025.json"

def export_table_to_ndjson():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        # Query to select data from table
        query = "SELECT ID, backup_date as BackupDate, server as Server, database as Database, size as Size, state as State, last_update as LastUpdate FROM daily_log"
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Write rows to NDJSON file
        with open(json_file_path, 'w') as json_file:
            for row in rows:
                json.dump(row, json_file, default=str)
                json_file.write('\n')  # Newline delimiter
        
        print(f"Data exported to {json_file_path} successfully.")
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    export_table_to_ndjson()
