#This script handles the transformation of data according to the table requirements.

import pandas as pd
import logging

def transform_data(df, table_name):
    """Transform the data according to table requirements"""
    try:
        if table_name == 'daily_log':
            df = df.rename(columns={
                'ID': 'ID',
                'backup_date': 'BackupDate',
                'server': 'Server',
                'database': 'Database',
                'size': 'Size',
                'state': 'State',
                'last_update': 'LastUpdate',
                'fileName': 'FileName'
            })
            df = df.drop(columns=['fileName'], errors='ignore')

        elif table_name == 'backup_log':
            df = df.rename(columns={
                'id': 'id',
                'backup_date': 'backup_date',
                'server': 'server',
                'size': 'size',
                'filepath': 'filepath',
                'last_update': 'last_update'
            })

        elif table_name == 'database_list':
            bool_columns = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat',
                            'encrypted', 'ssl', 'backup', 'load', 'size', 'active']
            for col in bool_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)
            df = df.rename(columns={
                'name': 'name',
                'ip': 'ip',
                'description': 'description',
                'os': 'os',
                'type': 'type',
                'version': 'version',
                'frequency': 'frequency',
                'location': 'location',
                'project': 'project',
                'encryption_type': 'encryption_type',
                'sun': 'sun',
                'mon': 'mon',
                'tue': 'tue',
                'wed': 'wed',
                'thu': 'thu',
                'fri': 'fri',
                'sat': 'sat',
                'encrypted': 'encrypted',
                'ssl': 'ssl',
                'user': 'user',
                'pwd': 'pwd',
                'backup': 'backup',
                'load': 'load',
                'size': 'size',
                'bucket': 'bucket',
                'authfile': 'authfile',
                'save_path': 'save_path',
                'active': 'active',
                'owner': 'owner',
                'environment': 'environment',
                'database_name': 'database_name',
                'creation_date': 'creation_date'
            })

        logging.info(f"Transformed data for table: {table_name}")
        return df
    except Exception as e:
        logging.error(f"Error transforming data for table {table_name}: {e}")
        raise
