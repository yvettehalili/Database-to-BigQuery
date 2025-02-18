#!/bin/bash

# Activate the virtual environment
source /backup/environments/backupv1/bin/activate

# Run the ETL Python script
python /backup/scripts/ti-dba-prod-01/etl_process.py "$@"
