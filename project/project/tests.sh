#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the data directory and database files
DATA_DIR="data"
CDI_DB="data/cdi_data.db"
AIR_QUALITY_DB="data/air_quality_data.db"

# Run the data pipeline
echo "Running the data pipeline..."
python3 main.py

# Validate that output files exist
echo "Validating output files..."
if [ -f "$CDI_DB" ] && [ -f "$AIR_QUALITY_DB" ]; then
    echo "Output files exist:"
    echo "  - $CDI_DB"
    echo "  - $AIR_QUALITY_DB"
else
    echo "One or more output files are missing!"
    exit 1
fi

# Success message
echo "All tests passed successfully!"
