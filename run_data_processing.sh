#!/bin/bash

# Variables
S3_BUCKET="your-s3-bucket-name"
S3_MOUNT_POINT="/mnt/s3bucket"
PYTHON_SCRIPT_PATH="$S3_MOUNT_POINT/Scripts/data_processing.py"
SOURCE_PATH="$S3_MOUNT_POINT/Stocks"
OUTPUT_PATH="$S3_MOUNT_POINT/Output"
INDEX_FILE_PATH="$S3_MOUNT_POINT/CompanyNames/top_companies.txt"

# Check if s3fs is installed
if ! command -v s3fs &> /dev/null
then
    echo "s3fs could not be found. Please install s3fs to proceed."
    exit 1
fi

# Mount the S3 bucket
if ! mountpoint -q "$S3_MOUNT_POINT"
then
    echo "Mounting S3 bucket..."
    s3fs $S3_BUCKET $S3_MOUNT_POINT -o use_cache=/tmp
else
    echo "S3 bucket is already mounted."
fi

# Check if mount was successful
if ! mountpoint -q "$S3_MOUNT_POINT"
then
    echo "Failed to mount S3 bucket. Exiting."
    exit 1
fi

# Run the data processing Python script
echo "Running data processing script..."
python3 $PYTHON_SCRIPT_PATH $SOURCE_PATH $OUTPUT_PATH $INDEX_FILE_PATH

# Check if the Python script executed successfully
if [ $? -eq 0 ]
then
    echo "Data processing completed successfully."
else
    echo "Data processing failed. Check the logs for more details."
    exit 1
fi

# Optionally, unmount the S3 bucket
# echo "Unmounting S3 bucket..."
# fusermount -u $S3_MOUNT_POINT

echo "Script execution completed."
