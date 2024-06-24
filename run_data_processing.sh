#!/bin/bash

# Define the S3 bucket name
S3_BUCKET="2401ft-mbd-predict-sithabiseni-mtshali-s3-source"

# Define the local mount point
S3_MOUNT_POINT="/home/ubuntu/s3-drive"

# Define local paths
LOCAL_SCRIPTS_PATH="$S3_MOUNT_POINT/Scripts"
LOCAL_OUTPUT_PATH="$S3_MOUNT_POINT/Output"
LOCAL_STOCKS_PATH="$S3_MOUNT_POINT/Stocks"
LOCAL_INDEX_FILE_PATH="$S3_MOUNT_POINT/CompanyNames/top_companies.txt"

# Ensure S3 bucket is mounted
if ! mountpoint -q "$S3_MOUNT_POINT"; then
    echo "Mounting S3 bucket..."
    s3fs $S3_BUCKET $S3_MOUNT_POINT -o iam_role=auto -o allow_other -o use_cache=/tmp -o nonempty
else
    echo "S3 bucket already mounted."
fi

# Download the data_processing.py script
aws s3 cp s3://$S3_BUCKET/Scripts/data_processing.py ./

# Run the data processing script
python3 ./data_processing.py $LOCAL_STOCKS_PATH $LOCAL_OUTPUT_PATH $LOCAL_INDEX_FILE_PATH
