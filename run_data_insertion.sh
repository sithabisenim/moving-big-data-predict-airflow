#!/bin/bash

# Define the S3 bucket name and mount point
S3_BUCKET="2401ft-mbd-predict-sithabiseni-mtshali-s3-source"
S3_MOUNT_POINT="/home/ubuntu/s3-drive"

# Ensure S3 bucket is mounted
if ! mountpoint -q "$S3_MOUNT_POINT"; then
    echo "S3 bucket is not mounted. Please run mount_s3_bucket.sh first."
    exit 1
fi

# Download the insert_data.py script from S3
aws s3 cp s3://$S3_BUCKET/Scripts/insert_data.py ./

# Run the data insertion script
python3 ./insert_data.py
