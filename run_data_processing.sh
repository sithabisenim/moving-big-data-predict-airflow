#!/bin/bash

# Define variables
S3_MOUNT_POINT="/home/ubuntu/s3-drive"
INDEX_FILE_PATH="${S3_MOUNT_POINT}/CompanyNames/top_companies.txt"
SOURCE_PATH="${S3_MOUNT_POINT}/Stocks"
OUTPUT_PATH="${S3_MOUNT_POINT}/Output"

# Ensure directories exist
mkdir -p "${S3_MOUNT_POINT}/CompanyNames"
mkdir -p "${S3_MOUNT_POINT}/Stocks"
mkdir -p "${S3_MOUNT_POINT}/Output"

# Mount the S3 bucket (if not already mounted)
# Replace 'your-s3-bucket-name' with your actual S3 bucket name
sudo s3fs 2401ft-mbd-predict-sithabiseni-mtshali-s3-source "${S3_MOUNT_POINT}" -o iam_role="auto"

# Download data_processing.py from S3 bucket
aws s3 cp s3://2401ft-mbd-predict-sithabiseni-mtshali-s3-source/Scripts/data_processing.py /home/ubuntu/data_processing.py

# Run the Python script
python3 /home/ubuntu/data_processing.py "${SOURCE_PATH}" "${OUTPUT_PATH}" "${INDEX_FILE_PATH}"

# Unmount the S3 bucket (optional)
# sudo umount "${S3_MOUNT_POINT}"

