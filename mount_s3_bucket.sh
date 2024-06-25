#!/bin/bash

# Define the S3 bucket name
S3_BUCKET="2401ft-mbd-predict-sithabiseni-mtshali-s3-source"

# Define the local mount point
S3_MOUNT_POINT="/home/ubuntu/s3-drive"

# Ensure S3 bucket is mounted
if ! mountpoint -q "$S3_MOUNT_POINT"; then
    echo "Mounting S3 bucket..."
    s3fs $S3_BUCKET $S3_MOUNT_POINT -o iam_role=auto -o allow_other -o use_cache=/tmp -o nonempty
else
    echo "S3 bucket already mounted."
fi
