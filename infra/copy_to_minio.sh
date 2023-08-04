#!/bin/bash

set -e

mc alias set minio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} 
mc alias list
mc mb minio/demo-data --ignore-existing
mc mb minio/dal --ignore-existing
mc mb minio/temp --ignore-existing
mc mb minio/checkpoints --ignore-existing
mc mb minio/raw-data --ignore-existing
mc cp /data/Customers.csv minio/demo-data/Customers/
mc cp /data/orders.json minio/demo-data/Orders/
mc cp /data/Industries.csv minio/demo-data/Industries/
mc cp /data/Products.csv minio/demo-data/Products/