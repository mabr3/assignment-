#!/bin/bash

JOB_FILE=$1
shift  # Shift the arguments to remove the first argument (JOB_FILE)

docker exec -it spark-iceberg spark-submit \
  --master local[*] \
  /home/iceberg/spark-apps/${JOB_FILE} "$@"

#$@ will allow the rest of the arguments