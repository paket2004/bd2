#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this
echo "PREPARE_DATA.SH"
hdfs dfs -put -f a.parquet / && \
    spark-submit \
        --executor-memory 4G \
        --driver-memory 2G \
        prepare_data.py && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /index/data && \
    echo "Done!"