#!/bin/bash
# Submit QBV Spark job
SPARK_HOME=/opt/spark
SCRIPT_DIR=$(dirname "$0")
python_exec="${SPARK_HOME}/bin/spark-submit"
"$python_exec" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  "${SCRIPT_DIR}/QBV.py"
