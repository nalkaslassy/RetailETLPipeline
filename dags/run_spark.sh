#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-/opt/airflow/data}"
ARCHIVE_DIR="${ARCHIVE_DIR:-/opt/airflow/archive}"
SCRIPT_PATH="/opt/airflow/dags/retail_project/RetailToPostgres.py"

file="$(ls -1 "${DATA_DIR}"/retail_batch_*.csv 2>/dev/null | head -n 1 || true)"
if [ -z "${file}" ]; then
  echo "No new files found"
  exit 0
fi

echo "Processing: ${file}"
echo "== ENV/versions =="
echo "JAVA_HOME=${JAVA_HOME:-}"
[ -n "${JAVA_HOME:-}" ] && "${JAVA_HOME}/bin/java" -version || true
/opt/bitnami/python/bin/python --version || true

# Ensure psycopg2 (only installs if missing)
if ! /opt/bitnami/python/bin/python -c 'import psycopg2' 2>/dev/null; then
  /opt/bitnami/python/bin/pip install --no-cache-dir psycopg2-binary==2.9.9
fi

/opt/bitnami/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.pyspark.python=/opt/bitnami/python/bin/python \
  --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python \
  --packages org.postgresql:postgresql:42.7.3 \
  "${SCRIPT_PATH}" "${file}"

mv "${file}" "${ARCHIVE_DIR}/"
