#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAR_PATH="${JAR_PATH:-${REPO_ROOT}/worker/target/worker-1.0.0.jar}"

if [[ ! -f "${JAR_PATH}" ]]; then
    echo "Worker fat-JAR not found at ${JAR_PATH}; building with Maven..."
    mvn -f "${REPO_ROOT}/pom.xml" -DskipTests package
fi

export DESIGN_MODE="design-a"
export WORKER_ID="${WORKER_ID:-worker-$(hostname)}"
export HOST_NAME="${HOST_NAME:-$(hostname)}"
export HOST_IP="${HOST_IP:-127.0.0.1}"
export PORT="${PORT:-7000}"
export WORKER_ROLES="${WORKER_ROLES:-ingest,index,search}"
export CRAWL_THREADS="${CRAWL_THREADS:-2}"
export INDEX_THREADS="${INDEX_THREADS:-2}"
export SEARCH_THREADS="${SEARCH_THREADS:-8}"
export CONSUL_ENABLED="${CONSUL_ENABLED:-true}"
export CONSUL_URL="${CONSUL_URL:-http://127.0.0.1:8500}"
export RABBITMQ_HOST="${RABBITMQ_HOST:-127.0.0.1}"
export RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
export LOG_DIR="${LOG_DIR:-${REPO_ROOT}/logs}"
export DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data}"

JAVA_OPTS_STR="${JAVA_OPTS:--Xms256m -Xmx1024m}"
read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS_STR}"

echo "Starting Design A worker: id=${WORKER_ID} host=${HOST_NAME} ip=${HOST_IP}:${PORT} roles=${WORKER_ROLES} threads=${CRAWL_THREADS}/${INDEX_THREADS}/${SEARCH_THREADS}"
exec java "${JAVA_OPTS_ARR[@]}" -jar "${JAR_PATH}"
