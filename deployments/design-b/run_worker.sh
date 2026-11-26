#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAR_PATH="${JAR_PATH:-${REPO_ROOT}/worker/target/worker-1.0.0.jar}"

if [[ ! -f "${JAR_PATH}" ]]; then
    echo "Worker fat-JAR not found at ${JAR_PATH}; building with Maven..."
    mvn -f "${REPO_ROOT}/pom.xml" -DskipTests package
fi

export DESIGN_MODE="design-b"
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
export HAZELCAST_CLUSTER_NAME="${HAZELCAST_CLUSTER_NAME:-search-cluster}"
export HAZELCAST_PORT="${HAZELCAST_PORT:-5701}"
export HAZELCAST_JOIN_MODE="${HAZELCAST_JOIN_MODE:-tcpip}"
export HAZELCAST_MEMBERS="${HAZELCAST_MEMBERS:-127.0.0.1:5701}"
export HAZELCAST_BACKUP_COUNT="${HAZELCAST_BACKUP_COUNT:-1}"
export LOG_DIR="${LOG_DIR:-${REPO_ROOT}/logs}"
export DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data}"

DEFAULT_JAVA_OPTS="-Xms512m -Xmx1536m --add-modules java.se --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"
JAVA_OPTS_STR="${JAVA_OPTS:-${DEFAULT_JAVA_OPTS}}"
read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS_STR}"

echo "Starting Design B worker: id=${WORKER_ID} host=${HOST_NAME} ip=${HOST_IP}:${PORT} roles=${WORKER_ROLES} hz=${HOST_IP}:${HAZELCAST_PORT} backups=${HAZELCAST_BACKUP_COUNT}"
exec java "${JAVA_OPTS_ARR[@]}" -jar "${JAR_PATH}"
