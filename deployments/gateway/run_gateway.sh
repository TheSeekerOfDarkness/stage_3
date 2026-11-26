#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NGINX_PORT="${NGINX_PORT:-8080}"
CONSUL_BIND_IP="${CONSUL_BIND_IP:-127.0.0.1}"
CONSUL_HTTP_ADDR="${CONSUL_HTTP_ADDR:-127.0.0.1:8500}"
START_LOCAL_CONSUL="${START_LOCAL_CONSUL:-true}"
LOG_DIR="${LOG_DIR:-${REPO_ROOT}/logs/gateway}"

for bin in consul-template nginx; do
    if ! command -v "${bin}" >/dev/null 2>&1; then
        echo "Missing required binary: ${bin}" >&2
        exit 1
    fi
done

if [[ "${START_LOCAL_CONSUL}" == "true" ]] && ! command -v consul >/dev/null 2>&1; then
    echo "Missing required binary: consul (or set START_LOCAL_CONSUL=false)" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/search-gateway.XXXXXX")"
mkdir -p "${TMP_DIR}/conf.d" "${TMP_DIR}/logs" "${TMP_DIR}/client_body" "${TMP_DIR}/proxy" "${TMP_DIR}/fastcgi" "${TMP_DIR}/uwsgi" "${TMP_DIR}/scgi"

CONSUL_PID=""
cleanup() {
    if [[ -f "${TMP_DIR}/nginx.pid" ]]; then
        nginx -p "${TMP_DIR}" -c "${TMP_DIR}/nginx.conf" -s quit >/dev/null 2>&1 || true
    fi
    if [[ -n "${CONSUL_PID}" ]]; then
        kill "${CONSUL_PID}" >/dev/null 2>&1 || true
    fi
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT INT TERM

if [[ "${START_LOCAL_CONSUL}" == "true" ]]; then
    consul agent -dev -client=0.0.0.0 -bind="${CONSUL_BIND_IP}" -advertise="${CONSUL_BIND_IP}" \
        >"${LOG_DIR}/consul.log" 2>&1 &
    CONSUL_PID=$!
    sleep 2
fi

cat <<EOF > "${TMP_DIR}/nginx.conf"
worker_processes auto;
pid ${TMP_DIR}/nginx.pid;
error_log ${LOG_DIR}/nginx-error.log warn;

events {
    worker_connections 1024;
}

http {
    default_type application/json;
    client_body_temp_path ${TMP_DIR}/client_body;
    proxy_temp_path ${TMP_DIR}/proxy;
    fastcgi_temp_path ${TMP_DIR}/fastcgi;
    uwsgi_temp_path ${TMP_DIR}/uwsgi;
    scgi_temp_path ${TMP_DIR}/scgi;

    map \$http_x_request_id \$req_id {
        default \$http_x_request_id;
        "" \$request_id;
    }

    log_format gateway_json escape=json
        '{"ts":"\$time_iso8601",'
        '"request_id":"\$req_id",'
        '"status":\$status,'
        '"upstream_addr":"\$upstream_addr",'
        '"upstream_status":"\$upstream_status",'
        '"request_time":\$request_time,'
        '"upstream_response_time":"\$upstream_response_time",'
        '"uri":"\$request_uri"}';

    access_log ${LOG_DIR}/nginx-access.log gateway_json;
    sendfile on;
    keepalive_timeout 65;

    include ${TMP_DIR}/conf.d/*.conf;
}
EOF

sed "s/listen 80;/listen ${NGINX_PORT};/g" "${SCRIPT_DIR}/search-gateway.ctmpl" > "${TMP_DIR}/search-gateway.ctmpl"

cat <<EOF > "${TMP_DIR}/conf.d/default.conf"
server {
    listen ${NGINX_PORT};

    location = /api/v1/search {
        default_type application/json;
        return 503 '{"error":"no_ready_search_backends"}';
    }

    location / {
        default_type application/json;
        return 404 '{"error":"not_found"}';
    }
}
EOF

nginx -p "${TMP_DIR}" -c "${TMP_DIR}/nginx.conf" -t
nginx -p "${TMP_DIR}" -c "${TMP_DIR}/nginx.conf"

echo "Gateway started: NGINX=http://0.0.0.0:${NGINX_PORT}/api/v1/search CONSUL_HTTP_ADDR=${CONSUL_HTTP_ADDR}"
consul-template \
    -consul-addr="${CONSUL_HTTP_ADDR}" \
    -wait="200ms:1s" \
    -template="${TMP_DIR}/search-gateway.ctmpl:${TMP_DIR}/conf.d/default.conf:nginx -p ${TMP_DIR} -c ${TMP_DIR}/nginx.conf -t && nginx -p ${TMP_DIR} -c ${TMP_DIR}/nginx.conf -s reload"
