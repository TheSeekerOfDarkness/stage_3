#!/bin/sh
set -eu

NGINX_PORT="${NGINX_PORT:-80}"
sed "s/listen 80;/listen ${NGINX_PORT};/g" /etc/consul-template/search-gateway.ctmpl > /tmp/search-gateway.ctmpl

cat <<EOF > /etc/nginx/conf.d/default.conf
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

nginx -t
nginx

exec consul-template \
    -consul-addr="${CONSUL_HTTP_ADDR:-consul:8500}" \
    -wait="200ms:1s" \
    -template="/tmp/search-gateway.ctmpl:/etc/nginx/conf.d/default.conf:nginx -t && nginx -s reload"
