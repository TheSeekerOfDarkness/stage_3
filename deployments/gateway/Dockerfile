FROM nginx:1.27-alpine

ARG TARGETARCH
ARG CONSUL_TEMPLATE_VERSION=0.39.1

RUN apk add --no-cache curl unzip ca-certificates \
    && ARCH="${TARGETARCH:-amd64}" \
    && curl -fsSL "https://releases.hashicorp.com/consul-template/${CONSUL_TEMPLATE_VERSION}/consul-template_${CONSUL_TEMPLATE_VERSION}_linux_${ARCH}.zip" -o /tmp/ct.zip \
    && unzip /tmp/ct.zip -d /usr/local/bin \
    && rm -f /tmp/ct.zip \
    && mkdir -p /etc/consul-template /etc/nginx/conf.d

COPY nginx.conf /etc/nginx/nginx.conf
COPY search-gateway.ctmpl /etc/consul-template/search-gateway.ctmpl
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 80
ENTRYPOINT ["/entrypoint.sh"]
