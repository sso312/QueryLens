FROM nginx:1.27-alpine

COPY docker/nginx/api.conf.tmpl /etc/nginx/api.conf.tmpl
COPY docker/nginx/render-api-conf.sh /docker-entrypoint.d/99-render-api-conf.sh

RUN chmod +x /docker-entrypoint.d/99-render-api-conf.sh
