FROM node:20-alpine AS builder

WORKDIR /app

ARG API_BASE_URL
ENV API_BASE_URL=$API_BASE_URL
ARG VIS_API_BASE_URL
ENV VIS_API_BASE_URL=$VIS_API_BASE_URL

COPY frontend/package.json /app/package.json
RUN npm install

COPY frontend /app
RUN npm run build \
  && mkdir -p /app/.next/standalone/.next \
  && cp -r /app/.next/static /app/.next/standalone/.next/static \
  && cp -r /app/public /app/.next/standalone/public

FROM node:20-alpine AS runner

WORKDIR /app

COPY --from=builder /app/.next/standalone /app
COPY --from=builder /app/.next/static /app/.next/static
COPY --from=builder /app/public /app/public

EXPOSE 3000
ENV HOSTNAME=0.0.0.0
ENV PORT=3000
CMD ["node", "server.js"]
