#!/bin/sh
set -eu

container="$(docker ps --quiet \
  --filter 'label=com.service-federation.service=database' \
  --filter "publish=${DB_PORT}")"

test -n "$container"
docker exec -i "$container" psql --quiet --set ON_ERROR_STOP=1 \
  --host 127.0.0.1 --username "$DB_USER" --dbname "$DB_NAME" <<'SQL'
CREATE TABLE IF NOT EXISTS visits (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  visited_at timestamptz NOT NULL DEFAULT now()
);
SQL
