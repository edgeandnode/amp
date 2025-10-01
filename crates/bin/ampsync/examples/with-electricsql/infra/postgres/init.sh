#!/usr/bin/env bash

set -e
set -u

function create_user_and_database() {
  local db=$1
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
        CREATE DATABASE $db;
        GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
}

if [ -n "${POSTGRES_DB_LIST:-}" ]; then
  for db in $(echo $POSTGRES_DB_LIST | tr ',' ' '); do
    create_user_and_database $db
  done
fi
