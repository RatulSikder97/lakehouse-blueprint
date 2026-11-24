#!/bin/sh
# Creates one database + role per platform component (runs once, on an empty volume).
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE lakekeeper LOGIN PASSWORD '${LAKEKEEPER_DB_PASSWORD}';
CREATE DATABASE lakekeeper OWNER lakekeeper;
CREATE ROLE dagster LOGIN PASSWORD '${DAGSTER_PG_PASSWORD}';
CREATE DATABASE dagster OWNER dagster;
SQL
