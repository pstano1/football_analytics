#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
sleep 5

echo "Creating core database with PostgreSQL engine..."
echo "Connecting to: ${POSTGRES_HOST}:${POSTGRES_PORT}, DB: ${POSTGRES_DB}, User: ${POSTGRES_USER}"

clickhouse-client --user ${CLICKHOUSE_USER} --password ${CLICKHOUSE_PASSWORD} --query "
CREATE DATABASE IF NOT EXISTS core
ENGINE = PostgreSQL('${POSTGRES_HOST}:${POSTGRES_PORT}', '${POSTGRES_DB}', '${POSTGRES_USER}', '${POSTGRES_PASSWORD}', 'core')
"

echo "Core database created successfully"