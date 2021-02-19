#!/usr/bin/env bash

# Run this to launch the docker containers for MSSQL Server & S3 endpoint

set -euxo pipefail

docker-compose down

# Build & start the services
docker-compose up -d --build

# Wait for SQL & S3 to be responsive
docker-compose run wait -c sql.data:1433,localstack:4566

echo "Services are ready"
# needs an extra 10s here for data seeding
# Test Fake News end to end
