#!/usr/bin/env bash
#
# Run this to switch off the docker containers for MSSQL Server & S3 endpoint

set -euxo pipefail

docker-compose down
