#!/usr/bin/env bash
set -euo pipefail

BUCKET_NAME="${1:?Uso: ./airflow_setup.sh <BUCKET_NAME>}"

if [[ -d "astro_project" ]]; then
    cd astro_project
    astro-cli dev kill || true
    cd ..
    rm -rf astro_project
fi

mkdir -p astro_project
cd astro_project

astro-cli dev init

cp -ra ../airflow/* .

mv ./include/.env.example include/.env

ENV_FILE="include/.env"

sed -i \
    -e "s|^BUCKET_NAME=.*|BUCKET_NAME=$BUCKET_NAME|" \
    "$ENV_FILE"

astro-cli dev start

docker exec \
$(docker ps --format '{{.Names}}' | grep webserver | head -n 1) \
airflow connections add my_spark_conn \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077