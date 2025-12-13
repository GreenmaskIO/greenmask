FROM golang:1.24-bookworm

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y wget gnupg2 lsb-release make \
    && echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -sc)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests  \
        postgresql-18 \
        postgresql-17 \
        postgresql-16 \
        postgresql-15 \
        postgresql-14 \
        postgresql-13 \
        postgresql-12 \
        postgresql-11

WORKDIR /var/lib/greenmask

COPY . .

RUN make build \
    && echo '#!/bin/bash \n\
IFS="," read -ra PG_VERSIONS_CHECK <<< "${PG_VERSIONS_CHECK}" \n\
for pg_version in ${PG_VERSIONS_CHECK[@]}; do \n\
    echo "### CHECK COMPATIBILITY WITH POSTGRESQL ${pg_version} ###" \n\
    export PG_HOST=$(echo "${PG_HOST_TEMPLATE}" | sed "s/<version>/${pg_version}/") \n\
    export STORAGE_S3_PREFIX="${pg_version}" \n\
    export URI="host=${PG_HOST} user=${PG_USER} password=${PG_PASSWORD} dbname=${PG_DATABASE} port=${PG_PORT}" \n\
    export PG_BIN_PATH="/usr/lib/postgresql/${pg_version}/bin/" \n\
    echo "### DEBUG ENVIRONMENT VARIABLES ###" \n\
    echo "PGHOST: ${PG_HOST} PGUSER: ${PG_USER} PGPASSWORD: ${PG_PASSWORD} PGDATABASE: ${PG_DATABASE} PGPORT: ${PG_PORT}" \n\
    echo "STORAGE_S3_PREFIX: ${STORAGE_S3_PREFIX}" \n\
    echo "URI: ${URI}" \n\
    echo "PG_BIN_PATH: ${PG_BIN_PATH}" \n\
    echo "### RUN TESTS ###" \n\
    go clean -testcache && go test -v ./tests/integration/... \n\
    if [ $? -eq 0 ]; then \n\
        echo "### SUCCESSFUL CHECK COMPATIBILITY WITH POSTGRESQL ${pg_version} ###" \n\
    else \n\
        echo "### FAIL CHECK COMPATIBILITY WITH POSTGRESQL ${pg_version} ###" \n\
        echo "### EXIT SCRIPT ###" \n\
        exit 2 \n\
    fi \n\
done \n' > /docker-entrypoint.sh \
    && chmod +x /docker-entrypoint.sh

CMD ["bash", "-c", "/docker-entrypoint.sh"]
