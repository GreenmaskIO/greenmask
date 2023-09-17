FROM debian:12

ARG DEBIAN_FRONTEND=noninteractive
ARG POSTGRESQL_VERSION=15
ARG GOPATH=/usr/local/go
ARG GOLINK="https://go.dev/dl/go1.21.1.linux-amd64.tar.gz"

RUN apt-get update && apt-get install -y gnupg2 wget systemctl
RUN bash -c 'source /etc/os-release && echo "deb https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN bash -c 'wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -'
RUN apt-get update && apt-get -y install postgresql-$POSTGRESQL_VERSION
RUN wget $GOLINK /tmp
RUN rm -rf $GOPATH && tar -C /usr/local -xzf go1.21.1.linux-amd64.tar.gz
RUN mkdir /var/lib/integration_tests
COPY .  /var/lib/integration_tests

COPY schema-dump.sql /tmp/
