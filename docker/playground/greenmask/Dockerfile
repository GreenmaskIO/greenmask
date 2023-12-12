FROM ubuntu:22.04

ARG GOOS=linux
ARG GOARCH=amd64
ARG DEBIAN_FRONTEND=noninteractive
ENV INSTALL_GO_VERSION="1.21.5"

RUN apt-get update && apt-get install -y wget gnupg2
RUN echo "deb https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests  postgresql-16
RUN wget https://go.dev/dl/go$INSTALL_GO_VERSION.linux-amd64.tar.gz && \
    rm -rf /usr/local/go &&  \
    tar -C /usr/local -xzf go$INSTALL_GO_VERSION.linux-amd64.tar.gz

RUN mkdir /var/lib/greenmask && cd /var/lib/greenmask
WORKDIR /var/lib/greenmask
COPY . .
RUN cd cmd/greenmask && /usr/local/go/bin/go build . && cd -

CMD ["./greenmask", "--config", "config.yml"]
