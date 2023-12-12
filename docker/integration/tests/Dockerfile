FROM ubuntu:22.04

ARG GOOS=linux
ARG GOARCH=amd64
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y wget gnupg2
RUN echo "deb https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests  \
    postgresql-16 \
    postgresql-15 \
    postgresql-14 \
    postgresql-13 \
    postgresql-12 \
    postgresql-11
RUN wget https://go.dev/dl/go1.21.1.linux-amd64.tar.gz && \
    rm -rf /usr/local/go &&  \
    tar -C /usr/local -xzf go1.21.1.linux-amd64.tar.gz

RUN mkdir /var/lib/greenmask && cd /var/lib/greenmask
WORKDIR /var/lib/greenmask
COPY . .
RUN cd cmd/greenmask && /usr/local/go/bin/go build . && cd -

CMD ["sh", "-c", "/usr/local/go/bin/go test -v ./tests/integration/..."]
