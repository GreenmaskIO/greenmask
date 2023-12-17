ARG GO_VERSION=1.21.5

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

WORKDIR /var/lib/greenmask

COPY . .

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOARM=$(echo ${TARGETVARIANT} | cut -d 'v' -f 2) make build

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget gnupg2 bash-completion && \
    echo "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install --no-install-recommends --no-install-suggests -y \
        postgresql-client-16 \
        postgresql-client-15 \
        postgresql-client-14 \
        postgresql-client-13 \
        postgresql-client-12 \
        postgresql-client-11 && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir ~/.bash_completions && \
    greenmask completion bash > /root/.bash_completions/greenmask.bash && \
    echo 'source /etc/bash_completion' >> /root/.bashrc && \
    echo 'source /root/.bash_completions/greenmask.bash' >> /root/.bashrc

COPY --from=build /var/lib/greenmask/greenmask /usr/bin

ENTRYPOINT ["/bin/bash"]
