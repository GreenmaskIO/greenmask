ARG GO_VERSION=1.24
ARG DEBIAN_RELEASE=bookworm
ARG MYSQL_VERSION=8.0

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${DEBIAN_RELEASE} AS build

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

WORKDIR /var/lib/greenmask

COPY ../.. .

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOARM=$(echo ${TARGETVARIANT} | cut -d 'v' -f 2) make build

FROM debian:${DEBIAN_RELEASE}-slim AS mysql

ARG DEBIAN_RELEASE=bookworm
ARG MYSQL_VERSION=8.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y wget gnupg2 bash-completion bzip2 openssl zstd xz-utils perl \
    && rm -rf /var/lib/apt/lists/*

RUN echo "deb [ signed-by=/etc/apt/keyrings/mysql.gpg ] http://repo.mysql.com/apt/debian/ ${DEBIAN_RELEASE} mysql-${MYSQL_VERSION}" > /etc/apt/sources.list.d/mysql.list

RUN set -eux; \
	key='BCA4 3417 C3B4 85DD 128E C6D4 B7B3 B788 A8D3 785C'; \
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$key"; \
	mkdir -p /etc/apt/keyrings; \
	gpg --batch --export "$key" > /etc/apt/keyrings/mysql.gpg; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME"

RUN apt-get update \
	&& apt-get install -y \
		mysql-community-client \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=build /var/lib/greenmask/greenmask /usr/bin

RUN mkdir /home/greenmask \
    && groupadd -g 10001 greenmask \
    && useradd -u 10000 -g greenmask greenmask \
    && chown -R greenmask:greenmask /home/greenmask

USER greenmask:greenmask

RUN mkdir ~/.bash_completions \
    && greenmask completion bash > ~/.bash_completions/greenmask.bash \
    && echo 'source /etc/bash_completion' >> ~/.bashrc \
    && echo 'source ~/.bash_completions/greenmask.bash' >> ~/.bashrc

WORKDIR /home/greenmask

ENTRYPOINT ["greenmask"]
