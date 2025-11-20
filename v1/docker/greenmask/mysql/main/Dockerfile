ARG GO_VERSION=1.24
ARG DEBIAN_RELEASE=bookworm
ARG MYSQL_VERSION=8.0.44

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${DEBIAN_RELEASE} AS build

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

WORKDIR /var/lib/greenmask

COPY ../.. .

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOARM=$(echo ${TARGETVARIANT} | cut -d 'v' -f 2) make build

FROM mysql:${MYSQL_VERSION}-${DEBIAN_RELEASE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y curl wget gnupg2 bash-completion

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
