FROM ubuntu:22.04 AS build

ARG GOOS=linux
ARG GOARCH=amd64
ARG DEBIAN_FRONTEND=noninteractive
ARG INSTALL_GO_VERSION="1.21.5"

RUN apt-get update && apt-get install -y wget make git
RUN wget https://go.dev/dl/go$INSTALL_GO_VERSION.linux-amd64.tar.gz && \
    rm -rf /usr/local/go &&  \
    tar -C /usr/local -xzf go$INSTALL_GO_VERSION.linux-amd64.tar.gz

RUN mkdir /var/lib/greenmask && cd /var/lib/greenmask
WORKDIR /var/lib/greenmask

COPY . .
RUN ls -l && pwd
RUN PATH=$PATH:/usr/local/go/bin/ make build

FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive

COPY --from=build /var/lib/greenmask/greenmask /usr/bin

RUN apt-get update && apt-get install -y wget gnupg2 bash-completion
RUN echo "deb https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
#RUN echo "deb https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list
#RUN wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | tee /etc/apt/trusted.gpg.d/pgdg.asc

RUN apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests  \
    postgresql-client-16 \
    postgresql-client-15 \
    postgresql-client-14 \
    postgresql-client-13 \
    postgresql-client-12 \
    postgresql-client-11

RUN mkdir ~/.bash_completions && \
    greenmask completion bash > /root/.bash_completions/greenmask.bash && \
    echo 'source /etc/bash_completion' >> /root/.bashrc && \
    echo 'source /root/.bash_completions/greenmask.bash' >> /root/.bashrc

CMD ["bash"]
