ARG DEBIAN_RELEASE=bookworm
ARG MYSQL_VERSION=8.0.44
ARG TMP_DIR=/tmp/schema

FROM mysql:${MYSQL_VERSION}-${DEBIAN_RELEASE}


ENV TMP_DIR=/tmp/schema

ENV DEBIAN_FRONTEND=noninteractive
ENV DEMODB_URL="https://github.com/datacharmer/test_db/archive/master.zip"

RUN echo 'TMP_DIR' $TMP_DIR

RUN apt-get update \
    && apt-get install -y curl wget gnupg2 bash-completion unzip

COPY filldb.sh /filldb.sh

RUN mkdir $TMP_DIR && \
    cd $TMP_DIR && \
    wget $DEMODB_URL && \
    unzip master.zip

RUN ls -la $TMP_DIR

RUN chmod +x ./filldb.sh

CMD ["./filldb.sh"]