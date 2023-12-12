FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

ENV PGPASSWORD=example
ENV TMP_DIR=/tmp/schema
ENV DEMODB_URL="https://github.com/morenoh149/postgresDBSamples"
ENV ORIGINAL_DB_NAME="original"
ENV TRANSFORMED_DB_NAME="transformed"
ENV DBHOST="db"
ENV DBUSER="postgres"
ENV DBPASSWORD="example"

RUN apt-get update && apt-get install -y wget git && mkdir /tmp/schema

RUN apt-get install --yes --no-install-recommends --no-install-suggests  \
    postgresql \
    postgresql-contrib


RUN cd $TMP_DIR && git clone $DEMODB_URL && cd $TMP_DIR/postgresDBSamples/adventureworks

COPY filldb.sh /filldb.sh

RUN chmod +x ./filldb.sh

CMD ["./filldb.sh"]
