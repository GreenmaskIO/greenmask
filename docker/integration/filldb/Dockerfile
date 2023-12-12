FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

ENV PGPASSWORD=example
ENV FILE_DUMP="demo-big-en.zip"
ENV TMP_DIR=/tmp/schema

RUN apt-get update && apt-get install -y wget && mkdir /tmp/schema

RUN apt-get install --yes --no-install-recommends --no-install-suggests  \
    postgresql \
    postgresql-contrib


COPY filldb.sh /filldb.sh

RUN chmod +x ./filldb.sh

CMD ["./filldb.sh"]
