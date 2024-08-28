FROM postgres:latest

ARG DEBIAN_FRONTEND=noninteractive

ENV PGPASSWORD=example
ENV FILE_DUMP="demo-big-en.zip"
ENV TMP_DIR=/tmp/schema

RUN apt-get update && apt-get install -y wget && mkdir /tmp/schema

COPY . /

RUN chmod +x ./filldb.sh

CMD ["./filldb.sh"]
