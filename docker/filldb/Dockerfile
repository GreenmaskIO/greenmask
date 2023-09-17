FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

ENV PGPASSWORD=example
ENV FILE_DUMP="demo-big-en.zip"
ENV TMP_DIR=/tmp/schema

RUN apt-get update && apt-get install -y wget && mkdir /tmp/schema

RUN apt-get install --yes --no-install-recommends --no-install-suggests  \
    postgresql \
    postgresql-contrib


#RUN printf "#!/bin/bash \
#\ncd $TMP_DIR \
#\nif [ ! -f $FILE_DUMP ]; then \
#\n    wget https://edu.postgrespro.com/$FILE_DUMP \
#\nfi\
#\nif  psql -U postgres -h db -c 'CREATE DATABASE demo;'; then \
#\ngzip -dc $FILE_DUMP | psql -U postgres -d demo -h db \
#\nfi\
#\npsql -U postgres -h db -c 'DROP DATABASE demo_restore;'\
#\npsql -U postgres -h db -c 'CREATE DATABASE demo_restore;'\
#" > /script.sh

COPY filldb.sh /filldb.sh

RUN chmod +x ./filldb.sh

CMD ["./filldb.sh"]
