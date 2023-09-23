#!/bin/bash
cd $TMP_DIR
if [ ! -f $FILE_DUMP ]; then
    wget https://edu.postgrespro.com/$FILE_DUMP
fi
for pgver in "11" "12" "13" "14" "15" "16"
do
  if  psql -p 5432 -h db-$pgver -U postgres -c 'CREATE DATABASE demo;'; then
    psql -p 5432 -h db-$pgver -U postgres -c 'DROP DATABASE demo_restore;'
    psql -p 5432 -h db-$pgver -U postgres -c 'CREATE DATABASE demo_restore;'
    gzip -dc $FILE_DUMP | psql -p 5432 -h db-$pgver -U postgres -d demo
  fi
done
