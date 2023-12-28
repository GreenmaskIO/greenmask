#!/bin/bash
# Copyright 2023 Greenmask
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cd $TMP_DIR
if [ ! -f $FILE_DUMP ]; then
    wget https://edu.postgrespro.com/$FILE_DUMP
fi
IFS="," read -ra PG_VERSIONS_CHECK <<< "${PG_VERSIONS_CHECK}"
for pgver in ${PG_VERSIONS_CHECK[@]}; do
  if  psql -p 5432 -h db-$pgver -U postgres -c 'CREATE DATABASE demo;'; then
    psql -p 5432 -h db-$pgver -U postgres -c 'DROP DATABASE demo_restore;'
    psql -p 5432 -h db-$pgver -U postgres -c 'CREATE DATABASE demo_restore;'
    gzip -dc $FILE_DUMP | psql -p 5432 -h db-$pgver -U postgres -d demo
  fi
done
