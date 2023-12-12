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

cd $TMP_DIR/postgresDBSamples/adventureworks

if ! psql -lqt -p 5432 -h playground-db -U postgres | cut -d \| -f 1 | grep -qw $ORIGINAL_DB_NAME; then
  psql -p 5432 -h playground-db -U postgres -c "CREATE DATABASE $ORIGINAL_DB_NAME;"
  psql -p 5432 -h playground-db -U postgres -d $ORIGINAL_DB_NAME < install.sql
else
  echo "database \"$ORIGINAL_DB_NAME\" has been already created: skipping"
fi

if ! psql -lqt -p 5432 -h playground-db -U postgres | cut -d \| -f 1 | grep -qw $TRANSFORMED_DB_NAME; then
  psql -p 5432 -h playground-db -U postgres -c "CREATE DATABASE $TRANSFORMED_DB_NAME;"
else
  echo "database \"$TRANSFORMED_DB_NAME\" has been already created: skipping"
fi
