#!/bin/bash
echo "alias psql='psql -U postgres -d $ORIGINAL_DB_NAME -h playground-db'" >> /root/.bashrc
echo "alias psql_o='psql -U postgres -d $ORIGINAL_DB_NAME -h playground-db'" >> /root/.bashrc
echo "alias psql_t='psql -U postgres -d $TRANSFORMED_DB_NAME -h playground-db'" >> /root/.bashrc
echo "alias cleanup='/var/lib/playground/cleanup.sh'" >> /root/.bashrc
bash
