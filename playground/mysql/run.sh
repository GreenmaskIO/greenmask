#!/bin/bash
echo "alias mysql='mysql --user $MYSQL_USER --database $ORIGINAL_DB_NAME --host $ORIGINAL_DB_HOST --port $ORIGINAL_DB_PORT'" >> ~/.bashrc
echo "alias mysql_o='mysql --user $MYSQL_USER --database $ORIGINAL_DB_NAME --host $ORIGINAL_DB_HOST --port $ORIGINAL_DB_PORT'" >> ~/.bashrc
echo "alias mysql_t='mysql --user $MYSQL_USER --database $TRANSFORMED_DB_NAME --host $TRANSFORMED_DB_HOST --port $TRANSFORMED_DB_PORT'" >> ~/.bashrc
echo "alias cleanup='/var/lib/playground/cleanup.sh'" >> ~/.bashrc
bash