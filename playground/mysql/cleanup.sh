#!/bin/bash

echo "Cleaning up MySQL databases..."
mysql --user $MYSQL_USER --database $TRANSFORMED_DB_NAME --host $TRANSFORMED_DB_HOST --port $TRANSFORMED_DB_PORT -e "DROP DATABASE IF EXISTS employees;"