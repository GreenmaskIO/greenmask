#!/bin/bash

if psql -lqt -p 5432 -h $DATABASE_HOST -U postgres | cut -d \| -f 1 | grep -qw $TRANSFORMED_DB_NAME; then
  psql -p 5432 -h $DATABASE_HOST -U postgres -c "DROP DATABASE $TRANSFORMED_DB_NAME;"
  psql -p 5432 -h $DATABASE_HOST -U postgres -c "CREATE DATABASE $TRANSFORMED_DB_NAME;"
fi
