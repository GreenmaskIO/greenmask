# Integration testing framework for MySQL

The idea is to use testify Suite to create a testing suite for MySQL.

We need to test the following features:

### Dump tests

Requirements:
* MySQL source server with data. You can run testcontainers
* mysqldump should be provided by bin path
* MinIO / Directory storage for dumps or Use in memory storage for dumps

Common verification cycle:
* Setup config
* Run dump
* Verify results in the storage

