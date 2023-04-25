# Greenmask

Stateless util for logical backup and data masking that backward compatible with pg_dump directory format

# Description

Greenfuscator is developing for simplifying process of staging environment deployment.
In one hand it has the masking and obfuscation features that may be declared in the config
in another hand it has backward compatibility with pg_dump directory backup format
that allows you to restore anything you want using pg_restore util.

# Components

* Util interface - ordinary command line interface that proxying mostly of pg_dump parameters
* TOC:
  * ArchiveHandler - Parser for TOC files in pg_dump directory with read/write function. 
    It implements TOC binary format that contains statement definition, dependencies and 
    another meta-information. The algorithm base on original pg_dump implementation rewritten 
    in GO
  * Entry - Simple structure that describes TOC file entry 
* PgDump - implements util for calling pg_dump using parameters that passed via Options
* Domains:
  * Config - simple YAML/ENV config with required params
  * Column - describes table column with assigned Masker 
    object that perform masking for that column
  * Table - describes table and contains attributes: Schema, Name, Columns and some meta-data
  * Tuple - instance of table record. Contains table pointer, original and masked tuple in bytes
* Masker - Interface that receive attribute value by string and returns transformed values
* Runner - parallel backup maker


TODO:
* Provide useful interface using cobra + viper
    * greenmask dump -c config.yml --any-params
    * greenmask restore --any-params {{ dumpId }}
    * greenmask dump-fetch {{ dumpId }} {{ dir }} 
    * greenmask dump-list --detail --json
    * greenmask delete {{ dumpId }}
    * greenmask s3 [ls|rm|get|push]
* Implement toc Read and Write suitable for any major versions
* Provide to pg_dump calls snapshot name in order to export it
* Implement concurrently COPY command
* Make json-like metadata that contains data from toc.dat file
* Implement graceful shutdown
  * Mark dump as failed
  * Terminate COPY commands
  * Delete tmp data from local storage
  * Terminate another forked processes if they are still alive
* Validate passed config with transformers
  * Tables exist
  * Columns exists
  * Transformer is suitable for type
  * Transformer is suitable for column constraints, multi-column constraints
* Unit tests
* Integration tests
  * For every supported major version of postgresql
* Python lib for working with greenmask
  * Receiving metadata as a parameter
  * Have a structures
    * Table
      * Name
      * Schema
    * Columns []:
      * Name
      * Type
      * Constraints
      * IsNull
      * Constraints
      * Unique
