# Architecture

## Preface

**Greenmask** is an open-source util written on go that provides features for logical backup dumping, obfuscation and 
restoration. Brings wide functionality for backup
anonymization and masking. It is written fully in pure go with ported required PostgreSQL library, it means that you 
can easy up and running it without caring about building them with an appropriate glibc and so on. Inspired by 
[Replibyte](https://www.replibyte.com/docs/introduction/) and 
[PostgreSQL anonimyzer](https://postgresql-anonymizer.readthedocs.io/en/stable/).

**Greenmask** implements three main approaches:

* Stateless - do not affect existed schema, it is just logical backup
* Extensible - bring users possibility to implement their own domain-based transformation in any language
* Declarative - define config in structured and easy parsed format

Though the previous approaches are the main, other important highlights should be mentioned:

* Backward compatible - support the same features and protocols as existed vanilla PostgreSQL utils
* Reliable and predictable  - works in expected domains and with strict types
* Easy to maintain - provide features for transformation validation with potential warnings
* Parallel execution - dump data parallel
* Provide variety storages - storages for local and remote data storing such as Directory or S3-like

## Architecture

### Common information

It is quite clear that the right way for performing logical backup dumping and restoration is using the core PostgreSQL 
utils such as pg_dump and pg_restore. Understanding that **Greenmask** was designed as a compatible with PostgreSQL vanila
utils, it performs only data dumping features by itself and delegate schema dumping and restoration to pg_dump and 
pg_restore. 



### Backing up

PostgreSQL backup is sparated by three section:

* pre-data - raw tables schema itself excluding PK, FK
* data - contains actual table data in COPY format, sequences current value setting up and Large Objects data
* post-data - contains definition of indexes, triggers, rules, constraints (such as PK, FK) 

**Greenmask** operates in runtime only with `data` section and delegates `pre-data` and `post-data` to pg_dump and 
pg_restore. 

PostgreSQL `pg_dump` util supports a few formats:

* plain - plain SQL-text script
* custom - custom format that allows manual selection and restoration reordering
* directory - directory format of output. It has binary meta-data file called `toc.dat` that contains commands 
  definition their order and dependencies. The data stores separately from toc.dat in `dat.gz` files. Directory format
  supports partial restoration, reordering and parallel multiprocess backing up and restoration
* tar - supports the same features as `directory` but also pack the directory into `tar` archive

Since `directory` is more suitable for parallel execution, partial restoration, and it contains clear meta-data file
that would be used for determining the backup and restoration steps it was decided that we need to develop util that 
inherit all those features and slightly adapt them for working with remote storages and obfuscation procedure.

`pg_dump` performs the table data dumping using `COPY` command in `TEXT` format. Keeping that in mind COPY parser as 
well as COPY protocol has been implemented. It brings reliability and compatibility with vanilla utils as well as 
predictable parsing.


## Storages
PostgreSQL vanilla utils `pg_dump` and `pg_restore` operates with file system object and is not brigs you any 
alternatives. Understanding modern backup requirements and delivering approaches **Greenmask** introducing `Storage` 
abstraction that implements kind of interface. Currently, **Greenmask** supports only two storages:

* S3 - might be any S3-like storage, such as AWS S3
* Directory - ordinary filesystem directory

Check the roadmap and/or suggest any other storage that would be fine to implement. Wi will try to deliver it. 

## Restoration

Grenmask restores schema using pg_restore but applying COPY data by itself using COPY protocol. Due to supporting 
variety of storages and awareness of the restoration metadata, **Greenmask** may download only required data. It might be
useful in case of partial restoration, for instance restoring only one table from the whole backup.

## Obfuscation and Validation
**Grenmask** operates with COPY lines, gathers schema metadata for golang driver and using this driver for transformation 
where required. It introduces a few abstraction for transformation and allows easy extend and develop your own 
transformers suitable for your domains. Using gathered schema metadata validates schema affection and warn you about it.

## Customization
**Greenmask** implements a framework for defining your custom transformers that might be reused later. It integrates easily
without recompiling - just PIPE (stdin/stdout) interaction.

# PostgreSQL version compatibility
Though it's expected that it might be working correctly in other EOL version we maintain only version from 11 and higher.
