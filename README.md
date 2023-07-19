# Greenmask

Stateless util for logical backup and data masking that backward compatible with pg_dump directory format

# Description

Greenmask is developing for simplifying process of staging environment deployment.
In one hand it has the masking and obfuscation features that may be declared in the config
in another hand it has backward compatibility with pg_dump directory backup format
that allows you to restore anything you want using pg_restore util.

# TODO:
1. Determine the way how to define the custom transformers:
   * Parameters definition
   * Settings such as unique, inline, tuple, types, include connection string
   * Initialisation procedure
   * Validation procedure and interaction protocol
   * Testing framework and the way how to test
   * Metadata model passing
* Write the Greenmask library for the Python

Models:
* Error model
  * Methods:
    * Error()
    * AddMeta(key, value)

```yaml
error:
  message: ""
  level: "info|warning|error"
  meta: {} # The kv that will be printed out in the log
```

How to initialise:
* Find transformer including statement list
  * Find all the files that has *__tdef.yml
  * In every file must be set the version of the API
  * Find the executable
  * If transformer supports validation run it with --validate and provide the required metadata
    * Listen to stderr
    * If stdout contains smth raise make a notice that transformer send info to stdout unexpectedly
    * If stderr contains json that would be casted explicitly to the warning model that cast 
      it and determine the error level 


Discussing issues:
* Where does transformers stores?
* How they would be gathered and installed?
* Where we can store the index of the existed transformers?
* 


# Components

# Backlog

TODO:
* Make json-like metadata that contains data from toc.dat file
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
