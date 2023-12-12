# Getting started

This page will walk you through the easiest way to get started with Greenmask for the first time. To complete this
guide, you will need to have **Docker** and **docker-compose** installed.

## About the Playground

The **Greenmask Playground** is a Docker Compose environment that includes the following components:

* **Original Database**: This is the source database you'll be working with.
* **Empty Database for Restoration**: An empty database where the restored data will be placed.
* **MinIO Storage**: Used for storage purposes.
* **Greenmask Utility**: The Greenmask tool itself, ready for use.

## Starting the Playground

To begin, follow these steps to set up the Greenmask Playground:

Clone the `greenmask` repository and navigate to the `greenmask` directory using the following commands:

```shell
git clone git@github.com:GreenmaskIO/greenmask.git && cd greenmask
```

Once you have cloned the repository, run `docker-compose` to start the environment:

```shell
docker-compose run greenmask
```

If you're experiencing problems with pulling images from Docker Hub, you can build the Greenmask image from source by
executing the following command:

```shell
docker-compose run greenmask-from-source
```

After executing these commands, you will have Greenmask up and running with a shell prompt inside the container. All
further operations will be carried out within this container's shell.

## Commands

Before proceeding to configure Greenmask, let's explore some of the available Greenmask commands.

```shell
greenmask 
  --log-format=[json|text] \ 
  --log-level=[debug|info|error] \
  --config=config.yml \ 
  [dump | list-dumps | delete | list-transformers | restore | show-dump | validate | completion]
```

Available Actions and Descriptions:

* **list-transformers**: Display a list of approved transformers along with their corresponding documentation.

* **validate**: Execute a validation process and generate a data diff for the transformation.

* **dump**: Perform a logical data dump, transform the data, and store it in the designated storage.

* **list-dumps**: Retrieve a list of all stored dumps within the chosen storage.

* **show-dump**: Present metadata information about a specific dump (equivalent to `pg_restore -l ./`).

* **restore**: Restore a dump either by specifying its ID or using the latest available dump to the target database.

* **delete**: Remove a dump with a specific ID from the storage.

* **completion**: Generate the autocompletion script for the specified shell

Please note that you can customize the logging format and level using the provided options, and you **must specify** a
configuration file (config.yml) to guide the tool's behavior.

## Building config

### The sample database

The Greenmask Playground utilizes
the [Microsoft AdventureWorks sample databases](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms),
which have been ported to PostgreSQL and sourced
from [morenoh149/postgresDBSamples](https://github.com/morenoh149/postgresDBSamples).

Within the playground, you'll find two predefined databases:

```text                  
    Name     |  Owner    
-------------+----------
 original    | postgres 
 transformed | postgres 
```

* **original** - This database contains the deployed AdventureWorks sample databases as-is.
* **transformed** - An empty database intended for restoring transformed dumps.

Within the **Greenmask container**, you'll have access to the following commands, which will be used throughout this
guide:

* `greenmask` - Launches the Greenmask obfuscation tool utility.
* `psql_o` - Connects to the `original` database using the psql utility.
* `psql_t` - Connects to the `transformed` database using the psql utility.
* `cleanup` - dDrops and recreates the `transformed` database as an empty container.

If you are using an external Integrated Development Environment (IDE), you can connect using the following connection
URIs:

* Original database: `postgresql://postgres:example@localhost:54316/original`
* Transformed database: `postgresql://postgres:example@localhost:54316/transformed`

### Creating a Simple Configuration

The Greenmask utility container is configured with a volume attached to the `./playground` directory located at the root
of the repository. Within this directory, there is a pre-defined configuration file called `config.yml`. You have the
flexibility to modify this configuration as needed, making adjustments or adding additional transformations.

Any changes made to this configuration file will be accessible within the container, allowing you to tailor Greenmask's
behavior to your specific requirements.

To build a basic configuration for Greenmask, you can follow these steps:

1. Obtain a list of currently available transformers by using the following command:

```shell
greenmask --config config.yml list-transformers
```

![img.png](docs/resources/list-transformers-example.png)

When building your configuration, ensure that you fill in all the required attributes, including the following sections:

* common
* storage
* dump
* restore

Below is an example of a minimal configuration in YAML format:

```yaml
common:
  pg_bin_path: "/usr/lib/postgresql/16/bin"
  tmp_dir: "/tmp"

storage:
  s3:
    endpoint: "http://storage:9000"
    bucket: "adventureworks"
    region: "us-east-1"
    access_key_id: "Q3AM3UQ867SPQQA43P2F"
    secret_access_key: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"

validate:
#  resolved_warnings:
#    - "aa808fb574a1359c6606e464833feceb"

dump:
  pg_dump_options: # pg_dump option that will be provided
    dbname: "host=db user=postgres password=example dbname=original"
    jobs: 10

  transformation: # List of tables to transform
    - schema: "humanresources" # Table schema
      name: "employee"  # Table name
      transformers: # List of transformers to apply
        - name: "NoiseDate" # name of transformers
          params: # Transformer parameters
            ratio: "10 year 9 mon 1 day"
            column: "birthdate" # Column parameter - this transformer affects scheduled_departure column

restore:
  pg_restore_options: # pg_restore option (you can use the same options as pg_restore has)
    jobs: 10
    dbname: "host=db user=postgres password=example dbname=transformed"
```

This example demonstrates the essential components of a Greenmask configuration file in YAML format. Please ensure that
you customize it according to your specific needs.

In the config above applied only one transformer on table `humanresources.employee` called `NoiseDate` with the
next parameters:

* ratio - add noise to the value up to "10 year 9 mon 1 day" eather before or after. For he current value is
  `1976-12-03` and the transformer generated the noise value randomly `1 year 3 mon` and decided to increase that value.
  The result will be `1978-02-033`
* column - there is a column name that is going to be affected called `birthdate`

### Run validation procedure

You can utilize the following command to initiate a validation procedure:

```shell
greenmask --config config.yml validate --data --diff --format vertical --rows-limit=2
```

The validation result will be displayed as follows:

![img.png](docs/resources/validate-result.png)

There is one warning; let's investigate it:

```yaml
{
  "hash": "aa808fb574a1359c6606e464833feceb",
  "meta": {
    "ColumnName": "birthdate",
    "ConstraintDef": "CHECK (birthdate >= '1930-01-01'::date AND birthdate <= (now() - '18 years'::interval))",
    "ConstraintName": "humanresources",
    "ConstraintSchema": "humanresources",
    "ConstraintType": "Check",
    "ParameterName": "column",
    "SchemaName": "humanresources",
    "TableName": "employee",
    "TransformerName": "NoiseDate"
  },
  "msg": "possible constraint violation: column has Check constraint",
  "severity": "warning"
}
```

The validation warnings include the following details:

* **hash** - A unique identifier for each validation warning, which can be used to exclude the warning from future
  checks
  by adding it to the `validate.resolved_warnings` configuration.
* **meta** - Contains essential information that helps identify the location in the configuration or the potentially
  violated constraint.
* **msg** - A comprehensive message that provides a detailed explanation of the warning's cause
* **severity** - Indicates the severity of the warning, which can be either "warning" or "error." In the case of an
  error, Greenmask will exit immediately with a non-zero exit code.

The next step in the validation procedure is to compare the data before and after the transformation. This comparison
is presented in a table format. Columns with a red background indicate that they have been affected by the
transformation. The green values represent the original data before the transformation, while the red values depict
the data after the transformation.

To exclude a warning from future runs, you can uncomment the resolved_warning attribute in the configuration file.

```yaml
validate:
  resolved_warnings:
    - "aa808fb574a1359c6606e464833feceb"
```

By adding the hash of a warning to the `validate.resolved_warnings` configuration in your `config.yml`
file, you can effectively exclude that specific warning from being displayed in subsequent runs of the validation
process using the command:

```shell
greenmask --config config.yml validate
```

### Dumping procedure

To perform the data dumping procedure, follow these steps:

1. Execute the following command to initiate the dump using your configured settings:
    ```shell
    greenmask --config config.yml dump
    ```

2. Once the dumping process is complete, you will find the dump with an associated ID in the designated storage.
   To list all available dumps, use the following command:
    ```shell
    greenmask --config config.yml list-dumps
    ```
   ![img.png](docs/resources/list-dumps.png)

3. If you wish to examine the data that is scheduled for restoration, you can use the show dump command.
   Provide the `dumpId` in your call to access the details:
   ```shell
    greenmask --config config.yml show-dump 1702489882319
   ```
   In the output below, you can observe the portion of objects that will be restored:
    ```shell 
    ;
    ; Archive created at 2023-12-13 17:51:22 UTC
    ;     dbname: original
    ;     TOC Entries: 986
    ;     Compression: 0
    ;     Dump Version: 16.1 (Ubuntu 16.1-1.pgdg22.04&#43;1)
    ;     Format: DIRECTORY
    ;     Integer: 4 bytes
    ;     Offset: 8 bytes
    ;     Dumped from database version: 16.0 (Debian 16.0-1.pgdg120&#43;1)
    ;     Dumped by pg_dump version: 16.1 (Ubuntu 16.1-1.pgdg22.04&#43;1)
    ;
    ;
    ; Selected TOC Entries:
    ;
    4666; 0 0 ENCODING - ENCODING
    4667; 0 0 STDSTRINGS - STDSTRINGS
    4668; 0 0 SEARCHPATH - SEARCHPATH
    4669; 1262 16384 DATABASE - original postgres
    14; 2615 18396 SCHEMA - hr postgres
    9; 2615 16524 SCHEMA - humanresources postgres
    4670; 0 0 COMMENT - SCHEMA humanresources postgres
    13; 2615 18343 SCHEMA - pe postgres
    8; 2615 16429 SCHEMA - person postgres
    4671; 0 0 COMMENT - SCHEMA person postgres
    15; 2615 18421 SCHEMA - pr postgres
    10; 2615 16586 SCHEMA - production postgres
    4672; 0 0 COMMENT - SCHEMA production postgres
    16; 2615 18523 SCHEMA - pu postgres
    11; 2615 17034 SCHEMA - purchasing postgres
    4673; 0 0 COMMENT - SCHEMA purchasing postgres
    
   ...
   ...
   ...
   4427; 2606 18157 FK CONSTRAINT sales shoppingcartitem FK_ShoppingCartItem_Product_ProductID postgres
   4428; 2606 18162 FK CONSTRAINT sales specialofferproduct FK_SpecialOfferProduct_Product_ProductID postgres
   4429; 2606 18167 FK CONSTRAINT sales specialofferproduct FK_SpecialOfferProduct_SpecialOffer_SpecialOfferID postgres
   4430; 2606 18182 FK CONSTRAINT sales store FK_Store_BusinessEntity_BusinessEntityID postgres
   4431; 2606 18187 FK CONSTRAINT sales store FK_Store_SalesPerson_SalesPersonID postgres

    ```

### Restoration Procedure

To restore data to the target database, you can use the following commands:

1. To restore data from a specific dump (identified by its **dumpId**), execute the following command:
   ```shell
      greenmask --config config.yml restore [dumpId]
   ```
   Replace **[dumpId]** with the appropriate dump identifier.

2. Alternatively, you can restore the latest available dump by using the reserved word **latest** like this:
   ```shell
    greenmask --config config.yml restore latest
   ```

3. After the restoration process is complete, you can verify the restored data by running the following PostgreSQL
   command:
   ```shell
    psql_t -xc 'select * from humanresources.employee limit 2;'
   ```
   This command will display the first two rows of the "flights" table in the target database, showing the restored
   data.

    ```
    -[ RECORD 1 ]----+-------------------------------------
    businessentityid | 1
    nationalidnumber | 295847284
    loginid          | adventure-works\ken0
    jobtitle         | Chief Executive Officer
    birthdate        | 1968-12-18
    maritalstatus    | S
    gender           | M
    hiredate         | 2009-01-14
    salariedflag     | t
    vacationhours    | 99
    sickleavehours   | 69
    currentflag      | t
    rowguid          | f01251e5-96a3-448d-981e-0f99d789110d
    modifieddate     | 2014-06-30 00:00:00
    organizationnode | /
    -[ RECORD 2 ]----+-------------------------------------
    businessentityid | 2
    nationalidnumber | 245797967
    loginid          | adventure-works\terri0
    jobtitle         | Vice President of Engineering
    birthdate        | 1970-05-04
    maritalstatus    | S
    gender           | F
    hiredate         | 2008-01-31
    salariedflag     | t
    vacationhours    | 1
    sickleavehours   | 20
    currentflag      | t
    rowguid          | 45e8f437-670d-4409-93cb-f9424a40d6ee
    modifieddate     | 2014-06-30 00:00:00
    organizationnode | /1/

    ```

### Deleting a Dump

To remove a specific dump from the storage, use the **delete** command with the appropriate **dumpId**.
Here's how to do it:

```shell
greenmask --config config.yml delete 1702489882319
```

After executing this command, the specified dump will be deleted from the storage.

To verify the changes, you can list the available dumps using the following command:

The result

```shell
greenmask --config config.yml list-dumps
```

The list displayed dumps will not include the deleted dump with the previously provided dumpId.

```
+----+------+----------+------+-----------------+----------+-------------+--------+
| ID | DATE | DATABASE | SIZE | COMPRESSED SIZE | DURATION | TRANSFORMED | STATUS |
+----+------+----------+------+-----------------+----------+-------------+--------+
+----+------+----------+------+-----------------+----------+-------------+--------+
```

## Conclusion

This is a straightforward example of using Greenmask. If you wish to explore more advanced transformation cases and
delve deeper into the documentation.

Additionally, if you have any questions or require further assistance, don't hesitate to reach out via
[Discord](https://discord.gg/97AKHdGD), [Telegram](https://t.me/greenmask_community), or by
emailing us at [support@greenmask.io](mailto:support@greenmask.io). Our team is here to help and
provide guidance as needed.

