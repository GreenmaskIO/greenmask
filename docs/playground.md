# Greenmask Playground

![Demo](assets/tapes/playground.gif)

Greenmask Playground is a sandbox environment in Docker with sample databases included to help you try Greenmask without any additional actions. It includes the following components:

* **Original database** — the source database you'll be working with.
* **Empty database for restoration** — an empty database where the restored data will be placed.
* **MinIO storage** — used for storage purposes.
* **Greenmask Utility** — Greenmask itself, ready for use.

!!! warning

    To complete this guide, you must have **Docker** and **docker-compose** installed.

## Setting up Greenmask Playground

1. Clone the `greenmask` repository and navigate to its directory by running the following commands:

    ```shell
    git clone git@github.com:GreenmaskIO/greenmask.git && cd greenmask
    ```

2. Once you have cloned the repository, start the environment by running Docker Compose:

    ```shell
    docker-compose run greenmask
    ```
!!! Tip

    If you're experiencing problems with pulling images from Docker Hub, you can build the Greenmask image from source by running the following command:

    ```shell
    docker-compose run greenmask-from-source
    ```

Now you have Greenmask Playground up and running with a shell prompt inside the container. All further operations will be carried out within this container's shell.

## Playground Workflow

To simplify your experience in the playground, use the following workflow:

1.  **Connect to the original DB**

    Connect to the original database to create schema and insert data. There are some aliases, for example `psql_o`, to connect psql to the original DB:

    ```shell
    psql_o
    ```

2.  **Edit Configuration**

    You can edit `playground/config.yml` using any editor in your host machine or container (the file is located in a docker volume so the changes you made in the host machine will be available in the container as well).

3.  **Dump original DB**

    ```shell
    greenmask --config config.yml dump
    ```

4.  **Restore into transformed DB**

    ```shell
    greenmask --config config.yml restore latest
    ```

5.  **Verify the result**

    Run `psql_t` and verify the result of subset:

    ```shell
    psql_t
    ```



## Useful Aliases

There are some aliases available to simplify your work:

* `psql_o` — connect to the original DB via psql.
* `psql_t` — connect to the transformed DB via psql.
* `cleanup` — drop and create transformed databases.

## Commands


Below you can see Greenmask commands:

* `dump` — performs a logical data dump, transforms the data, and stores it in the designated storage.

* `list-dumps` — retrieves a list of all stored dumps within the chosen storage.

* `delete` — removes a dump with a specific ID from the storage.

* `list-transformers` — displays a list of approved transformers and their documentation.

* `restore` — restores a dump either by specifying its ID or using the latest available dump to the target database.

* `show-dump` — presents metadata information about a specific dump (equivalent to `pg_restore -l ./`).

* `validate` — executes a validation process and generates a data diff for the transformation.

* `completion` — generates the autocompletion script for the specified shell.

To learn more about them, see [Commands](commands/index.md).

## Transformers

A configuration file is mandatory for Greenmask functioning. The pre-defined configuration file is stored at the repository root directory (`./playground/config.yml`). It also serves to define transformers which you can update to your liking in order to use Greenmask Playground more effectively and to get better understanding of the tool itself. To learn how to customize a configuration file, see [Configuration](configuration.md)

The pre-defined configuration file uses the [NoiseDate](built_in_transformers/standard_transformers/noise_date.md) transformer as an example. To learn more about other transformers and how to use them, see [Transformers](built_in_transformers/index.md).
