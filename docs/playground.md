# Greenmask Playground

![Demo](assets/tapes/playground.gif)

Greenmask Playground is a sandbox environment in Docker with sample databases included to help you try Greenmask without any additional actions. It includes the following components:

* **Original database** — the source database you'll be working with.
* **Empty database for restoration** — an empty database where the restored data will be placed.
* **MinIO storage** — used for storage purposes (an Azurite-based Azure Blob backend is also available — see [Using the Azure Blob storage backend](#using-the-azure-blob-storage-backend) — as well as an SFTP backend — see [Using the SSH storage backend](#using-the-ssh-storage-backend)).
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

    Alternatively, you can use the `make` target, which wraps the same command:

    ```shell
    make greenmask-latest
    ```
!!! Tip

    If you're experiencing problems with pulling images from Docker Hub, you can build the Greenmask image from source by running the following command:

    ```shell
    docker-compose run greenmask-from-source
    ```

    Or with `make`:

    ```shell
    make greenmask-from-source
    ```

Now you have Greenmask Playground up and running with a shell prompt inside the container. All further operations will be carried out within this container's shell.

## Using the Azure Blob storage backend

By default the playground stores dumps in a MinIO (S3-compatible) backend. You can instead run it against an [Azurite](https://github.com/Azure/Azurite) emulator to exercise the `azure` storage backend with a full dump/restore cycle.

Pass `STORAGE_BACKEND=azure` to the `make` targets to switch the backend:

```shell
# Run with the published image against Azure Blob storage
make greenmask-latest STORAGE_BACKEND=azure

# Or build from source and run against Azure Blob storage
make greenmask-from-source STORAGE_BACKEND=azure
```

The `STORAGE_BACKEND` parameter defaults to `s3`, so omitting it (or passing `STORAGE_BACKEND=s3`) uses the default MinIO (S3) backend.

Inside the container, the matching configuration is mounted as `config-azure.yml`:

```shell
greenmask --config config-azure.yml dump
greenmask --config config-azure.yml restore latest
```

!!! Tip

    If you prefer to invoke Docker Compose directly instead of the `make` targets, run:

    ```shell
    docker compose -f docker-compose.yml -f docker-compose-azure.yml run greenmask
    ```

    `docker-compose-azure.yml` is an override that swaps the `playground-storage` service for an Azurite emulator, so the base file must be passed first.

## Using the SSH storage backend

You can also run the playground against an [atmoz/sftp](https://github.com/atmoz/sftp) SFTP server to exercise the `ssh` storage backend with a full dump/restore cycle.

Pass `STORAGE_BACKEND=ssh` to the `make` targets to switch the backend:

```shell
# Run with the published image against the SFTP server
make greenmask-latest STORAGE_BACKEND=ssh

# Or build from source and run against the SFTP server
make greenmask-from-source STORAGE_BACKEND=ssh
```

The `STORAGE_BACKEND` parameter defaults to `s3`, so omitting it (or passing `STORAGE_BACKEND=s3`) uses the default MinIO (S3) backend.

Inside the container, the matching configuration is mounted as `config-ssh.yml`:

```shell
greenmask --config config-ssh.yml dump
greenmask --config config-ssh.yml list-dumps
greenmask --config config-ssh.yml restore latest
```

!!! Tip

    If you prefer to invoke Docker Compose directly instead of the `make` targets, run:

    ```shell
    docker compose -f docker-compose.yml -f docker-compose-ssh.yml run greenmask
    ```

    `docker-compose-ssh.yml` is an override that swaps the `playground-storage` service for an atmoz/sftp SFTP server, so the base file must be passed first.

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
