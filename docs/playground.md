# Greenmask Playground

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

To learn more about them, see [Commands](commands.md).

## Transformers

A configuration file is mandatory for Greenmask functioning. The pre-defined configuration file is stored at the repository root directory (`./playground/config.yml`). It also serves to define transformers which you can update to your liking in order to use Greenmask Playground more effectively and to get better understanding of the tool itself. To learn how to customize a configuration file, see [Configuration](configuration.md)

The pre-defined configuration file uses the [NoiseDate](built_in_transformers/standard_transformers/noise_date.md) transformer as an example. To learn more about other transformers and how to use them, see [Transformers](built_in_transformers/index.md).
