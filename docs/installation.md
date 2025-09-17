# Installation

## Prerequisites

* Ensure that you have PostgreSQL utilities preinstalled, matching the **major version**
  of your destination server.

* If you are building Greenmask from source, make sure you have the `make` utility installed.

## Via install script

You can quickly install Greenmask using the official install script:

```shell
curl -fsSL https://greenmask.io/install.sh | sh
```

Options can be passed directly to the installation script. For example, to see the available options:

```shell
curl -fsSL -o install.sh https://greenmask.io/install.sh
sh install.sh --help
```

> **Note:** Windows is not supported yet. The install script works on Linux and macOS.

## Via docker

You can find the docker images in the:

1. [Docker-hub page](https://hub.docker.com/r/greenmask/greenmask)

To run the greenmask container from DockerHub, use the following command:
```shell
docker run -it greenmask/greenmask:latest
```

2. GitHub container registry 

To run the greenmask container from GitHub registry, use the following command:
```shell
docker run -it ghcr.io/greenmaskio/greenmask:latest
```

!!! info
    
    For pre-releases (rc, beta, etc.), use explicit tags like `v0.2.0b2`.

## Via brew 

The greenmask build is [available in brew](https://formulae.brew.sh/formula/greenmask#default), 
but only a production build is available. To install the greenmask via brew, use the following command:

```shell
brew install greenmask
```

## From source

1. Clone the Greenmask repository by using the following command:

    ```bash
    git clone git@github.com:GreenmaskIO/greenmask.git
    ```

2. Once the repository is cloned, execute the following command to build Greenmask:

    ```bash
    make build
    ```

After completing the build process, you will find the binary named `greenmask` in the root directory of the repository.
Execute the binary to start using Greenmask.

## Playground

Greenmask Playground is a sandbox environment for your experiments in Docker with sample databases included to help you
try Greenmask without any additional actions. Read the [Playground](playground.md) guide to learn more.