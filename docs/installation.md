---
title: Installation
description: Install Greenmask via install script, pre-built binaries, Docker, Homebrew, or build from source. Requires PostgreSQL utilities matching your target server's major version.
keywords: [install greenmask, greenmask docker, greenmask homebrew, greenmask binary, "Enterprise support", "Open-Source", "PostgreSQL anonymization", "test data management", "compliance", "security", "agentic pipeline", "development cycle"]
---

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

### Install script options

The script takes command-line flags only. It doesn't read any environment variables.

| Flag                  | Default          | Description                                                                 |
|-----------------------|------------------|-----------------------------------------------------------------------------|
| `-v`, `--version VER` | `latest`         | Release tag to install, including the `v` prefix, for example `v0.2.26`.    |
| `-b`, `--bin-dir DIR` | `/usr/local/bin` | Directory to install the `greenmask` binary into. It must already exist.    |
| `-y`, `--yes`         | off              | Skip the confirmation prompt.                                               |
| `--debug`             | off              | Print every command the script runs (`set -x`).                             |
| `-h`, `--help`        | —                | Show help and exit.                                                         |

To pass flags when piping the script into `sh`, use `sh -s --`:

```shell
# Pin a specific version
curl -fsSL https://greenmask.io/install.sh | sh -s -- --version v0.2.26

# Pin a version and install into a user directory (no sudo needed)
mkdir -p "$HOME/.local/bin"
curl -fsSL https://greenmask.io/install.sh | sh -s -- --version v0.2.23 --bin-dir "$HOME/.local/bin"
```

What the script does:

* **Platform detection.** It reads `uname -s` and `uname -m`. Supported OS: `linux`, `darwin`. Supported architectures: `amd64` (`x86_64`), `arm64` (`aarch64`), `armv7`, `armv6`, `ppc64le`, `riscv64`, `s390x`. Any other OS or architecture stops the script with an error. Release archives for `darwin` exist only for `amd64` and `arm64`.
* **Version selection.** With `latest`, it downloads from `https://github.com/GreenmaskIO/greenmask/releases/latest/download/`. With `--version`, it downloads from `https://github.com/GreenmaskIO/greenmask/releases/download/<VER>/`. The value is used as the tag as is, so `0.2.26` without the `v` fails.
* **Download.** It fetches `greenmask-<os>-<arch>.tar.gz` with `curl` (HTTPS only, TLS 1.2+) or, if `curl` is missing, with `wget`, and extracts it into a temporary directory that is removed on exit.
* **Checksum.** It compares the SHA-256 of the extracted binary (computed with `sha256sum` or `shasum -a 256`) with the `greenmask.sha256` file shipped inside the same archive, and stops on a mismatch. This catches a corrupted download or extraction. It is not an independent authenticity check, because the checksum comes from the same archive as the binary and releases don't publish a separate checksum or signature file.
* **Install.** It installs the binary as `<bin-dir>/greenmask` with mode `0755`. If the directory isn't writable, it uses `sudo`, and fails if `sudo` isn't available.
* **Prompt.** It asks for confirmation before installing unless `--yes` is set or standard input isn't a terminal. When piped (`curl ... | sh`) or run in CI, it never prompts.
* **After install.** It warns if `<bin-dir>` isn't in `PATH` or if another `greenmask` binary shadows the new one. To uninstall, remove `<bin-dir>/greenmask`.

## Via pre-built binary

Each [GitHub release](https://github.com/GreenmaskIO/greenmask/releases) publishes one archive per platform:

* Linux: `greenmask-linux-amd64.tar.gz`, `greenmask-linux-arm64.tar.gz`, `greenmask-linux-armv6.tar.gz`, `greenmask-linux-armv7.tar.gz`, `greenmask-linux-ppc64le.tar.gz`, `greenmask-linux-riscv64.tar.gz`, `greenmask-linux-s390x.tar.gz`
* macOS: `greenmask-darwin-amd64.tar.gz`, `greenmask-darwin-arm64.tar.gz`
* Windows: `greenmask-windows-amd64.zip`, `greenmask-windows-arm64.zip`

Each archive contains the `greenmask` binary (`greenmask.exe` on Windows), `LICENSE`, and a checksum file for the binary (`greenmask.sha256` or `greenmask.exe.sha256`). There is no separate checksum file among the release assets.

To download, verify, and install a specific release on Linux (amd64):

```shell
VERSION=v0.2.26
ASSET=greenmask-linux-amd64.tar.gz

mkdir greenmask-dl && cd greenmask-dl
curl -fsSLO "https://github.com/GreenmaskIO/greenmask/releases/download/${VERSION}/${ASSET}"
tar xzf "${ASSET}"

# Verify the binary against the checksum shipped in the archive
sha256sum -c greenmask.sha256    # prints: greenmask: OK

sudo install -m 0755 greenmask /usr/local/bin/greenmask
greenmask --version
```

Replace `VERSION` with any other tag, for example `v0.2.23`, and `ASSET` with the archive for your platform. On macOS, use `shasum -a 256 -c greenmask.sha256` instead of `sha256sum`.

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

:::info

For pre-releases (rc, beta, etc.), use explicit tags like `v0.2.0b2`.
:::

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