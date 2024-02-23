# Installation

## Prerequisites

* Ensure that you have PostgreSQL utilities preinstalled, matching the **major version**
  of your destination server.

* If you are building Greenmask from source, make sure you have the `make` utility installed.

## From GitHub binaries

The easiest way to install Greenmask is by using the latest release's binary. Follow these steps:

1. Check the latest [Greenmask release](https://github.com/GreenmaskIO/greenmask/releases).
2. From **Assets**, download the required binary.
3. Execute the downloaded binary to start using Greenmask.

### Additional instructions for macOS users

For those downloading `greenmask-macos-amd64` or `greenmask-macos-arm64`, additional steps are required to ensure proper execution.

1. In your terminal, move to the directory where the Greenmask binary is located.

2. Change the file permissions to make it executable by using the following command:

    ```bash
    chmod 777 greenmask-macos-[version]
    ```

3. Remove a quarantine attribute, which macOS may have applied, by using the following command:

    ```bash
    xattr -d com.apple.quarantine greenmask-macos-[version]
    ```
    !!! info

        In both commands above, replace `[version]` with `amd64` or `arm64` according to your download.

## From source

1. Clone the Greenmask repository by using the following command:

    ```bash
    git clone git@github.com:GreenmaskIO/greenmask.git
    ```

2. Once the repository is cloned, execute the following command to build Greenmask:

    ```bash
    make build
    ```

After completing the build process, you will find the binary named `greenmask` in the root directory of the repository. Execute the binary to start using Greenmask.
