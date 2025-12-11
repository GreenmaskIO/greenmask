# Commands

## Introduction

```shell title="Greenmask available commands"
greenmask \
--log-format=[json|text] \
--log-level=[debug|info|warn] \
--config=config.yml \
[dump|list-dumps|delete|list-transformers|show-transformer|restore|show-dump]`
```

You can use the following commands within Greenmask:

* [list-transformers](list-transformers.md) — displays a list of available transformers along with their documentation
* [show-transformer](show-transformer.md) — displays information about the specified transformer
* [validate](validate.md) - performs a validation procedure by testing config, comparing transformed data, identifying 
potential issues, and checking for schema changes.
* [dump](dump.md) — initiates the data dumping process
* [restore](restore.md) — restores data to the target database either by specifying a `dumpId` or using the latest available dump
* [list-dumps](list-dumps.md) — lists all available dumps stored in the system
* [show-dump](show-dump.md) — provides metadata information about a particular dump, offering insights into its structure and
    attributes
* [delete](delete.md) — deletes a specific dump from the storage


For any of the commands mentioned above, you can include the following common flags:

* `--log-format` — specifies the desired format for log output, which can be either `json` or `text`. This parameter is
optional, with the default format set to `text`.
* `--log-level` — sets the desired level for log output, which can be one of `debug`, `info`, or `warn`. This parameter
is optional, with the default log level being `info`.
* `--config` — requires the specification of a configuration file in YAML format. This configuration file is mandatory
for Greenmask to operate correctly.
* `--help` — displays comprehensive help information for Greenmask, providing guidance on its usage and available
commands.
