# delete command

Delete dump from the storage with a specific ID


```text title="Supported flags"
Usage:
  greenmask delete [flags] [dumpId]

Flags:
      --before-date string   delete dumps older than the specified date in RFC3339Nano format: 2021-01-01T00:00.0:00Z
      --dry-run              do not delete anything, just show what would be deleted
      --prune-failed         prune failed dumps
      --prune-unsafe         prune dumps with "unknown-or-failed" statuses. Works only with --prune-failed
      --retain-for string    retain dumps for the specified duration in format: 1w2d3h4m5s6ms7us8ns
      --retain-recent int    retain the most recent N completed dumps (default -1)
```

```shell title="delete dump by id"
greenmask --config config.yml delete 1723643249862
```

```shell title="delete dumps older than the specified date"
greenmask --config config.yml delete --before-date 2021-01-01T00:00.0:00Z --dry-run 
```

```shell title="prune failed dumps"
greenmask --config config.yml delete --prune-failed --dry-run 
```

```shell title="prune dumps with 'unknown-or-failed' statuses"
greenmask --config config.yml delete --prune-failed --prune-unsafe --dry-run
```

```shell title="retain dumps for the specified duration"
greenmask --config config.yml delete --retain-for 1w5d --dry-run
```

```shell title="retain the most recent N completed dumps"
greenmask --config config.yml delete --retain-recent 5 --dry-run
```
