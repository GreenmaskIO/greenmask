# Parameters environment variables interpolation

## Description

Greenmask supports environment variable interpolation in transformer parameter values using
[POSIX Parameter Expansion](https://github.com/buildkite/interpolate) syntax. This lets you
inject secrets or environment-specific values into transformer parameters without hardcoding
them in the config file.

By default, interpolation is **disabled** for transformer `params` to avoid unintentional
expansion of literal `$` strings that may exist in the data being processed (e.g. a config
column that stores shell scripts or template text). To opt in, set `resolve_env: true` on
the specific transformer.

## Syntax

| Syntax | Description |
|---|---|
| `${VAR}` or `$VAR` | Replaced with the value of `VAR`; empty string if `VAR` is unset. |
| `${VAR:-default}` | Replaced with the value of `VAR`, or `"default"` if `VAR` is unset or empty. |
| `${VAR-default}` | Replaced with the value of `VAR`, or `"default"` if `VAR` is unset (but not if empty). |
| `${VAR:-}` | Replaced with an empty string when `VAR` is unset or empty (explicit empty default). |
| `${VAR?message}` | Replaced with the value of `VAR`; Greenmask exits with `message` if `VAR` is unset. |
| `$$VAR` | Escape sequence — produces the literal string `$VAR` without any env lookup. |

## Usage

Add `resolve_env: true` to the transformer configuration:

```yaml
transformers:
  - name: "Replace"
    resolve_env: true   # enable env var interpolation for this transformer's params
    params:
      value: "${NEW_PASSWORD}"
      column: "password"
```

!!! warning

    To apply env vars interpolation set `resolve_env: true` on the specific transformer.
    Without this flag, parameter values containing `$` are treated as plain strings.

## Example

### Schema

```sql
create table test (password text);
insert into test (password) values ('secure');
```

### Configuration

```yaml title="config.yml"
dump:
  transformation:
    - schema: "public"   # Table schema
      name: "test"       # Table name
      transformers:      # List of transformers to apply
        - name: "Replace"    # Transformer name
          resolve_env: true  # Enable env var interpolation for params
          params:            # Transformer parameters
            value: "${NEW_PASSWORD}"
            column: "password"   # Column to replace
```

### Running

```bash
export NEW_PASSWORD="s3cr3t!"
greenmask dump ...
```

The `password` column in every dumped row will be replaced with the value of `NEW_PASSWORD`
resolved at dump time.

!!! tip

    Use `${VAR?your error message}` to make a variable required. Greenmask will exit with
    the provided message if the variable is not set, making misconfiguration explicit.
