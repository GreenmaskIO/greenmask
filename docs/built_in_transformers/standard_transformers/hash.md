---
title: "Hash"
description: "The following example generates a hash from the jobtitle into sha1 and truncates the results after the 10th character."
keywords: ["Hash", "SHA256", "deterministic anonymization", "one-way hash", "greenmask transformer", "data anonymization", "postgresql", "mysql", "oracle", "Enterprise support", "Open-Source", "PostgreSQL anonymization", "test data management", "compliance", "security", "agentic pipeline", "development cycle"]
---

Generate a salted hash of the text value using the selected hash `function`. The result is hex-encoded. `NULL` values are always kept, the transformer has no `keep_null` parameter.

## Parameters

| Name       | Description                                                                                                                           | Default | Required | Supported DB types |
|------------|---------------------------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                                                                                                 |         | Yes      | text, varchar      |
| salt       | Salt string written before the value into the hash. Taken from `GREENMASK_GLOBAL_SALT` if not set. Empty means no salt.               |         | No       | -                  |
| function   | Hash algorithm to anonymize data. Can be any of `md5`, `sha1`, `sha256`, `sha512`, `sha3-224`, `sha3-254`, `sha3-384`, `sha3-512`.    | `sha3-254` | No       | -                  |
| max_length | Truncate the hex-encoded hash to this many characters. `0` means no truncation. Must not be negative.                                 | `0`     | No       | -                  |

The `sha3-254` value selects the SHA3-256 algorithm.

## Example: Generate hash from job title

The following example generates a hash from the `jobtitle` into sha1 and truncates the results after the 10th character.

We can set the salt via the environment variable `GREENMASK_GLOBAL_SALT`:

```shell
export GREENMASK_GLOBAL_SALT="12343567baaa"
```

```yaml title="Hash transformer example"
dump:
  transformation:
    - schema: "humanresources"
      name: "employee"
      transformers:
        - name: "Hash"
          params:
            column: "jobtitle"
            function: "sha1"
            max_length: 10
```

```bash title="Expected result"

| column name | original value                   | transformed |
|-------------|----------------------------------|-------------|
| jobtitle    | Research and Development Manager | 3a456da5c5  |

```
