Generate a hash of the text value using the `Scrypt` hash function under the hood. `NULL` values are kept.

## Parameters

| Name       | Description                                                                                                                           | Default | Required | Supported DB types |
|------------|---------------------------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                                                                                                 |         | Yes      | text, varchar      |
| function   | Hash algorithm to obfuscate data. Can be any of `md5`, `sha1`, `sha256`, `sha512`.                                                    | `sha1`  | No       | -                  |
| max_length | Indicates whether to truncate the hash tail and specifies at what length. Can be any integer number, where `0` means "no truncation". | `0`     | No       | -                  |

## Example: Generate hash from job title

The following example generates a hash from the `jobtitle` into sha1 and truncates the results after the 10th character.

```yaml title="Hash transformer example"
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

| column name | original value           | transformed |
|-------------|--------------------------|-------------|
| jobtitle    | Research and Development | Zpmfe8F+LV  |

```
