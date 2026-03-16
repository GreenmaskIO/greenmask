Transform data via external program using `stdin` and `stdout` interaction.

## Parameters

| Name               | Description                                                                                                                                                                                                                                                  | Default           | Required | Supported DB types |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|----------|--------------------|
| columns            | A list of column names to be affected. If empty, the entire tuple is used. Read about the structure further.                                                                                                                                                 |                   | Yes      | Any                |
| executable         | The path to the `executable` parameter file                                                                                                                                                                                                                  |                   | Yes      | -                  |
| args               | A list of parameters for the executable                                                                                                                                                                                                                      |                   | No       | -                  |
| driver             | The row driver with parameters that is used for interacting with cmd. See details below.                                                                                                                                                                     | `{"name": "csv"}` | No       | -                  |
| validate           | Performs a decoding operation using the PostgreSQL driver for data received from the command to ensure the data format is correct                                                                                                                            | `false`           | No       | -                  |
| timeout            | Timeout for sending and receiving data from the external command                                                                                                                                                                                             | `2s`              | No       | -                  |
| expected_exit_code | The expected exit code on SIGTERM signal. If the exit code is unexpected, the transformation exits with an error.                                                                                                                                            | `0`               | No       | -                  |
| skip_on_behaviour  | Skips transformation call if one of the provided columns has a `null` value (`any`) or each of the provided columns has `null` values (`all`). This option works together with the `skip_on_null_input` parameter on columns. Possible values: `all`, `any`. | `all`             | No       | -                  |

:::warning

The parameter `validate_output=true` may cause an error if the type does not have a PostgreSQL driver decoder 
implementation. Most of the types, such as `int`, `float`, `text`, `varchar`, `date`, `timestamp`, etc., have 
encoders and decoders, as well as inherited types like domain types based on them.

:::
## Description

The `Cmd` transformer allows you to send original data to an external program via `stdin` and receive transformed data
from `stdout`. It supports various interaction formats such as `json`, `csv`, or plain `text` for one-column
transformations. The interaction is performed line by line, so at the end of each sent data, a new line
symbol `\n` must be included.

### Types of interaction modes

#### text

Textual driver that is used only for one column transformation, thus you cannot provide here more than one column.
The value encodes into string laterally. For example, `2023-01-03 01:00:00.0+03`.

#### json

JSON line driver. It has two formats that can be passed through `driver.json_data_format`: `[text|bytes]`. Use
the `bytes` format for binary datatypes. Use the `text` format for non-binary datatypes and for those that can be
represented as string literals. The default `json_data_format` is `text`.

**Text format with indexes**

```json
{
  "column1": {
    "d": "some_value1",
    "n": false,
  },
  "column2": {
    "d": "some_value2",
    "n": false,
  }
}
```

**Bytes format with indexes**

```json
{
  "column1": {
    "d": "aGVsbG8gd29ybHNeODcxMjE5MCUlJSUlJQ==",
    "n": false,
  },
  "column2": {
    "d": "aGVsbG8gd29ybHNeODcxMjE5MCUlJSUlJQ==",
    "n": false,
  }
}
```

where:

* Each line is a JSON line with a map of attribute numbers to their values
* `d` — the raw data represented as base64 encoding for the bytes format or Unicode text for the text format. The base64
  encoding is needed because data can be binary.
* `n` — indicates if NULL is present

#### csv

CSV driver (comma-separated). The number of attributes is the same as the number of table columns, but the
columns that were not mentioned in the `columns` list are empty. The `NULL` value is represented as `\N`. Each attribute
is escaped by a quote (`"`). For example, if the transformed table has attributes `id`, `title`, and `created_at`, and
only `id` and `created_at` require transformation, then the CSV line will look as follows:

``` csv title="csv line example"
"123","","2023-01-03 01:00:00.0+03"
```

### Column object attributes

* `name` — the name of the column. This value is required. Depending on the attributes that follows further, this column
  may be used just as a value and is not affected in any way.
* `not_affected` — indicates whether the column is affected in the transformation. This attribute is required for the
  validation procedure when Greenmask is called with `greenmask dump --validate`. Setting `not_affected=true` can be
  helpful when the command transformer transforms data depending on the value of another column. For example, if you
  want to generate an `updated_at` column value depending on the `created_at` column value, you can set `created_at`
  to `not_affected=true`. The default value is `false`.
* `skip_original_data` — indicates whether the original data is required for the transformer. This attribute can be
  helpful for decreasing the interaction time. One use case is when the command works as a generator and returns the
  value without relying on the original data. The default value is `false`.
* `skip_on_null_input` — specifies whether to skip transformation when the original value is `null`. This attribute
  works in conjunction with the `skip_on_behaviour` parameter. For example, if you have two affected columns
  with `skip_on_null_input=true` and one column is `null`, then, if `skip_on_behaviour=any`, the transformation will be
  skipped, or, if `skip_on_behaviour=and`, the transformation will be performed. The default is `false`.


## Example: Apply transformation performed by external command in TEXT format

In the following example, `jobtitle` columns is transformed via external command transformer.

```python title="External transformer in python example"
#!/usr/bin/env python3
import signal
import sys

signal.signal(signal.SIGTERM, lambda sig, frame: exit(0))


# If we want to implement a simple generator, we need read the line from stdin and write any result to stdout
for _ in sys.stdin:
    # Writing the result to stdout with new line and flushing the buffer
    sys.stdout.write("New Job Title")
    sys.stdout.write("\n")
    sys.stdout.flush()

```

```yaml title="Cmd transformer config example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "Cmd"
      params:
        driver:
          name: "text"
        expected_exit_code: -1
        skip_on_null_input: true
        validate: true
        skip_on_behaviour: "any"
        timeout: 60s
        executable: "/var/lib/playground/test.py"
        columns:
          - name: "jobtitle"
            skip_original_data: true
            skip_on_null_input: true 
```



## Example: Apply transformation performed by external command in JSON format

In the following example, `jobtitle` and `loginid` columns are transformed via external command
transformer.

```python title="External transformer in python example"
#!/usr/bin/env python3
import json
import signal
import sys

signal.signal(signal.SIGTERM, lambda sig, frame: exit(0))

for line in sys.stdin:
    res = json.loads(line)
    # Setting dummy values
    res["jobtitle"] = {"d": "New Job Title", "n": False}
    res["loginid"]["d"] = "123"

    # Writing the result to stdout with new line and flushing the buffer
    sys.stdout.write(json.dumps(res))
    sys.stdout.write("\n")
    sys.stdout.flush()

```

```yaml title="Cmd transformer config example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "Cmd"
      params:
        driver:
          name: "json" # (1)
          json_data_format: "text" # (4)
        expected_exit_code: -1
        skip_on_null_input: true
        validate: true
        skip_on_behaviour: "any" # (2)
        timeout: 60s
        executable: "/var/lib/playground/test.py"
        columns:
          - name: "jobtitle"
            skip_original_data: true
            skip_on_null_input: true # (3)
          - name: "loginid"
            skip_original_data: false # (5)
            skip_on_null_input: true # (3)
```

1. Validate the received data via decode procedure using the PostgreSQL driver. Note that this may cause an error if the
   type is not supported in the PostgreSQL driver.
2. Skip transformation (keep the values) if one of the affected columns (`not_affected=false`) has a null value.
3. If a column has a null value, then skip it. This works in conjunction with `skip_on_behaviour`. Since it has the
   value any, if one of the columns (`jobtitle` or `loginid`) has a `null` value, then skip the
   transformation call.
4. The format of JSON can be either `text` or `bytes`. The default value is `text`.
5. The `skip_original_data` attribute is set to `true` the date will not be transfered to the command. This column
   will contain the empty original data
