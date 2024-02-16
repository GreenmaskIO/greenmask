Replace an original value by the provided one.

## Parameters

| Name      | Description                                                                                                                    | Default | Required | Supported DB types |
|-----------|--------------------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected                                                                            |         | Yes      | any                |
| replace   | The value to replace                                                                                                           |         | Yes      | -                  |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                                                   | `true`  | No       | -                  |
| validate  | Performs a decoding procedure via the PostgreSQL driver using the column type to ensure that values have correct type | `true`  | No       | -                  |

## Description

The `Replace` transformer replace an original value from the specified column with the provided one. It can optionally run a validation check with the `validate` parameter to ensure that the values are of a correct type before starting transformation. The behaviour for NULL values can be configured using the `keep_null` parameter.

## Example: Updating the `jobtitle` column

In the following example, the provided `value: "programmer"` is first validated through driver decoding. If the current value of the
`jobtitle` column is not `NULL`, it will be replaced with `programmer`. If the current value is `NULL`, it will
remain `NULL`.

``` yaml title="Replace transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
  - name: "Replace"
    params:
      column: "jobtitle"
      value: "programmer"
      keep_null: false
      validate: true
```

```bash title="Expected result"

| column name | original value          | transformed |
|-------------|-------------------------|-------------|
| jobtitle    | Chief Executive Officer | programmer  |
```
