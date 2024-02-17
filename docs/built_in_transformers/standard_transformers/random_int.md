Generate a random integer within the provided interval.

## Parameters

| Name      | Description                                                                            | Default | Required | Supported DB types                                  |
|-----------|----------------------------------------------------------------------------------------|---------|----------|-----------------------------------------------------|
| column    | The name of the column to be affected                                    |         | Yes      | int2 (smallint), int4 (int), int8 (bigint), numeric |
| min       | The minimum threshold for the random value. The value range depends on the column type. |         | Yes      | -                                                   |
| max       | The maximum threshold for the random value. The value range depends on the column type. |         | Yes      | -                                                   |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not           | `true`  | No       | -                                                   |

## Description

The `RandomInt` transformer generates a random integer within the specified `min` and `max` thresholds. The behaviour for NULL values can be configured using the `keep_null` parameter.

## Example: Generate random item quantity

In the following example, the `RandomInt` transformer generates a random value in the range from `1` to `30` and assigns it to
the `orderqty` column.

``` yaml title="RandomInt transformer example"
- schema: "sales"
  name: "salesorderdetail"
  transformers:
    - name: "RandomInt"
      params:
        column: "orderqty"
        min: 1
        max: 30
```

```bash title="Expected result"

| column name | original value | transformed |
|-------------|----------------|-------------|
| orderqty    | 1              | 8           |
```
