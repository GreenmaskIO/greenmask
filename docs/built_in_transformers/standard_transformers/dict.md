Replace values matched by dictionary keys.

## Parameters

| Name             | Description                                                                                                                            | Default | Required | Supported DB types |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column           | The name of the column to be affected                                                                                                  |         | Yes      | any                |
| values           | Value replace mapping as in: `{"string": "string"}`. The string with value `"\N"` is considered NULL.                                  |         | No       | -                  |
| default          | Shown if no value has been matched with dict. The string with value `"\N"` is considered NULL. By default is empty.                    |         | No       | -                  |
| fail_not_matched | When no value is matched with the dict, fails the replacement process if set to `true`, or keeps the current value, if set to `false`. | `true`  | No       | -                  |
| validate         | Performs the encode-decode procedure using column type to ensure that values have correct type                                         | `true`  | No       | -                  |

## Description

The `Dict` transformer uses a user-provided key-value dictionary to replace values based on matches specified in
the `values` parameter mapping. These provided values must align with the PostgreSQL type format. To validate the values
format before application, you can utilize the `validate` parameter, triggering a decoding procedure via the PostgreSQL
driver.

If there are no matches by key, an error will be raised according to a default `fail_not_matched: true` parameter. You
can change this behaviour by providing the `default` parameter, value from which will be shown in case of a missing
match.

In certain cases where the driver type does not support the validation operation, an error may occur. For setting or
matching a NULL value, use a string with the `\N` sequence.

## Example: Replace marital status

The following example replaces marital status from `S` to `M` or from `M` to `S` and raises an error if there is no
match:

``` yaml title="Dict transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "Dict"
      params:
        column: "maritalstatus"
        values:
          "S": "M"
          "M": "S"
        validate: true
        fail_not_matched: true
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>maritalstatus</td><td><span style="color:green">S</span></td><td><span style="color:red">M</span></td>
</tr>
</table>

