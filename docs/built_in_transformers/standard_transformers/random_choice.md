Replace values randomly chosen from a provided list.

## Parameters

| Name      | Description                                                                                                           | Default | Required | Supported DB types |
|-----------|-----------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected                                                                                 |         | Yes      | any                |
| values    | A list of values in any format. The string with value `\N` is considered NULL.                                        |         | Yes      | -                  |
| validate  | Performs a decoding procedure via the PostgreSQL driver using the column type to ensure that values have correct type | `true`  | No       |                    |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                                          | `true`  | No       |                    |

## Description

The `RandomChoice` transformer replaces one randomly chosen value from the list provided in the `values` parameter. You
can use the `validate` parameter to ensure that values are correct before applying the transformation. The behaviour for NULL values can be configured using the `keep_null` parameter.

## Example: Choosing randomly from provided dates

In this example, the provided values undergo validation through PostgreSQL driver decoding, and one value is randomly
chosen from the list.

```yaml title="RandomChoice transformer example"
    - schema: "humanresources"
      name: "jobcandidate"
      transformers:
        - name: "RandomChoice"
          params:
            column: "modifieddate"
            validate: true
            values:
              - "2023-12-21 07:41:06.891"
              - "2023-12-21 07:41:06.896"
```
