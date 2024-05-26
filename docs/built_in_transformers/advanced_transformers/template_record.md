Modify records using a Go template and apply changes by using the PostgreSQL driver functions. This transformer provides a way to implement custom transformation logic.

## Parameters

| Name     | Description                                                                                                                                                     | Default | Required | Supported DB types |
|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| columns  | A list of columns to be affected by the template. The list of columns will be checked for constraint violations.                              |         | No       | any                |
| template | A Go template string                                                                                                                                            |         | Yes      | -                  |
| validate | Validate the template result via PostgreSQL driver decoding procedure. Throws an error if a custom type does not have an encode-decoder implementation. | false   | No       | -                  |

## Description

`TemplateRecord` uses [Go templates](https://pkg.go.dev/text/template) to change data. However, while the [Template transformer](./template.md) operates with a single column and automatically applies results, the `TemplateRecord` transformer can make changes to a set of columns in the string, and using driver functions `.SetValue` or `.SetRawValue` is mandatory to do that.

With the `TemplateRecord` transformer, you can implement complicated transformation logic using basic or custom template functions. Below you can get familiar with the basic template functions for the `TemplateRecord` transformer. For more information about available custom template functions, see [Custom functions](custom_functions/index.md).

### Template functions

| Function               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                          | Signature                                                           |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `.GetColumnType`       | Returns a string with the column type.                                                                                                                                                                                                                                                                                                                                                                                                               | `.GetColumnType(name string) (typeName string, err error)`          |
| `.GetColumnValue`      | Returns an encoded value for a specified column or throws an error. A value can be any of `int`, `float`, `time`, `string`, `bool`, or `slice` or `map`.                                                                                                                                                                                                                                                                                             | `.GetColumnValue(name string) (value any, err error)`               |
| `.GetRawColumnValue`   | Returns a raw value for a specified column as a string or throws an error                                                                                                                                                                                                                                                                                                                                                                            | `.GetRawColumnValue(name string) (value string, err error)`         |
| `.SetColumnValue`      | Sets a new value of a specific data type to the column. The value assigned must be compatible with the PostgreSQL data type of the column. For example, it is allowed to assign an `int` value to an `INTEGER` column, but you cannot assign a `float` value to a `timestamptz` column.                                                                                                                                                              | `SetColumnValue(name string, v any) (bool, error)`                  |
| `.SetRawColumnValue`   | Sets a new raw value for a column, inheriting the column's existing data type, without performing data type validation. This can lead to errors when restoring the dump if the assigned value is not compatible with the column type. To ensure compatibility, consider using the `.DecodeValueByColumn` function followed by `.SetColumnValue`, for example, `{{ "13" \| .DecodeValueByColumn "items_amount" \| .SetColumnValue "items_amount" }}`. | `.SetRawColumnValue(name string, value any) (err error)`            |
| `.EncodeValueByColumn` | Encodes a value of any type into its raw string representation using the specified column name. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                                                                                                                                                                                                                      | `.EncodeValueByColumn(name string, value any) (res any, err error)` |
| `.DecodeValueByColumn` | Decodes a value from its raw string representation to a Golang type using the specified column name. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                                                                                                                                                                                                                 | `.DecodeValueByColumn(name string, value any) (res any, err error)` |
| `.EncodeValueByType`   | Encodes a value of any type into its string representation using the specified type name. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                                                                                                                                                                                                                            | `.EncodeValueByType(name string, value any) (res any, err error)`   |
| `.DecodeValueByType`   | Decodes a value from its raw string representation to a Golang type using the specified type name. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                                                                                                                                                                                                                   | `.DecodeValueByType(name string, value any) (res any, err error)`   |

## Example: Generate a random `created_at` and `updated_at` dates

Below you can see the table structure:

![img.png](../../assets/built_in_transformers/orders-schema.png)

The goal is to modify the `"created_at"` and `"updated_at"` columns based on the following rules:

* Do not change the value if the `created_at` is Null.
* If the `created_at` is not Null, generate the current time and use it as the minimum threshold for randomly
  generating the `updated_at` value.
* Assign all generated values using the `.SetColumnValue` function.


```yaml title="Template transformer example"
- name: "TemplateRecord"
  params:
    columns:
      - "created_at"
      - "updated_at"
    template: >
      {{ $val := .GetColumnValue "created_at" }}
      {{ if isNotNull $val }}
          {{ $createdAtValue := now }}
          {{ $maxUpdatedDate := date_modify "24h" $createdAtValue }}
          {{ $updatedAtValue := randomDate $createdAtValue $maxUpdatedDate }}
          {{ .SetColumnValue "created_at" $createdAtValue }}
          {{ .SetColumnValue "updated_at" $updatedAtValue }}
      {{ end }}
    validate: true
```

Expected result:

| column name | original value                | transformed                 |
|-------------|-------------------------------|-----------------------------|
| created_at  | 2021-01-20 07:01:00.513325+00 | 2023-12-17 19:37:29.910054Z |
| updated_at  | 2021-08-09 21:27:00.513325+00 | 2023-12-18 10:05:25.828498Z |
