Change a JSON document using `delete` and `set` operations. `NULL` values are kept.

## Parameters

| Name       | Properties      | Description                                                                                      | Default | Required | Supported DB types |
|------------|-----------------|--------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column     |                 | The name of the column to be affected                                                            |         | Yes      | json, jsonb        |
| operations |                 | A list of operations that contains editing `delete` and `set`                                    |         | Yes      | -                  |
|          ∟ | operation       | Specifies the operation type: `set` or `delete`                                                  |         | Yes      | -                  |
|          ∟ | path            | The path to an object to be modified. See path syntax below.                                     |         | Yes      | -                  |
|          ∟ | value           | A value to be assigned to the provided path                                                      |         | No       | -                  |
|          ∟ | value_template  | A Golang template to be assigned to the provided path. See the list of template functions below. |         | No       | -                  |
|          ∟ | error_not_exist | Throws an error if the key does not exist by the provided path. Disabled by default.             | `false` | No       | -                  |

## Description

The `Json` transformer applies a sequence of changing operations (`set` and/or `delete`) to a JSON document. The value can be static or dynamic. For the `set` operation type, a static value is provided in the `value` parameter, while a dynamic value is provided in the `value_template` parameter, taking the data received after template execution as a result. Both the `value` and `value_template` parameters are mandatory for the `set` operation.

### Path syntax

The Json transformer is based on [tidwall/sjson](https://github.com/tidwall/sjson) and supports the same path syntax. See their documentation for [syntax rules](https://github.com/tidwall/sjson#path-syntax).

### Template functions

| Function               | Description                                                                                                                                                                                                         | Signature                                                           |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `.GetPath`             | Returns the current path to which the operation is being applied                                                                                                                                                    | `.GetPath() (path string)`                                          |
| `.GetOriginalValue`    | Returns the original value to which the current operation path is pointing. If the value at the specified path does not exist, it returns  `nil`.                                                                   | `.GetOriginalValue() (value any)`                                   |
| `.OriginalValueExists` | Returns a boolean value indicating whether the specified path exists or not.                                                                                                                                        | `.OriginalValueExists() (exists bool)`                              |
| `.GetColumnValue`      | Returns an encoded into Golang type value for a specified column or throws an error. A value can be any of `int`, `float`, `time`, `string`, `bool`, or `slice` or `map`.                                           | `.GetColumnValue(name string) (value any, err error)`               |
| `.GetRawColumnValue`   | Returns a raw value for a specified column as a string or throws an error                                                                                                                                           | `.GetRawColumnValue(name string) (value string, err error)`         |
| `.EncodeValueByColumn` | Encodes a value of any type into its raw string representation using the specified column name. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.      | `.EncodeValueByColumn(name string, value any) (res any, err error)` |
| `.DecodeValueByColumn` | Decodes a value from its raw string representation to a Golang type using the specified column name. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible. | `.DecodeValueByColumn(name string, value any) (res any, err error)` |
| `.EncodeValueByType`   | Encodes a value of any type into its string representation using the specified type name. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.            | `.EncodeValueByType(name string, value any) (res any, err error)`   |
| `.DecodeValueByType`   | Decodes a value from its raw string representation to a Golang type using the specified type name. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.   | `.DecodeValueByType(name string, value any) (res any, err error)`   |

## Example: Changing JSON document

``` yaml title="Json transformer example"
- schema: "bookings"
  name: "aircrafts_data"
  transformers:
    - name: "Json"
      params:
        column: "model"
        operations:
          - operation: "set"
            path: "en"
            value: "Boeing 777-300-2023"
          - operation: "set"
            path: "seats"
            error_not_exist: True
            value_template: "{{ randomInt 100 400 }}"
          - operation: "set"
            path: "details.preperties.1"
            value: {"name": "somename", "description": null}
          - operation: "delete"
            path: "values.:2"
```
