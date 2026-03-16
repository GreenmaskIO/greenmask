# Parameters templating

## Description

It is allowed to generate parameter values from templates. It is useful when you don't want to write values manually,
but instead want to generate and initialize them dynamically.

Here you can find the list of template functions that can be used in the
template [Custom functions](advanced_transformers/custom_functions/index.md).

You can encode and decode objects using the driver function bellow.

### Template functions

| Function               | Description                                                                                                                                                                                                                                               | Signature                                                           |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `.GetColumnType`       | Returns a string with the column type.                                                                                                                                                                                                                    | `.GetColumnType(name string) (typeName string, err error)`          |
| `.EncodeValueByColumn` | Encodes a value of any type into its raw string representation using the specified column name. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                           | `.EncodeValueByColumn(name string, value any) (res any, err error)` |
| `.DecodeValueByColumn` | Decodes a value from its raw string representation to a Golang type using the specified column name. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                      | `.DecodeValueByColumn(name string, value any) (res any, err error)` |
| `.EncodeValueByType`   | Encodes a value of any type into its string representation using the specified type name. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                                 | `.EncodeValueByType(name string, value any) (res any, err error)`   |
| `.DecodeValueByType`   | Decodes a value from its raw string representation to a Golang type using the specified type name. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.                                                        | `.DecodeValueByType(name string, value any) (res any, err error)`   |
| `.DecodeValue`         | Decodes a value from its raw string representation to a Golang type using the data type assigned to the table column specified in the `column` parameter. Decoding is performed through the PostgreSQL driver. Throws an error if types are incompatible. | `.DecodeValueByColumn(value any) (res any, err error)`              |
| `.EncodeValue`         | Encodes a value of any type into its string representation using the type assigned to the table column specified in the `column` parameter. Encoding is performed through the PostgreSQL driver. Throws an error if types are incompatible.               | `.EncodeValue(value any) (res any, err error)`                      |

:::warning

If column parameter is not linked to column parameter, then functions `.DecodeValue` and `.EncodeValue` will return 
an error. You can use `.DecodeValueByType` and `.EncodeValueByType` or `.DecodeValueByColumn` and 
`.EncodeValueByColumn` instead.

:::
### Example

In the example below, the min and max values for the `birth_date` column are generated dynamically using the `now`
template function. The value returns the current date and time. The `tsModify` function is then used to subtract 30
(and 18) years. But because the parameter type is mapped on `column` parameter type, the `EncodeValue` function is used
to encode the value into the column type.

For example, if we have the now date as `2021-01-01`, the dynamically calculated `min` value will be `1994-01-01` and
the `max` value will be `2006-01-01`.

```sql
CREATE TABLE account
(
    id         SERIAL PRIMARY KEY,
    gender     VARCHAR(1) NOT NULL,
    email      TEXT       NOT NULL NOT NULL UNIQUE,
    first_name TEXT       NOT NULL,
    last_name  TEXT       NOT NULL,
    birth_date DATE,
    created_at TIMESTAMP  NOT NULL DEFAULT NOW()
);

INSERT INTO account (first_name, gender, last_name, birth_date, email)
VALUES ('John', 'M', 'Smith', '1980-01-01', 'john.smith@gmail.com');
```

```yaml
- schema: "public"
  name: "account"
  transformers:
    - name: "RandomDate"
      params:
        column: "birth_date"
        min: '{{ now | tsModify "-30 years" | .EncodeValue }}' # 1994
        max: '{{ now | tsModify "-18 years" | .EncodeValue }}' # 2006
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>birth_date</td><td><span>1980-01-01</span></td><td><span>1995-09-06</span></td>
</tr>
</table>
