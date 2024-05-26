# Dynamic parameters

## Description

Most transformers in Greenmask have dynamic parameters. This functionality is possible because Greenmask utilizes a
database driver that can encode and decode raw values into their actual type representations.

This allows you to retrieve parameter values directly from the records. This capability is particularly beneficial when
you need to resolve functional dependencies between fields or satisfy constraints. Greenmask processes transformations
sequentially. Therefore, when you reference a field that was transformed in a previous step, you will access the
transformed value.

## Definition

```yaml
dynamic_params:
  - column: "column_name" # (1)
    cast_to: "cast_function" # (2)
    template: "template_function" # (3)
    default_value: any # (4)
```

1. Name of the column from which the value is retrieved.
2. Function used to cast the column value to the desired type.
3. Default value used if the column's value is `NULL`.
4. Template used for casting the column value to the desired type.

## Dynamic parameter options

* `column` - Specifies the column name. The value from each record in this column will be passed to the transformer as a
  parameter.

* `cast_to` - Indicates the function used to cast the column value to the desired type. Before being passed to the
  transformer, the value is cast to this type. For more details, see  [Cast functions](#cast-functions).

* `template` - Defines the template used for casting the column value to the desired type. You can create your own
  template and incorporate predefined functions and operators to implement the casting logic or other logic required for
  passing the value to the transformer. For more details,
  see [Template functions](advanced_transformers/custom_functions/index.md).

* `default_value` - Determines the default value used if the column's value is `NULL`. This value is represented in raw
  format appropriate to the type specified in the `column` parameter.

## Cast functions

| name                   | description                                                                    | input type                                | output type                               |
|------------------------|--------------------------------------------------------------------------------|-------------------------------------------|-------------------------------------------|
| UnixNanoToDate         | Cast int value as Unix Timestamp in *Nano Seconds* to *date* type              | int2, int4, int8, numeric, float4, float8 | date                                      |
| UnixMicroToDate        | Cast int value as Unix Timestamp in *Micro Seconds* to *date* type             | int2, int4, int8, numeric, float4, float8 | date                                      |
| UnixMilliToDate        | Cast int value as Unix Timestamp in *Milli Seconds* to *date* type             | int2, int4, int8, numeric, float4, float8 | date                                      |
| UnixSecToDate          | Cast int value as Unix Timestamp in *Seconds* to *date* type                   | int2, int4, int8, numeric, float4, float8 | date                                      |
| UnixNanoToTimestamp    | Cast int value as Unix Timestamp in *Nano Seconds* to *timestamp* type         | int2, int4, int8, numeric, float4, float8 | timestamp                                 |
| UnixMicroToTimestamp   | Cast int value as Unix Timestamp in *Micro Seconds* to *timestamp* type        | int2, int4, int8, numeric, float4, float8 | timestamp                                 |
| UnixMilliToTimestamp   | Cast int value as Unix Timestamp in *Milli Seconds* to *timestamp* type        | int2, int4, int8, numeric, float4, float8 | timestamp                                 |
| UnixSecToTimestamp     | Cast int value as Unix Timestamp in *Seconds* to *timestamp* type              | int2, int4, int8, numeric, float4, float8 | timestamp                                 |
| UnixNanoToTimestampTz  | Cast int value as Unix Timestamp in *Nano Seconds* to *timestamptz* type       | int2, int4, int8, numeric, float4, float8 | timestamptz                               |
| UnixMicroToTimestampTz | Cast int value as Unix Timestamp in *Micro Seconds* to *timestamptz* type      | int2, int4, int8, numeric, float4, float8 | timestamptz                               |
| UnixMilliToTimestampTz | Cast int value as Unix Timestamp in *Milli Seconds* to *timestamptz* type      | int2, int4, int8, numeric, float4, float8 | timestamptz                               |
| UnixSecToTimestampTz   | Cast int value as Unix Timestamp in *Seconds* to *timestamptz* type            | int2, int4, int8, numeric, float4, float8 | timestamptz                               |
| DateToUnixNano         | Cast *date* value to *int* value as a Unix Timestamp in *Nano Seconds*         | date                                      | int2, int4, int8, numeric, float4, float8 |
| DateToUnixMicro        | Cast *date* value to *int* value as a Unix Timestamp in *Micro Seconds*        | date                                      | int2, int4, int8, numeric, float4, float8 |
| DateToUnixMilli        | Cast *date* value to *int* value as a Unix Timestamp in *Milli Seconds*        | date                                      | int2, int4, int8, numeric, float4, float8 |
| DateToUnixSec          | Cast *date* value to *int* value as a Unix Timestamp in *Seconds*              | date                                      | int2, int4, int8, numeric, float4, float8 |
| TimestampToUnixNano    | Cast *timestamp* value to *int* value as a Unix Timestamp in *Nano Seconds*    | timestamp                                 | int2, int4, int8, numeric, float4, float8 |
| TimestampToUnixMicro   | Cast *timestamp* value to *int* value as a Unix Timestamp in *Micro Seconds*   | timestamp                                 | int2, int4, int8, numeric, float4, float8 |
| TimestampToUnixMilli   | Cast *timestamp* value to *int* value as a Unix Timestamp in *Milli Seconds*   | timestamp                                 | int2, int4, int8, numeric, float4, float8 |
| TimestampToUnixSec     | Cast *timestamp* value to *int* value as a Unix Timestamp in *Seconds*         | timestamp                                 | int2, int4, int8, numeric, float4, float8 |
| TimestampTzToUnixNano  | Cast *timestamptz* value to *int* value as a Unix Timestamp in *Nano Seconds*  | timestamptz                               | int2, int4, int8, numeric, float4, float8 |
| TimestampTzToUnixMicro | Cast *timestamptz* value to *int* value as a Unix Timestamp in *Micro Seconds* | timestamptz                               | int2, int4, int8, numeric, float4, float8 |
| TimestampTzToUnixMilli | Cast *timestamptz* value to *int* value as a Unix Timestamp in *Milli Seconds* | timestamptz                               | int2, int4, int8, numeric, float4, float8 |
| TimestampTzToUnixSec   | Cast *timestamptz* value to *int* value as a Unix Timestamp in *Seconds*       | timestamptz                               | int2, int4, int8, numeric, float4, float8 |
| FloatToInt             | Cast float value to one of integer type. The fractional part will be discarded | numeric, float4, float8                   | int2, int4, int8, numeric                 |
| IntToFloat             | Cast int value to one of integer type                                          | int2, int4, int8, numeric                 | numeric, float4, float8                   |
| IntToBool              | Cast int value to boolean. The value with 0 is false, 1 is true                | int2, int4, int8, numeric, float4, float8 | bool                                      |
| BoolToInt              | Cast boolean value to int. The value false is 0, true is 1                     | bool                                      | int2, int4, int8, numeric, float4, float8 |

## Example: Functional dependency resolution between columns

There is simplified schema of the table `humanresources.employee` from the [playground](../playground.md):

```sql
       Column      |            Type                      
------------------+-----------------------------
 businessentityid | integer                      
 jobtitle         | character varying(50)        
 birthdate        | date                        
 hiredate         | date                         
Check constraints:
    CHECK (birthdate >= '1930-01-01'::date AND birthdate <= (now() - '18 years'::interval))
```

As you can see, there is a functional dependency between the `birthdate` and `hiredate` columns. Logically,
the `hiredate` should be later than the `birthdate`. Additionally, the `birthdate` should range from `1930-01-01`
to `18` years prior to the current date.

Imagine that you need to generate random `birthdate` and `hiredate` columns. To ensure these dates satisfy the
constraints, you can use dynamic parameters in the `RandomDate` transformer:

```yaml
- schema: "humanresources"
  name: "employee"
  transformers:

    - name: "RandomDate" # (1)
      params:
        column: "birthdate"
        min: '{{ now | tsModify "-30 years" | .EncodeValue }}' # (2)
        max: '{{ now | tsModify "-18 years" | .EncodeValue }}' # (3)

    - name: "RandomDate" # (4)
      params:
        column: "hiredate"
        max: "{{ now | .EncodeValue }}" # (5)
      dynamic_params:
        min:
          column: "birthdate" # (6)
          template: '{{ .GetValue | tsModify "18 years" | .EncodeValue }}' # (7)
```

1. Firstly we generate the `RadnomDate` for birthdate column. The result of the transformation will used as the minimum
   value for the next transformation for `hiredate` column.
2. Apply the template for static parameter. It calculates the now date and subtracts `30` years from it. The result
   is `1994`. The function tsModify return not a raw data, but time.Time object. For getting the raw value suitable for
   birthdate type we need to pass this value to `.EncodeValue` function. This value is used as the minimum value for
   the `birthdate` column.
3. The same as the previous step, but we subtract `18` years from the now date. The result is `2002`.
4. Generate the `RadnomDate` for `hiredate` column based on the value from the `birthdate`.
5. Set the maximum value for the `hiredate` column. The value is the current date.
6. The `min` parameter is set to the value of the `birthdate` column from the previous step. 
7. The template gets the value of the randomly generated `birthdate` value and adds `18` years to it.

Below is the result of the transformation:

![img.png](../assets/built_in_transformers/img.png)

From the result, you can see that all functional dependencies and constraints are satisfied.
