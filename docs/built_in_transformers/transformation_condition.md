# Transformation Condition

## Description

The transformation condition feature allows you to execute a defined transformation only if a specified condition is
met.
The condition must be defined as a boolean expression that evaluates to `true` or `false`. Greenmask uses
[expr-lang/expr](https://github.com/expr-lang/expr) under the hood. You can use all functions and syntax provided by the
`expr` library.

You can use the same functions that are described in
the [built-in transformers](/docs/built_in_transformers/advanced_transformers/custom_functions/index.md)

The transformers are executed one by one - this helps you create complex transformation pipelines. For instance
depending on value chosen in the previous transformer, you can decide to execute the next transformer or not.

## Record descriptors

To improve the user experience, Greenmask offers special namespaces for accessing values in different formats: either
the driver-encoded value in its real type or as a raw string.

- **`record`**: This namespace provides the record value in its actual type.
- **`raw_record`**: This namespace provides the record value as a string.

You can access a specific column’s value using `record.column_name` for the real type or `raw_record.column_name` for
the raw string value.

!!! warning

    A record may always be modified by previous transformers before the condition is evaluated. This means Greenmask does
    not retain the original record value and instead provides the current modified value for condition evaluation.

## Null values condition

To check if the value is null, you can use `null` value for the comparisson. This operation works compatibly
with SQL operator `IS NULL` or `IS NOT NULL`.

```text title="Is null cond example"
record.accountnumber == null && record.date > now()
```

```text title="Is not null cond example"
record.accountnumber != null && record.date <= now()
```

## Expression scope

Expression scope can be on table or specific transformer. If you define the condition on the table scope, then the
condition will be evaluated before any transformer is executed. If you define the condition on the transformer scope,
then the condition will be evaluated before the specified transformer is executed.

```yaml title="Table scope" 
- schema: "purchasing"
  name: "vendor"
  when: 'record.accountnumber == null || record.accountnumber == "ALLENSON0001"'
  transformers:
    - name: "RandomString"
      params:
        column: "accountnumber"
        min_length: 9
        max_length: 12
        symbols: "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
```

```yaml title="Transformer scope" 
- schema: "purchasing"
  name: "vendor"
  transformers:
    - name: "RandomString"
      when: 'record.accountnumber != null || record.accountnumber == "ALLENSON0001"'
      params:
        column: "accountnumber"
        min_length: 9
        max_length: 12
        symbols: "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
```

## Int and float value definition

It is important to create the integer or float value in the correct format. If you want to define the integer value you
must write a number without dot (`1`, `2`, etc.). If you want to define the float value you must write a number with
dot (`1.0`, `2.0`, etc.).

!!! warning

    You may see a wrong comparison result if you compare int and float, for example `1 == 1.0` will return `false`. 

## Architecture

Greenmask encodes the way only when evaluating the condition - this allows to optimize the performance of the
transformation if you have a lot of conditions that uses or (`||`) or and (`&&`) operators.

## Example: Chose random value and execute one of

In the following example, the `RandomChoice` transformer is used to choose a random value from the list of values.
Depending on the chosen value, the `Replace` transformer is executed to set the `activeflag` column to `true` or
`false`.

In this case the condition scope is on the transformer level.

```yaml
- schema: "purchasing"
  name: "vendor"
  transformers:
    - name: "RandomChoice"
      params:
        column: "name"
        values:
          - "test1"
          - "test2"

    - name: "Replace"
      when: 'record.name == "test1"'
      params:
        column: "activeflag"
        value: "false"

    - name: "Replace"
      when: 'record.name == "test2"'
      params:
        column: "activeflag"
        value: "true"
```

## Example: Do not transform specific columns

In the following example, the `RandomString` transformer is executed only if the `businessentityid` column value is not
equal to `1492` or `1`.

```yaml
  - schema: "purchasing"
    name: "vendor"
    when: '!(record.businessentityid | has([1492, 1]))'
    transformers:
      - name: "RandomString"
        params:
          column: "accountnumber"
          min_length: 9
          max_length: 12
          symbols: "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
```

## Example: Check the json attribute value

In the following example, the `RandomString` transformer is executed only if the `a` attribute in the `json_data` column
is equal to `1`.

```yaml
- schema: "public"
  name: "jsondata"
  when: 'raw_record.json_data | jsonGet("a") == 1'
  transformers:
    - name: "RandomString"
      params:
        column: "accountnumber"
        min_length: 9
        max_length: 12
        symbols: "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
```

