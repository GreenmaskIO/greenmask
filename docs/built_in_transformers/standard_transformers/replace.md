Replace an original value by the provided one.

## Parameters

| Name      | Description                                                                                                                    | Default | Required | Supported DB types |
|-----------|--------------------------------------------------------------------------------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected                                                                            |         | Yes      | any                |
| value     | The value to replace                                                                                                           |         | Yes      | -                  |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                                                   | `true`  | No       | -                  |
| validate  | Performs a decoding procedure via the PostgreSQL driver using the column type to ensure that values have correct type | `true`  | No       | -                  |

## Dynamic parameters

| Parameter | Supported types |
|-----------|-----------------|
| value     | any             |

!!! warning 

    The `validate` parameter in dynamic mode validates each dynamic value by decoding it value via the PostgreSQL 
    driver. In case there is type format violation the dump command will be terminated with an error message.

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

## Example: Set the same value from another column

In this example, the `jobdescription` column value will be set the same as for the `jobtitle` column.  
 


Create schema and insert data:
```sql
CREATE TABLE employee (
    jobtitle varchar(50),
    jobdescription varchar(50)
);

INSERT INTO employee (jobtitle, jobdescription) VALUES
('CEO', 'Chief Executive Officer');
```

```yaml title="Replace transformer example"
- schema: "public"
  name: "employee"
  transformers:
    - name: "RandomChoice"
      params:
        column: "jobtitle"
        values:
          - "Programmer"
          - "Analyst"
          - "Manager"

    - name: "Replace"
      params:
        column: "jobdescription"
        validate: true
      dynamic_params:
        value:
          column: "jobtitle"
```

Result:

```Text
        "public"."employee"
+-----------+----------------+-------------------------+------------------+
| %LineNum% | Column         | OriginalValue           | TransformedValue |
+-----------+----------------+-------------------------+------------------+
| 0         | jobtitle       | Chief Executive Officer | Manager          |
+           +----------------+-------------------------+------------------+
|           | jobdescription | Chief Executive Officer | Manager          |
+-----------+----------------+-------------------------+------------------+
```
