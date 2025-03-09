The `RandomPerson` transformer is designed to populate specified database columns with personal attributes such as
first name, last name, title and gender.

## Parameters

| Name            | Description                                                                                         | Default  | Required | Supported DB types                  |
|-----------------|-----------------------------------------------------------------------------------------------------|----------|----------|-------------------------------------|
| columns         | The name of the column to be affected                                                               |          | Yes      | text, varchar, char, bpchar, citext |
| gender          | set specific gender (possible values: Male, Female, Any)                                            | `Any`    | No       | -                                   |
| gender_mapping  | Specify gender name to possible values when using dynamic mode in "gender" parameter                | `Any`    | No       | -                                   |
| fallback_gender | Specify fallback gender if not mapped when using dynamic mode in "gender" parameter                 | `Any`    | No       | -                                   |
| engine          | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                                   |

## Description

The `RandomPerson` transformer utilizes a comprehensive list of first names to inject random first names into the
designated database column. This feature allows for the creation of diverse and realistic user profiles by
simulating a variety of first names without using real user data.

### *column* object attributes

* `name` — the name of the column where the personal attributes will be stored. This value is required.
* `template` - the template for the column value.
  You can use the next attributes: `.FirstName`, `.LastName` or `.Title`. For example, if you want to generate a full
  name, you can use the next template:
  `"{{ .FirstName }} {{ .LastName }}"`

* `hashing` - the bool value. Indicates whether the column value must be passed through the hashing function.
  The default value is `false`. If all column has `hashing` set to `false` (by default), then all columns will be
  hashed.
* `keep_null` - the bool value. Indicates whether NULL values should be preserved. The default value is `true`

### *gender_mapping* object attributes

`gender_mapping` - a dictionary that maps the gender value when `gender` parameters works in dynamic mode.
The default value is:

```json
{
  "Male": [
    "male",
    "M",
    "m",
    "man",
    "Man"
  ],
  "Female": [
    "female",
    "F",
    "f",
    "w",
    "woman",
    "Woman"
  ]
}
```

### *fallback_gender*

Gender that will be used if `gender_mapping` was not found. This parameter is optional
and required only for `gender` parameter in dynamic mode. The default value is `Any`.

## Example: Populate random first name and last name for table user_profiles in static mode

This example demonstrates how to use the `RandomPerson` transformer to populate the `name` and `surname` columns in
the `user_profiles` table with random first names, last name, respectively.

```sql title="Create table user_profiles and insert data"

CREATE TABLE personal_data
(
    id      SERIAL PRIMARY KEY,
    name    VARCHAR(100),
    surname VARCHAR(100),
    sex     CHAR(1) CHECK (sex IN ('M', 'F'))
);

-- Insert sample data into the table
INSERT INTO personal_data (name, surname, sex)
VALUES ('John', 'Doe', 'M'),
       ('Jane', 'Smith', 'F'),
       ('Alice', 'Johnson', 'F'),
       ('Bob', 'Lee', 'M');
```

```yaml title="RandomPerson transformer example"
- schema: public
  name: personal_data
  transformers:
    - name: "RandomPerson"
      params:
        gender: "Any"
        columns:
          - name: "name"
            template: "{{ .FirstName }}"
          - name: "surname"
            template: "{{ .LastName }}"
        engine: "hash"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>name</td><td><span style="color:green">John</span></td><td><span style="color:red">Zane</span></td>
</tr>
<tr>
<td>surname</td><td><span style="color:green">Doe</span></td><td><span style="color:red">McCullough</span></td>
</tr>
</table>

## Example: Populate random first name and last name for table user_profiles in dynamic mode

This example demonstrates how to use the `RandomPerson` transformer to populate the `name`, `surname` using dynamic
gender

```yaml title="RandomPerson transformer example with dynamic mode"
- schema: public
  name: personal_data
  transformers:
    - name: "RandomPerson"
      params:
        columns:
          - name: "name"
            template: "{{ .FirstName }}"
          - name: "surname"
            template: "{{ .LastName }}"
        engine: "random"
      dynamic_params:
        gender:
          column: sex
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>name</td><td><span style="color:green">John</span></td><td><span style="color:red">Martin</span></td>
</tr>
<tr>
<td>surname</td><td><span style="color:green">Doe</span></td><td><span style="color:red">Mueller</span></td>
</tr>
</table>
