The `RandomCompany` transformer is designed to populate specified database columns with company attributes such as
name and company suffix

## Parameters

| Name    | Description                                                                                         | Default  | Required | Supported DB types                  |
| ------- | --------------------------------------------------------------------------------------------------- | -------- | -------- | ----------------------------------- |
| columns | The name of the column to be affected                                                               |          | Yes      | text, varchar, char, bpchar, citext |
| engine  | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                                   |

## Description

The `RandomCompany` transformer utilizes a comprehensive list of company names to inject random company names into the
designated database column. This feature allows for the creation of diverse and realistic company data by
simulating a variety of company names without using real company data.

### _column_ object attributes

- `name` — the name of the column where the attributes will be stored. This value is required.
- `template` - the template for the column value.
  You can use the attributes: `.CompanyName`, `.CompanySuffix`. For example, if you want to generate a full
  company name, you can use the next template:
  `"{{ .CompanyName }} {{ .CompanySuffix }}"`

- `hashing` - the bool value. Indicates whether the column value must be passed through the hashing function.
  The default value is `false`. If all column has `hashing` set to `false` (by default), then all columns will be
  hashed.
- `keep_null` - the bool value. Indicates whether NULL values should be preserved. The default value is `true`

## Example: Populate random first name and last name for table company_profiles in static mode

This example demonstrates how to use the `RandomCompany` transformer to populate the `name` column in
the `company_profiles` table with random company names, and company suffixes respectively.

```sql title="Create table company_profiles and insert data"

CREATE TABLE company_data
(
    id      SERIAL PRIMARY KEY,
    name    VARCHAR(100),
);

-- Insert sample data into the table
INSERT INTO personal_data (name, surname, sex)
VALUES ('ACME Corp'),
       ('Foo LLP'),
       ('Bar Inc.'),
```

```yaml title="RandomCompany transformer example"
- schema: public
  name: company_data
  transformers:
    - name: "RandomCompany"
      params:
        columns:
          - name: "name"
            template: "{{ .CompanyName  }} {{ .CompanySuffix }}"
        engine: "hash"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>name</td><td><span>ACME Corp</span></td><td><span>Bright Ridge LLP.</span></td>
</tr>
</table>
