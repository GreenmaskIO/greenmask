Generate email addresses for a specified column.

## Parameters

| Name                 | Description                                                                                         | Default                                                                                                                                                                | Required | Supported DB types                  |
|----------------------|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------|
| column               | The name of the column to be affected                                                               |                                                                                                                                                                        | Yes      | text, varchar, char, bpchar, citext |
| keep_original_domain | Keep original of the original address                                                               | `false`                                                                                                                                                                | No       | -                                   |
| local_part_template  | The template for local part of email                                                                |                                                                                                                                                                        | No       | -                                   |
| domain_part_template | The template for domain part of email                                                               |                                                                                                                                                                        | No       | -                                   |
| domains              | List of domains for new email                                                                       | `["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com", "mail.com", "zoho.com", "yandex.com", "protonmail.com", "gmx.com", "fastmail.com"]` | No       | -                                   |
| validate             | Validate generated email if using template                                                          | `false`                                                                                                                                                                | No       | -                                   |
| max_random_length    | Max length of randomly generated part of the email                                                  | `32`                                                                                                                                                                   | No       | -                                   |
| keep_null            | Indicates whether NULL values should be preserved                                                   | `false`                                                                                                                                                                | No       | -                                   |
| engine               | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random`                                                                                                                                                               | No       | -                                   |

## Description

The `RandomEmail` transformer generates random email addresses for the specified database column. By default, the
transformer generates random email addresses with a maximum length of 32 characters. The `keep_original_domain`
parameter allows you to preserve the original domain part of the email address. The `local_part_template`
and `domain_part_template` parameters enable you to specify templates for the local and domain parts of the email
address, respectively. If the `validate` parameter is set to `true`, the transformer will validate the generated email
addresses against the specified templates. The `keep_null` parameter allows you to preserve existing NULL values in the
column.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Templates parameters

In each template you have access to the columns of the table by using the `{{ .column_name }}` syntax. Note that
all values are strings. For example, you can use for assembling the email address by accessing to `first_name` and
`last_name` columns `{{ .first_name | lower }}.{{ .last_name | lower }}`.

The transformer always generates random sequences for the email, and you can use it by accessing
the `{{ .random_string }}` variable. For example, we can add random string in the end of local part
`{{ .first_name | lower }}.{{ .last_name | lower }}.{{ .random_string }}`.

Read more about template function [Template functions](../advanced_transformers/custom_functions/index.md).

## Random email generation using first name and last name

In this example, the `RandomEmail` transformer generates random email addresses for the `email` column in the `account`
table. The transformer generates email addresses using the `first_name` and `last_name` columns as the local part
of the email address and adds a random string to the end of the local part with length 10 characters. The original
domain part of the email address is preserved.

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

```yaml title="RandomEmail transformer example"
- schema: "public"
  name: "account"
  transformers:
    - name: "RandomEmail"
      params:
        column: "email"
        engine: "hash"
        keep_original_domain: true
        local_part_template: "{{ first_name | lower }}.{{ last_name | lower }}.{{ .random_string | trunc 10 }}"
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>email</td><td><span style="color:green">john.smith@gmail.com</span></td><td><span style="color:red">john.smith.a075d99e2d@gmail.com</span></td>
</tr>
</table>

## Simple random email generation

In this example, the `RandomEmail` transformer generates random email addresses for the `email` column in the `account`
table. The transformer generates random email addresses with a maximum length of 10 characters.

```yaml title="RandomEmail transformer example"
- schema: "public"
  name: "account"
  transformers:
    - name: "RandomEmail"
      params:
        column: "email"
        max_random_length: 10
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>email</td><td><span style="color:green">john.smith@gmail.com</span></td><td><span style="color:red">john.smith.a075d99e2d@gmail.com</span></td>
</tr>
</table>
