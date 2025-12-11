Replace a string using a regular expression.

## Parameters

| Name    | Description                                                                                          | Default | Required | Supported DB types                  |
|---------|------------------------------------------------------------------------------------------------------|---------|----------|-------------------------------------|
| column  | The name of the column to be affected                                                                |         | Yes      | text, varchar, char, bpchar, citext |
| regexp  | The regular expression pattern to search for in the column's value                                   |         | Yes      | -                                   |
| replace | The replacement value. This value may be replaced with a captured group from the `regexp` parameter. |         | Yes      | -                                   |

## Description

The `RegexpReplace` transformer replaces a string according to the applied regular expression. The valid regular
expressions syntax is the same as the general syntax used by Perl, Python, and other languages. To be precise, it is the
syntax accepted by RE2 and described in the [Golang documentation](https://golang.org/s/re2syntax), except for `\C`.

## Example: Removing leading prefix from `loginid` column value

In the following example, the original values from `loginid` matching the `adventure-works\{{ id_name }}` format are
replaced with `{{ id_name }}`.

``` yaml title="RegexpReplace transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
  - name: "RegexpReplace"
    params:
      column: "loginid"
      regexp: "adventure-works\\\\(.*)"
      replace: "$1"
```

```bash title="Expected result"

| column name | original value       | transformed |
|-------------|----------------------|-------------|
| loginid     | adventure-works\ken0 | ken0        |
```

:::note

YAML has control symbols, and using them without escaping may result in an error. In the example above, the prefix
of `id` is separated by the `\` symbol. Since this symbol is a control symbol, we must escape it using `\\`.
However, the '\' symbol is also a control symbol for regular expressions, which is why we need to
double-escape it as `\\\\`.

:::