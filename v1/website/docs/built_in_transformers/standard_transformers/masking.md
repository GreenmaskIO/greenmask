Mask a value using one of the masking rules depending on your domain. `NULL` values are kept.

## Parameters

| Name   | Description                                                                                                     | Default   | Required | Supported DB types                  |
|--------|-----------------------------------------------------------------------------------------------------------------|-----------|----------|-------------------------------------|
| column | The name of the column to be affected                                                                           |           | Yes      | text, varchar, char, bpchar, citext |
| type   | Data type of attribute (`default`, `password`, `name`, `addr`, `email`, `mobile`, `tel`, `id`, `credit`, `url`) | `default` | No       | -                                   |

## Description

The `Masking` transformer replaces characters with asterisk `*` symbols depending on the provided data type. If the
value is `NULL`, it is kept unchanged. It is based on [ggwhite/go-masker](https://github.com/ggwhite/go-masker) and
supports the following masking rules:

|    Type     | Description                                                                                                                                                          |
|:-----------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|   default   | Returns `*` symbols with the same length, e.g. input: `test1234` output: `********`                                                                                  |
|    name     | Masks the second letter the third letter in a word, e. g. input: `ABCD` output: `A**D`                                                                               |
|  password   | Always returns `************`                                                                                                                                        |
|   address   | Keeps first 6 letters, masks the rest, e. g. input: `Larnaca, makarios st` output: `Larnac*************`                                                             |
|    email    | Keeps a domain and the first 3 letters, masks the rest, e. g. input: `ggw.chang@gmail.com` output: `ggw****@gmail.com`                                               |
|   mobile    | Masks 3 digits starting from the 4th digit, e. g. input: `0987654321` output: `0987***321`                                                                           |
|  telephone  | Removes `(`, `)`, ` `, `-` chart, and masks last 4 digits of telephone number, then formats it to `(??)????-????`, e. g. input: `0227993078` output: `(02)2799-****` |
|     id      | Masks last 4 digits of ID number, e. g. input: `A123456789` output: `A12345****`                                                                                     |
| credit_cart | Masks 6 digits starting from the 7th digit, e. g. input `1234567890123456` output `123456******3456`                                                                 |
|     url     | Masks the password part of the URL, if applicable, e. g. `http://admin:mysecretpassword@localhost:1234/uri` output: `http://admin:xxxxx@localhost:1234/uri`          |

## Example: Masking employee national ID number

In the following example, the national ID number of an employee is masked.

``` yaml title="Masking transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "Masking"
      params:
        column: "nationalidnumber"
        type: "id"
```

```bash title="Expected result"

| column name      | original value | transformed |
|------------------|----------------|-------------|
| nationalidnumber | 295847284      | 295847****  |
```
