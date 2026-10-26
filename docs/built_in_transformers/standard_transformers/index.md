---
title: "Index"
description: "Standard transformers are ready-to-use methods that require no customization and perform with just as little as parameters input. Below you can find an…"
keywords: ["standard transformers", "greenmask", "data anonymization", "greenmask transformer", "postgresql", "mysql", "oracle", "Enterprise support", "Open-Source", "PostgreSQL anonymization", "test data management", "compliance", "security", "agentic pipeline", "development cycle"]
---

# Standard transformers

Standard transformers are ready-to-use methods that require no customization and perform with just as little as parameters input. Below you can find an index of all standard transformers currently available in Greenmask.

## Transformer names

Transformer names are case-sensitive and written in PascalCase, exactly as `greenmask list-transformers` prints them. A name such as `random_email` or `nullify` fails validation with `transformer not found`. The table maps common search terms to the actual transformer names:

| You are looking for                     | Transformer name                                                                                  |
|-----------------------------------------|---------------------------------------------------------------------------------------------------|
| nullify, set to NULL                    | [SetNull](set_null.md)                                                                            |
| random text                             | [RandomString](random_string.md), [RandomWord](random_word.md), [RandomSentence](random_sentence.md), [RandomParagraph](random_paragraph.md) |
| random email                            | [RandomEmail](random_email.md)                                                                    |
| hash                                    | [Hash](hash.md)                                                                                   |
| shift a date (date noise)               | [NoiseDate](noise_date.md)                                                                        |
| random IP address                       | [RandomIp](random_ip.md)                                                                          |
| constant value                          | [Replace](replace.md)                                                                             |

## Applying transformers in the config

Transformers are set per table under `dump.transformation`. Each table entry has `schema`, `name` and a `transformers` list; each transformer has a `name` and `params`:

```yaml title="dump.transformation example"
dump:
  transformation:
    - schema: "public"
      name: "account"
      transformers:
        - name: "RandomEmail"
          params:
            column: "email"
            engine: "hash"
        - name: "RandomString"
          params:
            column: "login"
            min_length: 8
            max_length: 16
        - name: "RandomSentence"
          params:
            column: "bio"
        - name: "Hash"
          params:
            column: "external_id"
            function: "sha256"
        - name: "NoiseDate"
          params:
            column: "birth_date"
            max_ratio: "1 year"
            truncate: "day"
        - name: "RandomIp"
          params:
            column: "last_login_ip"
            subnet: "10.0.0.0/8"
        - name: "SetNull"
          params:
            column: "notes"
```

See [Configuration](../../configuration.md) for the rest of the `dump` section.

## The keep_null parameter

`keep_null` controls what happens when the original value is `NULL`:

* `true` — the `NULL` is left as is and nothing is generated for this row.
* `false` — the transformer generates a value for the row as if the original were not `NULL`.

Non-`NULL` values are transformed either way. Transformers that support `keep_null` and its default:

| Transformers                                                                                                                                                                                                                                                                                                      | Default |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| RandomBool, RandomChoice, RandomDate, RandomUnixTimestamp, RandomEmail, RandomFloat, RandomInt, RandomNumeric, RandomString, RandomUuid, HashedPassword, Replace, [Json](../advanced_transformers/json.md)                                                                                                                             | `true`  |
| RandomLatitude, RandomLongitude, RandomMonthName, RandomYearString, RandomDayOfWeek, RandomDayOfMonth, RandomCentury, RandomTimezone, RandomDomainName, RandomURL, RandomUsername, RandomPassword, RandomWord, RandomSentence, RandomParagraph, RandomCCType, RandomCCNumber, RandomCurrency, RandomAmountWithCurrency, RandomPhoneNumber, RandomTollFreePhoneNumber, RandomE164PhoneNumber | `true`  |
| RandomPerson, RandomCompany (set per item in `columns`)                                                                                                                                                                                                                                                           | `true`  |
| RealAddress (set per item in `columns`)                                                                                                                                                                                                                                                                           | `false` |

Transformers without `keep_null` handle `NULL` in a fixed way:

* `Hash`, `NoiseDate`, `NoiseFloat`, `NoiseInt`, `NoiseNumeric`, `Masking` and `RegexpReplace` always leave `NULL` unchanged.
* `RandomIp` always generates a value, also for `NULL`.
* `Dict` can map `NULL` like any other value, see [Dict](dict.md).
* `SetNull` always writes `NULL`.

## All standard transformers

1. [Cmd](cmd.md) — transforms data via external program using `stdin` and `stdout` interaction.
1. [Dict](dict.md) — replaces values matched by dictionary keys.
1. [Hash](hash.md) — generates a salted hash of the text value.
1. [HashedPassword](hashed_password.md) — replaces a password hash with a bcrypt hash of one known password.
1. [Masking](masking.md) — masks a value using one of the masking behaviors depending on your domain.
1. [NoiseDate](noise_date.md) — randomly adds or subtracts a duration within the provided ratio interval to the original date value.
1. [NoiseFloat](noise_float.md) — adds or subtracts a random fraction to the original float value.
1. [NoiseNumeric](noise_numeric.md) — adds or subtracts a random fraction to the original numeric value.
1. [NoiseInt](noise_int.md) — adds or subtracts a random fraction to the original integer value.
1. [RandomBool](random_bool.md) — generates random boolean values.
1. [RandomChoice](random_choice.md) — replaces values randomly chosen from a provided list.
1. [RandomDate](random_date.md) — generates a random date in a specified interval.
1. [RandomFloat](random_float.md) — generates a random float within the provided interval.
1. [RandomInt](random_int.md) — generates a random integer within the provided interval.
1. [RandomString](random_string.md) — generates a random string using the provided characters within the specified length range.
1. [RandomUuid](random_uuid.md) — generates a random unique user ID.
1. [RandomLatitude](random_latitude.md) — generates a random latitude value.
1. [RandomLongitude](random_longitude.md) — generates a random longitude value.
1. [RandomUnixTimestamp](random_unix_timestamp.md) — generates a random Unix timestamp.
1. [RandomDayOfWeek](random_day_of_week.md) — generates a random day of the week.
1. [RandomDayOfMonth](random_day_of_month.md) — generates a random day of the month.
1. [RandomMonthName](random_month_name.md) — generates the name of a random month.
1. [RandomYearString](random_year_string.md) — generates a random year as a string.
1. [RandomCentury](random_century.md) — generates a random century.
1. [RandomCompany](random_company.md) — generates a random company.
1. [RandomTimezone](random_timezone.md) — generates a random timezone.
1. [RandomEmail](random_email.md) — generates a random email address.
1. [RandomUsername](random_username.md) — generates a random username.
1. [RandomPassword](random_password.md) — generates a random password.
1. [RandomDomainName](random_domain_name.md) — generates a random domain name.
1. [RandomURL](random_url.md) — generates a random URL.
1. [RandomMac](random_mac.md) — generates a random MAC addresses.
1. [RandomIp](random_ip.md) — generates a random IPv4 or IPv6 addresses.
1. [RandomWord](random_word.md) — generates a random word.
1. [RandomSentence](random_sentence.md) — generates a random sentence.
1. [RandomParagraph](random_paragraph.md) — generates a random paragraph.
1. [RandomCCType](random_cc_type.md) — generates a random credit card type.
1. [RandomCCNumber](random_cc_number.md) — generates a random credit card number.
1. [RandomCurrency](random_currency.md) — generates a random currency code.
1. [RandomAmountWithCurrency](random_amount_with_currency.md) — generates a random monetary amount with currency.
1. [RandomPerson](random_person.md) — generates a random person data (first name, last name, etc.)
1. [RandomPhoneNumber](random_phone_number.md) — generates a random phone number.
1. [RandomTollFreePhoneNumber](random_toll_free_phone_number.md) — generates a random toll-free phone number.
1. [RandomE164PhoneNumber](random_e164_phone_number.md) — generates a random phone number in E.164 format.
1. [RealAddress](real_address.md) — generates a real address.
1. [RegexpReplace](regexp_replace.md) — replaces a string using a regular expression.
1. [Replace](replace.md) — replaces an original value by the provided one.
1. [SetNull](set_null.md) — sets `NULL` value to the column.
