# Core functions

Below you can find custom core functions which are divided into categories based on the transformation purpose.

## PostgreSQL driver functions

| Function      | Description                                                                                                     |
|---------------|-----------------------------------------------------------------------------------------------------------------|
| `null`        | Returns the `NULL` value that can be used for the driver encoding-decoding operations                           |
| `isNull`      | Returns `true` if the checked value is `NULL`                                                                   |
| `isNotNull`   | Returns `true` if the checked value is *not* `NULL`                                                             |
| `sqlCoalesce` | Works as a standard SQL `coalesce` function. It allows you to choose the first non-NULL argument from the list. |

## JSON output function

| Function         | Description                                                                              |
|------------------|------------------------------------------------------------------------------------------|
| `jsonExists`     | Checks if the path value exists in JSON. Returns `true` if the path exists.              |
| `mustJsonGet`    | Gets the JSON attribute value by path and throws an error if the path does not exist     |
| `mustJsonGetRaw` | Gets the JSON attribute raw value by path and throws an error if the path does not exist |
| `jsonGet`        | Gets the JSON attribute value by path and returns nil if the path does not exist         |
| `jsonGetRaw`     | Gets the JSON attribute raw value by path and returns nil if the path does not exist     |
| `jsonSet`        | Sets the value for the JSON document by path                                             |
| `jsonSetRaw`     | Sets the raw value for the JSON document by path                                         |
| `jsonDelete`     | Deletes an attribute from the JSON document by path                                      |
| `jsonValidate`   | Validates the JSON document syntax and throws an error if there are any issues           |
| `jsonIsValid`    | Checks the JSON document for validity and returns `true` if it is valid                  |
| `toJsonRawValue` | Casts any type of value to the raw JSON value                                            |

## Testing functions

| Function   | Description                            |
|------------|----------------------------------------|
| `isInt`    | Checks if the value of an integer type |
| `isFloat`  | Checks if the value of a float type    |
| `isNil`    | Checks if the value is nil             |
| `isString` | Checks if the value of a string type   |
| `isMap`    | Checks if the value of a map type      |
| `isSlice`  | Checks if the value of a slice type    |
| `isBool`   | Checks if the value of a boolean type  |

## Transformation and generators

### masking

Replaces characters with asterisk `*` symbols depending on the provided masking rule. If the
value is `NULL`, it is kept unchanged. This function is based on [ggwhite/go-masker](https://github.com/ggwhite/go-masker).

=== "Masking rules"

    | Rule          | Description                                                                                                      | Example input                                      | Example output                          |
    |---------------|------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|-----------------------------------------|
    | `default`     | Returns the sequence of `*` symbols of the same length                                                           | `test1234`                                         | `********`                              |
    | `name`        | Masks the second and the third letters                                                                           | `ABCD`                                             | `A**D`                                  |
    | `password`    | Always returns a sequence of `*`                                                                                 |                                                    |                                         |
    | `address`     | Keeps first 6 letters, masks the rest                                                                            | `Larnaca, makarios st`                             | `Larnac*************`                   |
    | `email`       | Keeps a domain and the first 3 letters, masks the rest                                                           | `ggw.chang@gmail.com`                              | `ggw****@gmail.com`                     |
    | `mobile`      | Masks 3 digits starting from the 4th digit                                                                       | `0987654321`                                       | `0987***321`                            |
    | `telephone`   | Removes `(`, `)`, ` `, `-` symbols, masks last 4 digits of a telephone number, and formats it to `(??)????-????` | `0227993078`                                       | `(02)2799-****`                         |
    | `id`          | Masks last 4 digits of an ID                                                                                     | `A123456789`                                       | `A12345****`                            |
    | `credit_card` | Masks 6 digits starting from the 7th digit                                                                       | `1234567890123456`                                 | `123456******3456`                      |
    | `url`         | Masks the password part of the URL (if applicable)                                                               | `http://admin:mysecretpassword@localhost:1234/uri` | `http://admin:xxxxx@localhost:1234/uri` |

=== "Signature"

    `masking(dataType string, value string) (res string, err error)`

=== "Parameters"

    * `dataType` — one of the masking rules (see previous tab)
    * `value` — the original string value

=== "Return values"

    * `res` — a masked string
    * `err` — an error if there is an issue

### truncateDate

Truncates datetime up to the provided `part`.

=== "Signature"

    `truncateDate(part string, original time.Time) (res time.Time, err error)`

=== "Parameters"

    * `part` — the truncation part. Must be one of `nano`, `second`, `minute`, `hour`, `day`, `month`, or `year`
    * `original` — the original datetime value

=== "Return values"

    * `res` — a truncated datetime
    * `err` — an error if there is an issue

### noiseDatePgInterval

Adds or subtracts a random duration in the provided `interval` to or from the original date value.

=== "Signature"

    `noiseDate(interval string, original time.Time) (res time.Time, err error)`

=== "Parameters"

    * `interval` — the maximum value of `ratio` that is added to the original value. The format is the same as in the [PostgreSQL interval format](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT).
    * `original` — the original time value

=== "Return values"

    * `res` — a noised date
    * `err` — an error if there is an issue

### noiseFloat

Adds or subtracts a random fraction to or from the original float value. Multiplies the original float value by a provided random value that is not higher than the `ratio` parameter and adds it to the original value with the option to specify the decimal via the `decimal` parameter.

=== "Signature"

    `noiseFloat(ratio float, decimal int, value float) (res float64, err error)`

=== "Parameters"

    * `ratio` — the maximum multiplier value in the interval (0:1). The value will be randomly generated up to `ratio`, multiplied by the original value, and the result will be added to the original value.
    * `decimal` — the decimal of the resulted value
    * `value` — the original value

=== "Return values"

    * `res` — a noised float value
    * `err` — an error if there is an issue

### noiseInt

Adds or subtracts a random fraction to or from the original integer value. Multiplies the original integer value by a provided random value that is not higher than the `ratio` parameter and adds it to the original value.

=== "Signature"

    `noiseInt(ratio float, value float) (res int, err error)`

=== "Parameters"

    * `ratio` — the max multiplier value in the interval (0:1). The value will be generated randomly up to `ratio`, multiplied by the original value, and the result will be added to the original value.
    * `value` — the original value

=== "Return values"

    * `res` — a noised integer value
    * `err` — an error if there is an issue

### randomBool

Generates a random boolean value.

### randomDate

Generates a random date within the provided interval.

=== "Signature"

    `randomDate(min time.Time, max time.Time) (res time.Time, err error)`

=== "Parameters"

    * `min` — the minimum random value threshold
    * `max` — the maximum random value threshold

=== "Return values"

    * `res` — a randomly generated date value
    * `err` — an error if there is an issue

### randomFloat

Generates a random float value within the provided interval.

=== "Signature"

    `randomFloat(min any, max any, decimal int) (res float, err error)`

=== "Parameters"

    * `min` — the minimum random value threshold
    * `max` — the maximum random value threshold
    * `decimal` — the decimal of the resulted value

=== "Return values"

      * `res` — a randomly generated float value
      * `err` — an error if there is an issue

### randomInt

Generates a random integer value within the provided interval.

=== "Signature"

    `randomInt(min int, max int) (res int, err error)`

=== "Parameters"

    * `min` — the minimum random value threshold
    * `max` — the maximum random value threshold

=== "Return values"

    * `res` — a randomly generated int value
    * `err` — an error if there is an issue

### randomString

Generates a random string using the provided characters within the specified length range.

=== "Signature"

    `randomString(minLength int, maxLength int, symbols string) (res string, err error)`

=== "Parameters"

    * `minLength` — the minimum string length
    * `maxLength` — the maximum string length
    * `symbols` — a string with a set of symbols which can be used. The default value is
      `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`

=== "Return values"

    * `res` — a randomly generated string value
    * `err` — an error if there is an issue

### roundFloat

Rounds a float value up to provided decimal.

=== "Signature"

    `roundFloat(decimal int, original float) (res float, err error)`

=== "Parameters"

    * `decimal` — the decimal of the value
    * `original` — the original float value

=== "Return values"

    * `res` — a rounded float value
    * `err` — an error if there is an issue
