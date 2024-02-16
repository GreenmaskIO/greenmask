# Faker functions

Greenmask uses [go-faker/faker](https://github.com/go-faker/faker) under the hood for generating of synthetic data.

## Faker functions: Address

| Function           | Description                                                                                          | Signature                              |
|--------------------|------------------------------------------------------------------------------------------------------|----------------------------------------|
| `fakerRealAddress` | Generates a random real-world address that includes: city, state, postal code, latitude, and longitude | `fakerRealAddress() (res ReadAddress)` |
| `fakerLatitude`    | Generates random fake latitude                                                                       | `fakerLatitude() (res float64)`        |
| `fakerLongitude`   | Generates random fake longitude                                                                      | `fakerLongitude() (res float64)`       |

## Faker functions: Datetime

| Function          | Description                                                            | Signature                        |
|-------------------|------------------------------------------------------------------------|----------------------------------|
| `fakerUnixTime`   | Generates random Unix time in seconds                                  | `fakerLongitude() (res int64)`   |
| `fakerDate`       | Generates random date with the pattern of `YYYY-MM-DD`                 | `fakerDate() (res string)`       |
| `fakerTimeString` | Generates random time                                                  | `fakerTimeString() (res string)` |
| `fakerMonthName`  | Generates a random month                                               | `fakerMonthName() (res string)`  |
| `fakerYearString` | Generates a random year                                                | `fakerYearString() (res string)` |
| `fakerDayOfWeek`  | Generates a random day of a week                                       | `fakerDayOfWeek() (res string)`  |
| `fakerDayOfMonth` | Generates a random day of a month                                      | `fakerDayOfMonth() (res string)` |
| `fakerTimestamp`  | Generates a random timestamp with the pattern of `YYYY-MM-DD HH:MM:SS` | `fakerTimestamp() (res string)`  |
| `fakerCentury`    | Generates a random century                                             | `fakerCentury() (res string)`    |
| `fakerTimezone`   | Generates a random timezone name                                       | `fakerTimezone() (res string)`   |
| `fakerTimeperiod` | Generates a random time period with the patter of either `AM` or `PM`  | `fakerTimeperiod() (res string)` |

## Faker functions: Internet

| Function          | Description                                                                       | Signature                        |
|-------------------|-----------------------------------------------------------------------------------|----------------------------------|
| `fakerEmail`      | Generates a random email                                                          | `fakerEmail() (res string)`      |
| `fakerMacAddress` | Generates a random MAC address                                                    | `fakerMacAddress() (res string)` |
| `fakerDomainName` | Generates a random domain name                                                    | `fakerDomainName() (res string)` |
| `fakerURL`        | Generates a random URL with the pattern of `https://www.domainname.some/somepath` | `fakerURL() (res string)`        |
| `fakerUsername`   | Generates a random username                                                       | `fakerUsername() (res string)`   |
| `fakerIPv4`       | Generates a random IPv4 address                                                   | `fakerIPv4() (res string)`       |
| `fakerIPv6`       | Generates a random IPv6 address                                                   | `fakerIPv6() (res string)`       |
| `fakerPassword`   | Generates a random password                                                       | `fakerPassword() (res string)`   |

## Faker functions: words and sentences

| Function         | Description                                             | Signature                       |
|------------------|---------------------------------------------------------|---------------------------------|
| `fakerWord`      | Generates a random word                                 | `fakerWord() (res string)`      |
| `fakerSentence`  | Generates a random sentence                             | `fakerSentence() (res string)`  |
| `fakerParagraph` | Generates a random sequence of sentences as a paragraph | `fakerParagraph() (res string)` |

## Faker functions: Payment

| Function                  | Description                                                      | Signature                                |
|---------------------------|------------------------------------------------------------------|------------------------------------------|
| `fakerCCType`             | Generates a random credit card type, e.g. VISA, MasterCard, etc. | `fakerCCType() (res string)`             |
| `fakerCCNumber`           | Generates a random credit card number                            | `fakerCCNumber() (res string)`           |
| `fakerCurrency`           | Generates a random currency name                                 | `fakerCurrency() (res string)`           |
| `fakerAmountWithCurrency` | Generates random amount preceded with random currency            | `fakerAmountWithCurrency() (res string)` |

## Faker functions: Person

| Function               | Description                                              | Signature                             |
|------------------------|----------------------------------------------------------|---------------------------------------|
| `fakerTitleMale`       | Generates a random male title from the predefined list   | `fakerTitleMale() (res string)`       |
| `fakerTitleFemale`     | Generates a random female title from the predefined list | `fakerTitleFemale() (res string)`     |
| `fakerFirstName`       | Generates a random first name                            | `fakerFirstName() (res string)`       |
| `fakerFirstNameMale`   | Generates a random male first name                       | `fakerFirstNameMale() (res string)`   |
| `fakerFirstNameFemale` | Generates a random female first name                     | `fakerFirstNameFemale() (res string)` |
| `fakerFirstLastName`   | Generates a random last name                             | `fakerFirstLastName() (res string)`   |
| `fakerName`            | Generates a random full name preceded with a title       | `fakerName() (res string)`            |

## Faker functions: Phone

| Function                   | Description                                                          | Signature                                 |
|----------------------------|----------------------------------------------------------------------|-------------------------------------------|
| `fakerPhoneNumber`         | Generates a random phone number                                      | `fakerPhoneNumber() (res string)`         |
| `fakerTollFreePhoneNumber` | Generates a random phone number with the pattern of `(123) 456-7890` | `fakerTollFreePhoneNumber() (res string)` |
| `fakerE164PhoneNumber`     | Generates a random phone number with the pattern of `+12345678900`   | `fakerE164PhoneNumber() (res string)`     |

## Faker functions: UUID

| Function              | Description                                            | Signature                       |
|-----------------------|--------------------------------------------------------|---------------------------------|
| `fakerUUIDHyphenated` | Generates a random unique user ID separated by hyphens | `fakerUUID() (res string)`      |
| `fakerUUIDDigit`      | Generates a random unique user ID in the HEX format    | `fakerUUIDDigit() (res string)` |
