# Advanced transformers

Advanced transformers are modifiable anonymization methods that users can adjust based on their needs by using [custom functions](custom_functions/index.md).

Below you can find an index of all advanced transformers currently available in Greenmask.

1. [Json](json.md) — changes a JSON content by using `delete` and `set` operations.
2. [Template](template.md) — executes a Go template of your choice and applies the result to a specified column.
3. [TemplateRecord](template_record.md) — modifies records by using a Go template of your choice and applies the changes via the PostgreSQL
driver.
