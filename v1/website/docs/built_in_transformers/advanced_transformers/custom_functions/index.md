# Template custom functions

Within Greenmask, custom functions play a crucial role, providing a wide array of options for implementing diverse
logic. Under the hood, the custom functions are based on
the [sprig Go's template functions](https://masterminds.github.io/sprig/). Greenmask enhances this capability by
introducing additional functions and transformation functions. These extensions mirror the logic found in
the [standard transformers](../../standard_transformers/index.md) but offer you the flexibility to implement intricate
and comprehensive logic tailored to your specific needs.

Currently, you can use template custom functions for the [advanced transformers](../index.md):

* [Json](../json.md)
* [Template](../template.md)
* [TemplateRecord](../template_record.md)

and for the [Transformation condition feature](../../transformation_condition.md) as well.

Custom functions are arbitrarily divided into 2 groups:

- [Core functions](core_functions.md) — custom functions that vary in purpose and include PostgreSQL driver, JSON
  output, testing, and transformation functions.
- [Faker functions](faker_function.md) — custom function of a *faker* type which generate synthetic data.
