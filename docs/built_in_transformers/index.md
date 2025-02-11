# About transformers

Transformers in Greenmask are methods which are applied to anonymize sensitive data. All Greenmask transformers are
split into the following groups:

- [Dynamic parameters](dynamic_parameters.md) — transformers that require an input of parameters and generate
  random data based on them.
- [Transformation engines](transformation_engines.md) — the type of generator used in transformers. Hash (deterministic)
  and random (randomization)
- [Parameters templating](parameters_templating.md) — generate static parameters values from templates.
- [Transformation conditions](transformation_condition.md) — conditions that can be applied to transformers. If the
  condition is not met, the transformer will not be applied.
- [Transformation Inheritance](transformation_inheritance.md) — transformation inheritance for partitioned tables and
  tables with foreign keys. Define once and apply to all.
- [Standard transformers](standard_transformers/index.md) — transformers that require only an input of parameters.
- [Advanced transformers](advanced_transformers/index.md) — transformers that can be modified according to user's needs
  with the help of [custom functions](advanced_transformers/custom_functions/index.md).
- Custom transformers — coming soon...
