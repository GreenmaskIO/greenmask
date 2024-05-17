# About transformers

Transformers in Greenmask are methods which are applied to obfuscate sensitive data. All Greenmask transformers are
split into the following groups:

- [Transformation engines](transformation_engines.md) — the type of generator used in transformers. Hash (deterministic)
  and random (randomization)
- [Dynamic parameters](dynamic_parameters.md) — transformers that require an input of parameters and generate
  random data based on them.
- [Standard transformers](standard_transformers/index.md) — transformers that require only an input of parameters.
- [Advanced transformers](advanced_transformers/index.md) — transformers that can be modified according to user's needs
  with the help of [custom functions](advanced_transformers/custom_functions/index.md).
- Custom transformers — coming soon...
