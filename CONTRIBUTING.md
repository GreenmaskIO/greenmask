# Contributing to Greenmask

Thank you for your interest in Greenmask! Bug reports, ideas and pull requests are welcome.

## Before you start

* For a bug, open an [issue](https://github.com/GreenmaskIO/greenmask/issues) with the Greenmask version, the PostgreSQL version and the steps to reproduce it.
* For a new feature or a larger change, open an issue or ask in [Discord](https://discord.gg/tAJegUKSTB) first, so we can agree on the approach before you write the code.

## Code

* `make build` builds the binary, `make tests` runs the unit tests and `make lint` runs `golangci-lint`.
* `make integration-local` runs the integration tests in Docker.

## Documentation

The documentation lives in `docs/` and is published on [greenmask.io](https://greenmask.io) with Docusaurus. The published site has its own config and styles, so this repository has a small preview project in `tools/docs-preview/` that renders `docs/` with the stock Docusaurus theme.

Preview the docs locally (requires Node.js 20 or newer):

```shell
make docs-serve   # dev server with live reload on http://localhost:3000
make docs-build   # production build into tools/docs-preview/build
```

`make docs-build` fails on broken Markdown links. Links to other products of the site, such as `/docs/arcave/`, are reported as warnings, since they exist only on the published site.

When you change the docs:

* Add new pages to `docs/sidebars.json`.
* Use the bracket syntax for admonition titles: `:::warning[Title]`, not `:::warning Title`.
* Check that admonitions, tabs and code blocks render as expected in `make docs-serve`.
