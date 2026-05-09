# Security Policy

## Supported Versions

Security fixes are generally provided for the following supported versions:

| Version               | Support Status |
|-----------------------|----------------|
| Latest stable release | Supported      |
| Older releases        | Unsupported    |

We recommend always running the latest stable release of Greenmask.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security vulnerability in Greenmask, please report it responsibly by emailing:

**security@greenmask.io**

Alternatively, you may
use [GitHub's private vulnerability reporting](https://github.com/GreenmaskIO/greenmask/security/advisories/new).

### What to include in your report

To help us triage and reproduce the issue quickly, please include:

- A clear description of the vulnerability and its potential impact
- The affected version(s)
- Step-by-step reproduction instructions or a proof-of-concept
- Any relevant configuration snippets (redact sensitive credentials)
- Your suggested severity (Critical / High / Medium / Low)

### What to expect

- **Acknowledgement**: We will acknowledge receipt of your report within **72 hours**.
- **Assessment**: We will provide an initial assessment within **7 days**.
- **Fix & Disclosure**: We aim to address critical vulnerabilities as quickly as reasonably possible and coordinate
  disclosure timelines with reporters when appropriate.

We follow [coordinated vulnerability disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure) and
ask that you refrain from public disclosure until a fix is available.

## Security Model

Greenmask is designed around a stateless, in-flight data transformation architecture intended to minimize long-lived
exposure of sensitive production data.

During anonymization workflows:

- Raw production data is streamed and transformed during logical dumping
- Data may be anonymized before being persisted to external storage
- Greenmask does not require persistent storage of unmasked datasets as part of its normal workflow
- Transformation pipelines execute directly within the dump stream

This architecture is intended to reduce the risk associated with long-lived raw production copies in lower-trust
environments.

Greenmask is designed to support secure development and testing workflows in regulated and compliance-driven
environments.

## Security Considerations

### Database credentials

Greenmask requires database credentials to connect to PostgreSQL (and experimental MySQL support). Ensure:

- Credentials are stored in environment variables or a secrets manager, not hard-coded in config files.
- The database user has only the minimum permissions required for the operation (dump or restore).
- Config files containing connection strings are protected with appropriate filesystem permissions (`chmod 600`).

### Storage backends

When using S3-compatible storage, follow your cloud provider's security best practices:

- Use IAM roles or short-lived credentials instead of long-lived access keys where possible.
- Enable server-side encryption on the target bucket.
- Restrict bucket access to only the principals that need it.

### Transformation configuration

- Treat Greenmask configuration files as sensitive; they may contain transformation logic that reveals which fields hold
  PII.
- The `cmd` transformer executes arbitrary shell commands. Validate and restrict any user-supplied transformer commands.

### Network

- Run Greenmask in a network environment where it can reach the database and storage backend without exposing either to
  the public internet.
- Use TLS for database connections (`sslmode=require` or stronger).

## Scope

The following are generally considered in scope:

- Vulnerabilities in the Greenmask binary and its Go packages
- Insecure default configuration that could lead to data exposure
- Credential or secret leakage in logs, error messages, or output files
- Dependency vulnerabilities with a clear exploitation path in Greenmask

The following are generally considered out of scope:

- Vulnerabilities in the underlying database engine (PostgreSQL, MySQL)
- Issues requiring physical access to the machine
- Social engineering attacks

## Dependency Scanning

We use Go's built-in tooling and automated dependency scanning to monitor for known CVEs in our dependencies. You can
audit the current dependency tree locally with:

```bash
govulncheck ./...
```

Security fixes may be documented in release notes and security advisories where appropriate.

## Acknowledgements

We thank all security researchers who responsibly disclose vulnerabilities and help make Greenmask safer for everyone.