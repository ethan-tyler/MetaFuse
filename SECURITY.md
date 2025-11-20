# Security Policy

## Supported Versions

We currently support the following versions of MetaFuse with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

As the project matures, we will update this table to reflect our long-term support policy.

## Reporting a Vulnerability

We take the security of MetaFuse seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please email security concerns to:

**ethan@urbanskitech.com**

You should receive a response within **24-48 hours** for critical issues. If for some reason you do not, please follow up to ensure we received your original message.

### What to Include

Please include the following information in your report to help us better understand the nature and scope of the issue:

* Type of issue (e.g., buffer overflow, SQL injection, XSS, etc.)
* Full paths of source file(s) related to the manifestation of the issue
* The location of the affected source code (tag/branch/commit or direct URL)
* Any special configuration required to reproduce the issue
* Step-by-step instructions to reproduce the issue
* Proof-of-concept or exploit code (if possible)
* Impact of the issue, including how an attacker might exploit it

This information will help us triage your report more quickly.

### Our Commitment

* We will acknowledge your email within **24-48 hours** for critical vulnerabilities, and **3-5 business days** for lower-severity issues
* We will send a more detailed response within **7 days** indicating the next steps in handling your report
* We will keep you informed about the progress towards a fix and full announcement
* We will credit you in the security advisory (unless you prefer to remain anonymous)

## Preferred Languages

We prefer all communications to be in English.

## Security Update Policy

When a security issue is identified and confirmed:

1. We will prepare a fix in a private repository
2. We will release a new version with the fix
3. We will publish a security advisory on GitHub
4. We will credit the reporter (unless anonymity is requested)

## Known Security Considerations

### SQLite Injection

MetaFuse uses prepared statements throughout the codebase to prevent SQL injection. If you find any raw SQL concatenation, please report it.

### Catalog File Access

The local SQLite backend stores data in files. Ensure proper file permissions are set in production environments:

```bash
chmod 600 metafuse_catalog.db
```

### API Authentication (Future)

The current v0.1.x API does not include authentication. **Do not expose the API to untrusted networks.**

Authentication and authorization are planned for future releases (see [roadmap](docs/roadmap.md)).

## Secure Development Practices

Our development process includes:

* Dependency scanning via `cargo audit` in CI
* Regular dependency updates via Dependabot
* Code review requirements for all PRs
* Static analysis with `clippy`
* Memory safety guarantees from Rust

## Contact

For non-security-related questions, please use:
* GitHub Issues: [https://github.com/ethan-tyler/MetaFuse/issues](https://github.com/ethan-tyler/MetaFuse/issues)
* GitHub Discussions: [https://github.com/ethan-tyler/MetaFuse/discussions](https://github.com/ethan-tyler/MetaFuse/discussions)

For security issues only: **ethan@urbanskitech.com**
