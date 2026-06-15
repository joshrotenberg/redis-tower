# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately** so they can be fixed
before public disclosure.

- Preferred: use GitHub's [private vulnerability reporting][gh-private] -- open
  the **Security** tab and choose **Report a vulnerability**.
- Alternatively, email the maintainer at <joshrotenberg@gmail.com> with the
  details and a way to reach you.

Please include:

- the affected crate(s) and version(s),
- a description of the vulnerability and its impact,
- steps to reproduce or a proof of concept, if available.

You can expect an acknowledgement within a few business days. We will keep you
informed of progress toward a fix and coordinate disclosure timing with you.

Please **do not** open a public issue for a security vulnerability.

## Supported versions

redis-tower is pre-1.0 and under active development. Security fixes are applied
to the latest released minor version on the `0.x` series. See the stability and
versioning policy in the [README](README.md#stability-and-versioning) for the
support and deprecation model.

## Supply chain

Every pull request runs `cargo deny` (advisories, licenses, bans, sources) and
`cargo audit` against the [RustSec advisory database](https://rustsec.org). The
policy lives in [`deny.toml`](deny.toml). The workspace contains no `unsafe`
code -- every crate sets `#![forbid(unsafe_code)]`.

[gh-private]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability
