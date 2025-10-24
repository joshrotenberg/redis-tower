# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to: josh@gifsicle.net

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include the following information (as much as you can provide) to help us better understand the nature and scope of the possible issue:

* Type of issue (e.g. buffer overflow, SQL injection, cross-site scripting, etc.)
* Full paths of source file(s) related to the manifestation of the issue
* The location of the affected source code (tag/branch/commit or direct URL)
* Any special configuration required to reproduce the issue
* Step-by-step instructions to reproduce the issue
* Proof-of-concept or exploit code (if possible)
* Impact of the issue, including how an attacker might exploit the issue

This information will help us triage your report more quickly.

## Security Update Process

1. The security issue is received and assigned a primary handler
2. The problem is confirmed and affected versions are determined
3. Code is audited to find any similar problems
4. Fixes are prepared for all supported releases
5. New versions are released and security advisory is published

## Security Best Practices

When using redis-tower:

### Connection Security
- **Use TLS**: When available (future versions), always use TLS for production Redis connections
- **Authentication**: Always use Redis AUTH or ACL authentication
- **Network Isolation**: Keep Redis servers on private networks
- **Firewall Rules**: Restrict Redis access to known application servers

### Command Safety
- **Input Validation**: Always validate user input before using in Redis commands
- **Key Naming**: Use consistent key naming patterns to avoid conflicts
- **Script Security**: Be cautious with EVAL/EVALSHA commands and user-provided scripts
- **Privilege Separation**: Use Redis ACLs to limit command access per client

### Application Security
- **Secret Management**: Never hardcode Redis passwords in source code
- **Environment Variables**: Use environment variables or secret management systems
- **Least Privilege**: Grant minimum necessary Redis permissions
- **Audit Logging**: Enable Redis command logging for sensitive operations

### Denial of Service Protection
- **Connection Limits**: Configure connection pool limits appropriately
- **Timeout Settings**: Set reasonable timeouts to prevent hanging connections
- **Rate Limiting**: Use Tower middleware for rate limiting if needed
- **Memory Limits**: Configure Redis maxmemory settings

### Data Protection
- **Encryption at Rest**: Enable Redis encryption if handling sensitive data
- **Key Expiration**: Set TTLs on sensitive data
- **Data Sanitization**: Sanitize data before storage
- **Regular Backups**: Maintain regular Redis backups

## Known Security Considerations

### RESP Protocol
- redis-tower currently uses RESP2 protocol
- RESP3 with improved security features is planned for v0.2.0
- No known RESP2 vulnerabilities in our implementation

### Cluster Mode
- Cluster slot redirects are handled automatically
- ASKING command is used appropriately
- No known cluster-specific vulnerabilities

### Scripting
- EVAL/EVALSHA commands are supported
- Users are responsible for script safety
- No sandboxing is provided beyond Redis's built-in protections

## Dependencies

We regularly update dependencies to patch security vulnerabilities. Major dependencies:

- **Tower**: Well-maintained, security-focused ecosystem
- **Tokio**: Industry-standard async runtime
- **resp-parser**: Custom RESP parser (regularly audited)

Run `cargo audit` to check for known vulnerabilities in dependencies.

## Security Features

### Current (v0.1.x)
- Strong typing prevents command injection
- No unsafe code (`#![deny(unsafe_code)]`)
- Comprehensive error handling
- Feature-gated optional functionality

### Planned (v0.2.x+)
- TLS support
- Enhanced connection security
- RESP3 protocol support
- Client-side caching with secure invalidation

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine affected versions
2. Audit code to find similar problems
3. Prepare fixes for supported versions
4. Release new versions as quickly as possible
5. Publish security advisory with CVE if appropriate

We aim to disclose security vulnerabilities responsibly:
- Private disclosure period: 7-14 days
- Coordinated disclosure with affected parties
- Public disclosure after fix is available

## Credit

We appreciate the security research community's efforts in responsibly disclosing vulnerabilities. Security researchers who report valid vulnerabilities will be credited in:

- Security advisory
- CHANGELOG.md
- GitHub security advisory (if applicable)

## Contact

For security issues: josh@gifsicle.net  
For general issues: https://github.com/joshrotenberg/redis-tower/issues

## Additional Resources

- [Redis Security](https://redis.io/docs/management/security/)
- [Tower Security](https://github.com/tower-rs/tower/security)
- [Rust Security](https://www.rust-lang.org/policies/security)
