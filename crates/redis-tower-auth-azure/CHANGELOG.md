# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Microsoft Entra ID credentials from managed identity or any Azure SDK
  `TokenCredential`, with shared caching and proactive refresh at approximately
  75% of token lifetime (#475)
