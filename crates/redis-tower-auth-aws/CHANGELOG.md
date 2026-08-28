# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-auth-aws-v0.1.0) - 2026-08-26

### Added

- SigV4-presigned ElastiCache IAM credentials for provisioned replication
  groups and serverless caches, with shared caching and proactive rotation
  before the 15-minute token expiry (#475)
