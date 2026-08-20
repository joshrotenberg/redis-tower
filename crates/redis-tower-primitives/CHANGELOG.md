# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `DistributedLock` with required TTL, random owner tokens, atomic fencing,
  compare-and-delete release, extension, and an explicitly spawned owned
  renewal lifecycle (#490)
- `GcraRateLimiter` with required quota/window, Redis server time, single-key
  sorted-set state, and server-computed remaining/retry/reset values (#490)
- public, line-documented Lua constants and live Redis safety coverage (#490)
- `LeaderElection` with owned renewal, explicit abdication, and separable
  elected/renewal-failed/demoted events (#491)
- `ExpirableSemaphore` with Redis-time lease pruning and token-checked renew
  and release operations (#491)
- `CountDownLatch` with initialize-if-absent, non-negative atomic countdown,
  and explicit timeout/expiry-aware polling (#491)
