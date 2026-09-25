# redis-tower documentation

redis-tower is a Redis client for Rust where every connection is a Tower
`Service`. Commands have concrete request and response types, concurrent work
can be auto-pipelined, and operational policy composes through Tower layers.

If you are evaluating or adopting the project, start here:

- [Migrating from redis-rs](MIGRATING-FROM-REDIS-RS.md) maps the most common
  redis-rs idioms to redis-tower.
- [Migrating from Fred](MIGRATING-FROM-FRED.md) covers client construction,
  command execution, pipelines, transactions, topology, pub/sub, reconnection,
  and shutdown.
- [Production tuning](PRODUCTION-TUNING.md) explains client selection,
  pipeline and pool sizing, timeouts, backpressure, reconnects, observability,
  and graceful shutdown.
- [Cloud and rotating credentials](CLOUD-AUTH.md) covers AWS ElastiCache IAM,
  Microsoft Entra ID, reconnect-time refresh, proactive reauthentication, and
  token-handling boundaries.
- [Serverless and scale-to-zero](SERVERLESS.md) covers deferred connection,
  cold-start lifecycle semantics, invocation deadlines, and shutdown.
- [Pool health probing design](POOL-HEALTH-PROBING.md) records why active
  probing and idle reaping are pool-native, explicitly spawned tasks.
- [Client-side caching](CLIENT-SIDE-CACHING.md) covers cloneable cached clients,
  tracking modes, invalidation races, failure behavior, and cache metrics.
- [Distributed primitives](PRIMITIVES.md) covers fenced locks, leader election,
  expirable semaphores, countdown latches, delayed queues, block-allocated IDs,
  Redis-time GCRA rate limiting, cluster keys, and failure behavior.
- The [command cookbook](COMMAND-COOKBOOK.md) maps typed command categories to
  response shapes, binary data, pipelines and transactions, stream ownership,
  raw/module replies, dedicated sessions, and topology entry points.
- The [client comparison](FEATURE-MATRIX.md) records pinned, primary-source
  contracts for Rust and cross-language Redis clients.
- The [test conformance report](TEST-CONFORMANCE.md) maps generated test
  inventory, topology and server matrices, and destructive fault coverage to
  their source and CI evidence.
- [Differential testing](DIFFERENTIAL-TESTING.md) records the MCP-derived
  semantic corpus against a pinned redis-rs oracle and its normalization rules.
- [Binary data and typed arguments](BINARY-DATA.md) explains byte ownership,
  exact Cluster routing, response types, and the per-family compatibility
  inventory.
- [RESP codec support and validation](PROTOCOL-TESTING.md) defines accepted and
  rejected wire forms, the fragmentation oracle, reviewed fuzz seeds, and
  mutation triage.
- The [release process](RELEASING.md) records the manual prepare/publish split,
  workspace publish order, validation gates, and post-publish verification.

For installation, a quick API overview, and examples across the main modules,
see the [repository README](https://github.com/joshrotenberg/redis-tower).
The typed API reference on [docs.rs](https://docs.rs/redis-tower) provides
crate-level tours, module guides, and compiled examples alongside every public
type and method.

## Local preview

Install [mdBook](https://rust-lang.github.io/mdBook/guide/installation.html),
then run:

```bash
mdbook serve --open
```

For the same non-interactive checks used in CI:

```bash
mdbook build
python3 scripts/check_docs_links.py
```

The generated site is written under `target/mdbook` and is never checked in.
