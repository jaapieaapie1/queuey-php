# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First release of `queuey/queuey`: a pure-PHP 8.3+ port of the Rust
[queuey](https://github.com/jaapieaapie1/queuey) job-queue library, wire-compatible
with it. The contract both implementations satisfy is
[`docs/INTEROP-SPEC.md`](docs/INTEROP-SPEC.md).

### Added

- **Queue sets from attributes.** `#[Queues(prefix: ...)]` on a PHP enum and
  `#[Queue(...)]` on its cases, resolved by the `QueueSetFromAttributes` trait
  (`queueName()`, `config()`, `all()`, `fromQueueName()`). A case with no
  attribute gets every default and the `snake_case` form of its name, matching
  the Rust derive macro.
- **Jobs from attributes.** `#[OnQueue(Queue, name: ..., retry: ...)]` binds a job
  class to one queue and sets its wire `job_type`, which defaults to the FQCN.
  `PayloadFromProperties` implements `Job::toPayload()` / `Job::fromPayload()` by
  reflection over the constructor parameters, encoding an empty payload as `{}`
  rather than `[]`.
- **Handlers.** `JobHandler` for a class, `FnHandler::for()` for a callable, and
  the optional `ProvidesRetryPolicy` for a per-payload retry policy. Outcomes are
  exceptions: `RetryableFailure`, `FatalFailure`, `JobDeferred`; anything else
  escaping a handler is retryable.
- **`Producer`** with `enqueue()`, `enqueueAfter()`, `defer()` and
  `EnqueueOptions` (`now()`, `after()`, `deferred()`, `withCorrelationId()`), plus
  `Producer::undeclared()` for queues somebody else owns. A job whose queue is not
  part of the producer's set is rejected with `JobBelongsToOtherQueueSet` — PHP's
  runtime stand-in for Rust's compile-time bound.
- **`Worker`** and `WorkerBuilder`: `handler()`, `queues()`, `retryOverride()`,
  `jobRetryOverride()`, `onDeadLetter()`, `jobTimeout()`, `maxJobs()`,
  `closeBackendOnShutdown()`, `logger()`, `clock()`. Run with `run()`,
  `runFor()` or `runUntil()`; `WorkerHandle` carries `shutdown()`, `processed()`
  and `settleFailures()`. `SIGTERM`/`SIGINT` request a graceful shutdown when
  ext-pcntl is loaded.
- **Retries** — `RetryPolicy` and `Backoff` (none, fixed, exponential with base,
  factor, cap and full jitter), resolved per delivery with the precedence
  `ProvidesRetryPolicy` > `jobRetryOverride` > `retryOverride` >
  `#[OnQueue(retry:)]` > `#[Queue(retry:)]` > `RetryPolicy::none()`.
- **Deferral** — `JobDeferred` holds a job for a stated delay without spending an
  attempt and returns it at the queue's highest priority, ahead of the backlog;
  `#[Queue(maxPriority: 0)]` opts a queue out of priorities entirely.
- **Dead letters** — `DeadLetterHook` / `FnDeadLetterHook` called before the
  delivery is settled, for all four `DeadLetterCause` values (`NoHandler`,
  `Decode`, `Fatal`, `Exhausted`), with the dead-letter reason strings the wire
  contract pins.
- **`RabbitMqBackend`** on php-amqplib: the full topology (main queue, `q.dead`,
  a hold queue per delay with a name-derived TTL and self-expiry), publisher
  confirms with mandatory routing, one attempt per publish, publish-before-ack on
  every settle, malformed bodies forwarded verbatim to `q.dead`, and consumer
  tags prefixed `queuey-php.`. Configured with `RabbitMqOptions` (connection
  name, suffixes, granularities, heartbeat and timeouts) and `amqp[s]://` URLs
  whose `heartbeat` and `connection_timeout` query parameters are read exactly as
  lapin reads them.
- **Automatic reconnection** — single-flight, paced by a `ReconnectPolicy`
  (default `BackoffPolicy`: unlimited, jittered, 500ms base, 30s cap), replaying
  every declared `QueueConfig` on the new connection and resubscribing consumers;
  a single closed consumer channel is resubscribed on its own.
- **`MemoryBackend`** for tests: deterministic, clock-driven, with `pending()`,
  `held()`, `dead()`, `acked()`, `succeeded()`, `retried()` and `settled()`
  inspection, plus `FrozenClock` and a pluggable sleeper.
- **`Duration`**, with the same humantime-ish grammar as the Rust macros
  (`"500ms"`, `"30s"`, `"2m"`, `"1h"`, `"7d"`, a bare integer meaning seconds).
- **PSR-3 logging** throughout, at the same levels as the Rust implementation.
- **Cross-language interop harness** (`interop/`): a Rust binary and a PHP script
  speaking one protocol, and `interop/run.sh` running eight scenarios in both
  directions against a live broker.
- **Documentation**: `docs/INTEROP-SPEC.md` (the wire contract) and
  `docs/DESIGN.md` (the port's design).
