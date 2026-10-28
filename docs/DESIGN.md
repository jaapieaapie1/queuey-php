# queuey-php design (v1)

Pure PHP 8.3+ port of `../queuey`. Wire contract: `docs/INTEROP-SPEC.md` (authoritative).
Framework-free; Laravel/Symfony wrappers come later as separate packages.
This is the *why*; the user-facing tour of the same surface is the
[README](../README.md).

## Package

- Composer `queuey/queuey`, namespace `Queuey\`, PSR-4 `src/` → `Queuey\`, `tests/` → `Queuey\Tests\`.
- `"php": "^8.3"` (64-bit build: millisecond timestamps do not fit a 32-bit `int`), `ext-json`, `ext-mbstring` (Unicode `snake_case`), `php-amqplib/php-amqplib: ^3.7`, `psr/log: ^3`. No other runtime deps (UUID v4 via `random_bytes`).
- Dev: `pestphp/pest ^4` (which brings PHPUnit 12), `phpstan/phpstan ^2` (level max, `phpstan.neon.dist`), `friendsofphp/php-cs-fixer ^3` (`@PER-CS2.0` + `declare_strict_types`, `.php-cs-fixer.dist.php`).
- Every file: `declare(strict_types=1);`. `final` by default. Readonly value objects. Native types everywhere; generics via PHPDoc `@template`.
- Scripts: `composer test` (Pest, unit suite only), `composer test:integration` (needs `AMQP_URL`), `composer test:all`, `composer stan`, `composer cs` / `composer cs:fix`, `composer check` (cs + stan + unit).
- Layout:
  ```
  src/                  core (framework- and broker-agnostic)
  src/Attributes/       #[Queues], #[Queue], #[OnQueue]
  src/Memory/           MemoryBackend (tests, local dev)
  src/RabbitMq/         php-amqplib backend
  tests/Unit/           no broker
  tests/Integration/    skipped unless AMQP_URL is set (print a skip notice)
  interop/              cross-language harness (rust bin + php scripts + run.sh)
  ```

## Core types (`Queuey\`)

### `Duration` (final, readonly properties)
Integer **milliseconds** internally (`int $ms >= 0`).
`Duration::ms(int)`, `::seconds(int|float)`, `::minutes()`, `::hours()`, `::days()`, `::zero()`,
`::parse(string)` — humantime-ish: `"500ms"`, `"1s"`, `"2m"`, `"1h"`, `"7d"`, bare int = seconds
(same grammar as the Rust macros; also used in attributes).
`toMs(): int`, `toSeconds(): float`, `isZero()`, `equals()`, `compare()`, `min()/max()`.
Sub-ms inputs round **up**.

### `Envelope` (final)
Public readonly-ish fields exactly as §1 of the spec: `string $jobId, string $jobType, string $queue, int $attempt, int $enqueuedAtMs, int $deferrals, int $priority, ?string $correlationId, mixed $payload`.
- `Envelope::new(Job $job): self` (uuid v4, attempt 1, now ms, payload = `$job->toPayload()` normalised to the faithful form).
- `Envelope::raw(string $jobType, string $queue, mixed $payload): self`.
- `withCorrelationId(string): self`, `nextAttempt(): self`, `deferred(int $priority): self` (pure; return new instances).
- `toJson(): string`, `Envelope::fromJson(string): self` (throws `MalformedEnvelope`).
- `payload` is kept in **object-faithful** form (`stdClass` for JSON objects). `decode(class-string<J>): J` hands `J::fromPayload()` the array form (`Json::toArray`). Helper `Queuey\Internal\Json` with `encode()`, `decode()`, `toArray()`.
- `age(Clock): Duration`.

### `Clock` interface (`nowMs(): int`) + `SystemClock`, `FrozenClock(int $ms)->advance(Duration)`.

### Retry
`Backoff` (final): static ctors `none()`, `fixed(Duration)`, `exponential(Duration $base = 1s, float $factor = 2.0, Duration $max = 5m, bool $jitter = true)`; `delayFor(int $failedAttempt): Duration`; `kind(): BackoffKind` enum (`None|Fixed|Exponential`) plus getters. Math per spec §5; jitter via `random_int` on ms (uniform `[0, d]`).
`RetryPolicy` (final, readonly properties): `int $maxAttempts (>=1)`, `Backoff $backoff`; `::none()`, `::fixed(int, Duration)`, `::exponential(int)`, `::new(...)`; `decide(int $failedAttempt): RetryDecision`.
`RetryDecision` (final, readonly properties): `::retry(Duration)`, `::giveUp()`, `isRetry()`, `delay(): ?Duration`.

### Queue configuration
`QueueConfig` (final, readonly, withers): `string $name, int $prefetch = 16, RetryPolicy $retry = none, bool $durable = true, ?Duration $messageTtl = null, ?int $maxPriority = 10`.
`::new(string $name)`, `withPrefetch(int 1..65535)`, `withRetry()`, `withDurable()`, `withMessageTtl(Duration)`, `withMaxPriority(int 0..255)` (0 stores `null`). Validation throws `InvalidConfiguration`.

`QueueSet` interface — implemented by a **backed or pure enum**:
```php
interface QueueSet {
    public function queueName(): string;         // fully qualified, prefix applied
    public function config(): QueueConfig;
    /** @return list<static> */ public static function all(): array;
    public static function fromQueueName(string $name): ?static;
}
```
Trait `QueueSetFromAttributes` implements all four by reading `#[Queues]` on the enum and `#[Queue]` on cases via reflection, cached in a static per-enum. Default queue name = `snake_case(CaseName)` (e.g. `SendEmails` → `send_emails`), matching the Rust macro.

### Attributes (`Queuey\Attributes`, all `final`, `Attribute::TARGET_*` set)
- `#[Queues(?string $prefix = null)]` on the enum. Prefix must be non-blank and not end with `.`.
- `#[Queue(?string $name = null, ?int $prefetch = null, ?bool $durable = null, ?string $messageTtl = null, ?int $maxPriority = null, ?Retry $retry = null)]` on cases.
- `#[OnQueue(QueueSet $queue, ?string $name = null, ?Retry $retry = null)]` on job classes. `name` default = FQCN.
- `Retry` (plain final class, attribute-safe: only scalars): `new Retry(int $maxAttempts = 1, string $backoff = 'none'|'fixed'|'exponential', ?string $delay = null, ?string $base = null, ?float $factor = null, ?string $max = null, ?bool $jitter = null)`; `toPolicy(): RetryPolicy`. Validate like the Rust macros: `maxAttempts >= 1`, durations `> 0`, `delay` only with fixed, `base <= max`, factor finite `> 0`.
All attribute validation errors → `InvalidConfiguration` with the offending class/case in the message.

`JobMetadata` (internal, cached): resolves a job class → `(QueueSet $queue, string $name, ?RetryPolicy $retry)` from `#[OnQueue]`. `JobMetadata::for(class-string): self`. Missing attribute → `InvalidConfiguration`.

### Jobs and handlers
```php
interface Job {
    /** JSON-encodable value; use an object/assoc array for struct-like jobs. */
    public function toPayload(): mixed;
    /** @param mixed $payload decoded JSON (assoc arrays for objects) */
    public static function fromPayload(mixed $payload): static;
}
```
Trait `PayloadFromProperties`: `toPayload()` = `(object)` of public (incl. promoted readonly) properties; `fromPayload()` = `new static(...$payload)` by constructor parameter names (throws `DecodeError` on missing/extra/typed mismatch). Good enough for scalar/array/nullable fields; anything else implements the two methods by hand.

```php
/** @template T of Job */
interface JobHandler {
    /** @return class-string<T> */ public function jobClass(): string;   // instance method: FnHandler picks its class at runtime
    /** @param T $job */ public function handle(Job $job, JobContext $ctx): void;
}
/** Optional, checked with instanceof. @template T of Job */
interface ProvidesRetryPolicy { /** @param T $job */ public function retryPolicyFor(Job $job): ?RetryPolicy; }
```
`FnHandler::for(class-string<T> $jobClass, callable(T, JobContext): void $fn): JobHandler`.

Outcomes are signalled with exceptions (`Queuey\Exception\`):
- `JobFailed` (abstract, extends `RuntimeException`) → `RetryableFailure`, `FatalFailure`, each with `::withMessage(string)` and `::because(Throwable)`.
- `JobDeferred(Duration $delay, string $reason = 'deferred')` with `::for(Duration, ?string)`.
- Any other `Throwable` escaping `handle()` counts as **retryable** with message `get_class: message`.
Library errors: `QueueyException` (interface) + `InvalidConfiguration`, `MalformedEnvelope`, `DecodeError`, `UnknownQueue`, `NoHandler`, `DuplicateHandler`, `ShutDown`, `ConsumerStopped`, `BackendException` (wrapping broker errors: `PublishReturned`, `PublishNacked`, `DelayTooLong`, `AlreadySettled`, `ConnectionLost`).

`JobContext` (final, readonly properties): `string $jobId, string $jobType, string $queue, int $attempt, int $maxAttempts, int $deferrals, int $priority, Duration $age, ?string $correlationId`; `isLastAttempt()`. Public constructor so handlers are unit-testable without a worker.

### Dead letters
`DeadLetterCause` enum `NoHandler|Decode|Fatal|Exhausted` with `reachedHandler()`.
`DeadLetter` (final, readonly properties): `Envelope $envelope, DeadLetterCause $cause, string $reason, ?int $maxAttempts`; `attempts()`.
`interface DeadLetterHook { public function onDeadLetter(DeadLetter $dead): void; }` + `FnDeadLetterHook(callable)`.

### Backend abstraction (synchronous)
```php
interface Delivery {
    public function envelope(): Envelope;
    public function ack(): void;
    public function deadLetter(string $reason): void;          // publish to q.dead (confirmed) THEN ack
    public function retry(Envelope $next, Duration $delay): void;   // hold queue (retry granularity) THEN ack
    public function defer(Envelope $next, Duration $delay): void;   // hold queue (deferred granularity) THEN ack
}
interface Backend {
    public function declare(QueueConfig ...$queues): void;
    public function publish(Envelope $envelope, ?Duration $delay = null): void;  // delay → hold queue, retry granularity
    public function defer(Envelope $envelope, Duration $delay): void;           // hold queue, deferred granularity
    /** Register a consumer; deliveries are handed to $onDelivery from inside wait(). */
    public function consume(QueueConfig $queue, callable $onDelivery): void;
    /** Block until at least one delivery was dispatched or $timeout seconds elapsed (null = no limit). */
    public function wait(?float $timeoutSeconds = null): void;
    public function cancelConsumers(): void;   // stop receiving; in-hand deliveries stay settleable
    public function close(): void;
}
```

### Producer
`new Producer(class-string<QueueSet> $queues, Backend $backend, ?LoggerInterface)` declares `all()` configs. `Producer::undeclared(...)` skips the declare (cannot defer/enqueueAfter → `UnknownQueue`).
`enqueue(Job $job, ?EnqueueOptions $opts = null): string` (job id), `enqueueAfter(Job, Duration)`, `defer(Job, Duration)`.
`EnqueueOptions::now()/after(Duration)/deferred(Duration)`, `->withCorrelationId(string)`.
Runtime guard: the job's `#[OnQueue]` queue must be a case of `$queues`, else `JobBelongsToOtherQueueSet` (extends `InvalidConfiguration`). This is PHP's stand-in for Rust's compile-time check.

### Worker
`Worker::builder(class-string<QueueSet> $queues, Backend $backend): WorkerBuilder`
Builder: `handler(JobHandler)`, `queues(QueueSet ...$only)`, `retryOverride(QueueSet, RetryPolicy)`, `jobRetryOverride(class-string, RetryPolicy)`, `onDeadLetter(DeadLetterHook)`, `jobTimeout(Duration)` (implemented with `pcntl_alarm` when ext-pcntl is loaded, otherwise `InvalidConfiguration`), `closeBackendOnShutdown(bool $close = true)` (the feature is off unless called), `logger(LoggerInterface)`, `clock(Clock)`, `build(): Worker` (validates duplicate handlers, resolves policies per spec §5 precedence, declares queues, registers consumers lazily in `run()`).
`Worker::run(): void` — ends with `ConsumerStopped` when the backend stops delivering (closed or gone). Loop: `wait(timeout ~1s)` so shutdown flags and signals are observed; stop on `handle()->shutdown()`; if `ext-pcntl` is loaded, `SIGTERM`/`SIGINT` request shutdown (`pcntl_async_signals(true)`). On stop: `cancelConsumers()`, settle what is in hand, optionally `close()`. `runFor(Duration)`, `runUntil(callable(): bool)` and `maxJobs(int)` builder option for tests/supervisors.
`WorkerHandle`: `shutdown()`, `isShutdown()`, `settleFailures(): int`, `processed(): int`.
Concurrency is 1 (synchronous PHP); prefetch still shapes broker-side buffering. Logging via PSR-3 with the same levels as Rust (deferral INFO, retry WARN, dead-letter ERROR, settle failure ERROR).

### MemoryBackend (`Queuey\Memory`)
Deterministic, no threads: queues are arrays ordered by priority (stable within a level; a new envelope goes behind everything with `priority >= its own`). Delays become `dueAtMs` on the injected `Clock`; `wait()` releases due holds (earliest `dueAtMs` first) then dispatches at most one delivery per registered consumer per call, or yields through the injected sleeper (`usleep` by default) until the timeout. Inspection: `pending(queue): list<Envelope>`, `held(queue)`, `dead(queue): list<array{envelope, reason}>`, `acked(): int` (acks: success, retry, deferral — never a dead letter), `succeeded(): int`, `retried(): int`, `settled(): int` (every settle, dead letters included). Closed → `ShutDown`, `wait()` included.

### RabbitMQ backend (`Queuey\RabbitMq`)
- `RabbitMqBackend::connect(string $url, ?RabbitMqOptions $options = null, ?LoggerInterface $logger = null)`; URL `amqp[s]://user:pass@host:port/vhost[?params]` (vhost percent-decoded; `%2f` → `/`; default vhost `/`; credentials default to `guest`/`guest` only when the URL carries no userinfo at all). TLS by building the connection through `AMQPConnectionFactory` with `AMQPConnectionConfig::setIsSecure(true)` for `amqps`. Query parameters are read as lapin reads them and applied over the options: `heartbeat` (seconds), `connection_timeout` (**milliseconds**); `channel_max` is ignored with a debug log and anything else is an `InvalidConfiguration`.
- `RabbitMqOptions` (readonly, withers): `connectionName`, `deadSuffix='.dead'`, `declareDeadLetterQueues=true`, `deferredSuffix='.deferred'`, `retryGranularity=1s`, `deferredGranularity=1s`, `?ReconnectPolicy reconnect = BackoffPolicy default`, `heartbeat=30`, `connectionTimeout=10.0`, `readWriteTimeout=...`, `publishConfirmTimeout=30.0`.
- Internals: `Topology` (pure functions: names, args as `AMQPTable`, ttl rounding — unit-tested against spec §3), `Codec` (props/headers — unit-tested against §2), `AmqpUrl` (URL + query parameters), `Connection` (slot + reconnect + remembered configs replay), `Publisher` (one confirm channel, mandatory, return/nack listeners → exceptions; one declare channel for hold queues; every abandoned channel is closed, never just dropped), `AmqpError` (what a php-amqplib failure means), `RabbitMqDelivery`.
- Consumer channel per queue with `basic_qos(prefetch, false)`. `wait()` = `$channel->wait(null, false, $timeout)` across channels (php-amqplib: one connection, iterate channels with a short per-channel timeout, or use `select` on the connection).
- Reconnect per spec §8. Errors that mean "connection gone" (`AMQPConnectionClosedException`, `AMQPIOException`, `AMQPHeartbeatMissedException`, `AMQPChannelClosedException` for the shared channels) trigger it. Errors that mean "only this consumer is gone" (`AMQPProtocolChannelException`, `AMQPBasicCancelException` — a deleted queue) resubscribe that one consumer, paced by the same `ReconnectPolicy`.
- A publish is attempted **once** (spec §4): a dead connection is repaired before anything is written, but a drop during the publish or its confirm is a `ConnectionLost`, because the broker may already hold the message. One consumer per queue per backend; a second `consume()` for the same queue is an `InvalidConfiguration`.

## Testing

- Unit: retry math (mirror the Rust test names/values), envelope fixtures byte-for-byte, topology names/args/ttl rounding table, codec props, attribute parsing + validation errors, Producer/Worker against `MemoryBackend` with `FrozenClock` (success, retry-then-success, give-up, fatal, no-handler, decode failure, unexpected throwable → retryable, deferral keeps attempt/increments deferrals/uses queue max priority, dead-letter hook per cause, hook throws → still dead-lettered, precedence of retry overrides, foreign job rejected by producer/worker, shutdown drains).
- Integration (`AMQP_URL`): declare/publish/consume/retry/defer/dead-letter/malformed/hold-queue self-expiry/`PRECONDITION_FAILED` surfaced/unroutable publish returned/unknown queue defer.
- Interop (`interop/`): PHP producer → Rust worker and Rust producer → PHP worker, covering plain success, retry-then-success, fatal → `.dead` with headers, deferral. Driven by `interop/run.sh` against `AMQP_URL`.
