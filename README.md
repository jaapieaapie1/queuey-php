# queuey-php

Type-safe job queues for PHP on RabbitMQ, wire-compatible with
[queuey](https://github.com/jaapieaapie1/queuey) (Rust). The same envelope, the
same queue arguments, the same retry and deferral semantics, the same
dead-letter reasons — so a PHP producer can feed a Rust worker, a Rust producer
can feed a PHP worker, and a service can be ported one process at a time instead
of all at once.

* Queues are an **enum** with `#[Queues]` / `#[Queue]`; jobs are **classes** with
  `#[OnQueue]`. A job knows its queue, and a job from another application is
  rejected the moment it is enqueued rather than landing on the wrong queue.
* Optional retries with **exponential backoff** (base, factor, cap, full jitter),
  fixed delay, or none, configurable per queue, overridable per job, and
  replaceable at runtime per worker, so the numbers can come from configuration
  instead of a release.
* A **dead-letter hook**: one callback for every job a worker gives up on,
  including the ones no handler ever saw (no handler registered, undecodable
  payload).
* Fatal vs retryable failures, dead-letter queues, graceful shutdown, PSR-3
  logging.
* **Deferral** for rate limits: a job that cannot run *yet* waits exactly as long
  as the API asks and comes back ahead of the backlog, without spending an
  attempt.
* **Automatic reconnection**: a dropped broker connection is a pause, not a
  failure. Publishes wait, consumers resubscribe, `Worker::run()` keeps going;
  policy is pluggable.
* Transport-agnostic core with an in-memory backend for tests that separates
  attempts from successes.
* Framework-free. Two runtime dependencies: `php-amqplib` and `psr/log`.

## Install

```sh
composer require queuey/queuey
```

PHP **8.3+** on a **64-bit** build (millisecond timestamps do not fit a 32-bit
`int`), with `ext-json` and `ext-mbstring`. `ext-pcntl` is optional and buys two
things: `SIGTERM`/`SIGINT` graceful shutdown and `WorkerBuilder::jobTimeout()`.

## Quickstart

```php
use Queuey\Attributes\OnQueue;
use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\Attributes\Retry;
use Queuey\Job;
use Queuey\JobContext;
use Queuey\JobHandler;
use Queuey\PayloadFromProperties;
use Queuey\Producer;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;
use Queuey\RabbitMq\RabbitMqBackend;
use Queuey\Worker;

#[Queues(prefix: 'myapp')]
enum AppQueues implements QueueSet
{
    use QueueSetFromAttributes;

    #[Queue(prefetch: 10)]
    case Emails;                                    // -> myapp.emails

    #[Queue(retry: new Retry(maxAttempts: 3, backoff: 'exponential', base: '1s', max: '2m'))]
    case Images;                                    // -> myapp.images
}

#[OnQueue(AppQueues::Emails, retry: new Retry(maxAttempts: 5, backoff: 'exponential'))]
final class SendEmail implements Job
{
    use PayloadFromProperties;

    public function __construct(
        public readonly string $to,
        public readonly string $body,
    ) {}
}

/** @implements JobHandler<SendEmail> */
final class SendEmailHandler implements JobHandler
{
    public function jobClass(): string
    {
        return SendEmail::class;
    }

    public function handle(Job $job, JobContext $ctx): void
    {
        \assert($job instanceof SendEmail);
        echo "sending to {$job->to}, attempt {$ctx->attempt} of {$ctx->maxAttempts}\n";
    }
}

$backend = RabbitMqBackend::connect('amqp://guest:guest@localhost:5672/%2f');

$producer = new Producer(AppQueues::class, $backend);
$producer->enqueue(new SendEmail('a@b.c', 'hi'));

$worker = Worker::builder(AppQueues::class, $backend)
    ->handler(new SendEmailHandler())
    // The worker leaves the shared backend open by default; this process has
    // nothing else to do with it.
    ->closeBackendOnShutdown(true)
    ->build();

$worker->run();
```

`QueueSetFromAttributes` fills in `queueName()`, `config()`, `all()` and
`fromQueueName()` by reflection. A case with no `#[Queue]` gets every default
and the `snake_case` form of its own name. `PayloadFromProperties` implements
`toPayload()` / `fromPayload()` for a job whose constructor parameters are its
payload; anything richer writes the two methods by hand.

For a handler too small to deserve a class there is `FnHandler`:

```php
use Queuey\FnHandler;

FnHandler::for(SendEmail::class, function (SendEmail $job, JobContext $ctx) use ($mailer): void {
    $mailer->send($job->to, $job->body);
});
```

## Runtime guarantees instead of compile-time

Rust's `Producer<Q, _>` and `Worker<Q, _>` are generic over the queue set, so
enqueueing another application's job does not compile. PHP has no such bound:
the queue set is a constructor argument, and the same mistake is caught at the
moment of the enqueue or at `build()`, as a
`Queuey\Exception\JobBelongsToOtherQueueSet` (a subclass of
`InvalidConfiguration`).

```php
$producer = new Producer(AppQueues::class, $backend);
$producer->enqueue(new BuildReport(1));            // BuildReport belongs to OtherQueues
```

```text
Queuey\Exception\JobBelongsToOtherQueueSet:
BuildReport is published to OtherQueues::Reports, but this producer publishes to AppQueues
```

The same check runs on the worker, for a `JobHandler` and an `FnHandler` alike,
and it runs at `build()` — before the process consumes anything — rather than on
the first delivery:

```text
Queuey\Exception\JobBelongsToOtherQueueSet:
Queuey\FnHandler handles BuildReport, which is published to OtherQueues::Reports,
but this worker consumes AppQueues
```

`jobRetryOverride()` and `retryOverride()` are checked the same way, so an
override that could never fire is an error rather than a line that quietly does
nothing.

## Deferral (rate limits, `Retry-After`)

A job that cannot run *yet* is not a job that failed. The motivating case: an
external API answers `429 Too Many Requests` with `Retry-After: 30`. Nothing
went wrong. The job has to wait exactly that long, and then run **before** the
backlog that piled up meanwhile. That is a deferral:

```php
use Queuey\Duration;
use Queuey\Exception\FatalFailure;
use Queuey\Exception\JobDeferred;

/** @implements JobHandler<CallApi> */
final class CallApiHandler implements JobHandler
{
    public function __construct(private readonly HttpClient $http) {}

    public function jobClass(): string
    {
        return CallApi::class;
    }

    public function handle(Job $job, JobContext $ctx): void
    {
        \assert($job instanceof CallApi);
        $response = $this->http->get($job->url);

        if ($response->status === 429) {
            // A handler that wants a cap enforces its own: nothing else will.
            if ($ctx->deferrals >= 5) {
                throw FatalFailure::withMessage('still rate limited after five deferrals');
            }

            $retryAfter = Duration::seconds((int) ($response->header('Retry-After') ?? 30));

            throw JobDeferred::for($retryAfter, 'rate limited');
        }
    }
}
```

* A deferral costs no attempt. `$ctx->attempt` is unchanged and the retry policy
  is never consulted: neither its backoff (the handler states the delay) nor its
  `maxAttempts` (nothing failed, so nothing is dead-lettered). The worker logs it
  at `info`, not `warning`/`error`. A job may defer itself indefinitely.
* The job comes back first. The held envelope returns at the highest priority its
  queue knows, while everything enqueued normally sits at `0`.
  `$ctx->deferrals` counts how often it happened; `$ctx->priority` is the
  priority the delivery arrived with. "First" means first among what is still
  *on* the queue: a consumer with prefetch 10 may already be holding ten backlog
  messages, and those run first.
* The priority lasts exactly one round. If the job then fails and is retried, the
  retry goes back out at priority `0`. A retry is not a deferral, and nothing is
  owed a permanent place at the front. `$ctx->deferrals` still carries.

The producer-side twin skips the first run: park the job for a delay right away,
and have it arrive at the top when the delay is up:

```php
$producer->defer(new CallApi($url), Duration::seconds(30));
```

Contrast with `enqueueAfter()`, which waits the same way but returns at priority
`0`, behind the backlog. Both are `enqueue()` with an `EnqueueOptions`
(`EnqueueOptions::now()`, `::after($delay)`, `::deferred($delay)`, each with an
optional `->withCorrelationId(...)`), so a correlation id composes with either.

### `maxPriority`

How many priority levels a queue is declared with, as a queue attribute:

| attribute | meaning |
|---|---|
| `#[Queue(maxPriority: 10)]` | ten levels; deferred jobs return at `10` (the default) |
| `#[Queue(maxPriority: 0)]` | not a priority queue; the broker ignores priorities and deferred jobs come back FIFO |

```php
#[Queues(prefix: 'myapp')]
enum AppQueues implements QueueSet
{
    use QueueSetFromAttributes;

    #[Queue(prefetch: 10, maxPriority: 10)]
    case Api;

    /** Order matters more than latency here: plain FIFO, deferral included. */
    #[Queue(name: 'img', maxPriority: 0)]
    case Images;
}
```

### RabbitMQ topology

* The main queue `q` is declared with `x-max-priority` when `maxPriority` is set;
  every publish carries the AMQP `priority` property and an `x-deferrals` header.
* Each wait, deferral and retry alike, goes into a **hold queue per delay**,
  `q.deferred.<ttlMs>` (e.g. `myapp.api.deferred.30000`), declared on demand
  right before the publish: `x-message-ttl = ttlMs`, dead-lettering back to `q`,
  and `x-expires = ttlMs * 2` so an idle hold queue deletes itself. Both
  arguments follow from the name alone, so every process declares the same queue
  identically. Delays are rounded up to a granularity (`deferredGranularity` for
  deferrals, `retryGranularity` for retries and `enqueueAfter`, both 1s by
  default), so `Retry-After: 30` and a 29.2s delay share one queue. Every message
  in a hold queue has the same TTL, so it drains strictly in order: no
  head-of-line blocking between delays, which is why retries use the same
  mechanism instead of a shared wait queue with per-message expirations.
* Hold queues are declared on their own channel, not on the one that publishes,
  so a declare the broker rejects cannot fail the publishes in flight beside it.
* A delay may not exceed **about 24.8 days** (half of RabbitMQ's maximum TTL,
  which is what keeps `x-expires` above the TTL). A longer one is refused with
  `Queuey\Exception\DelayTooLong` rather than quietly released early.
* The queue you defer onto, or retry or `enqueueAfter` on, has to have been
  declared through the same backend, the one `new Producer(...)` or
  `WorkerBuilder::build()` gave you. A `Producer::undeclared()` cannot defer or
  delay: there would be no `q` for the hold queue to dead-letter into
  (`UnknownQueue`).
* `q.dead` is untouched by this, and hold queues never get a priority.

### Upgrading an existing deployment

RabbitMQ refuses to change the arguments of a queue that already exists: a `q`
declared before this feature has no `x-max-priority`, and redeclaring it with one
closes the channel with `PRECONDITION_FAILED`. Either:

* delete the existing `q` (drain it first) and let the next declare recreate it
  with priorities, **or**
* set `#[Queue(maxPriority: 0)]` on those queues. Deferral still works, the held
  jobs come back FIFO instead of ahead of the backlog.

Queues created from now on get `x-max-priority = 10` by default. The same applies
to *every* argument: a queue whose `durable` or `messageTtl` disagrees with what
is already on the broker is a `PRECONDITION_FAILED`, surfaced as an error rather
than a hang.

## Runtime knobs on the worker

The things the attributes cannot decide for you, because they are operational
rather than structural:

```php
use Queuey\DeadLetter;
use Queuey\Duration;
use Queuey\FnDeadLetterHook;
use Queuey\RetryPolicy;

$worker = Worker::builder(AppQueues::class, $backend)
    ->handler(new SendEmailHandler())
    ->handler(new SendReceiptHandler())
    // Replace what the attributes declared, for this process: whole queue, or one job type.
    ->retryOverride(AppQueues::Emails, RetryPolicy::exponential($attemptsFromConfig))
    ->jobRetryOverride(SendReceipt::class, RetryPolicy::none())
    // Learn about every job this worker gives up on, whatever the cause.
    ->onDeadLetter(new FnDeadLetterHook(function (DeadLetter $dead): void {
        error_log(\sprintf('%s after %d attempts: %s', $dead->cause->name, $dead->attempts(), $dead->reason));
    }))
    // Interrupt a handler that overruns; needs ext-pcntl.
    ->jobTimeout(Duration::seconds(30))
    ->closeBackendOnShutdown(true)
    ->build();

$worker->retryPolicyFor(SendEmail::class);     // what this process will actually apply
```

One job at a time, rather than one kind of job, is `ProvidesRetryPolicy` — an
optional interface a handler also implements, checked with `instanceof`, and the
only hook that sees the decoded payload, so it is the only one that can give this
webhook endpoint ten attempts and its neighbour on the same queue three:

```php
use Queuey\ProvidesRetryPolicy;

/**
 * @implements JobHandler<Deliver>
 * @implements ProvidesRetryPolicy<Deliver>
 */
final class DeliverHandler implements JobHandler, ProvidesRetryPolicy
{
    /** @param array<string, RetryPolicy> $endpoints */
    public function __construct(private readonly array $endpoints) {}

    public function jobClass(): string
    {
        return Deliver::class;
    }

    public function retryPolicyFor(Job $job): ?RetryPolicy
    {
        \assert($job instanceof Deliver);

        return $this->endpoints[$job->endpoint] ?? null;
    }

    public function handle(Job $job, JobContext $ctx): void { /* ... */ }
}
```

Retry precedence, highest first:
`ProvidesRetryPolicy::retryPolicyFor()` > `jobRetryOverride()` >
`retryOverride()` > `#[OnQueue(retry: ...)]` > `#[Queue(retry: ...)]` >
`RetryPolicy::none()`. It is resolved after decode and before `handle()`, so
`$ctx->maxAttempts` already reflects it. Overrides belong to one worker; a
`Producer` never consults a policy.

The hook's `cause` is `DeadLetterCause::NoHandler`, `Decode`, `Fatal` or
`Exhausted`, and it is called *before* the delivery is settled, so a notification
is never lost for a job that is already gone. It is at-least-once, like the rest
of the system, so a hook must tolerate being called twice for the same job. It
never changes the outcome: the job is dead-lettered either way, and a throwing
hook is logged at `error` and stepped over.

`jobTimeout()` is `pcntl_alarm()` plus a `SIGALRM` handler that throws
`RetryableFailure` into whatever the handler is doing. Three caveats: it needs
**ext-pcntl** (`build()` throws `InvalidConfiguration` without it), its
resolution is whole seconds (rounded up), and a handler blocked inside a C call
that never returns to the VM is not interrupted. A handler that catches the
throw and returns normally is still reported as an overrun.

## Reconnection

A dropped connection is a pause, not a failure. The backend notices, rebuilds the
connection, replays **every** `QueueConfig` it has ever declared (a restarted
broker lost its non-durable queues), resubscribes the consumers and carries on;
`Worker::run()` does not return. A `PRECONDITION_FAILED` during that replay is
logged at `error` and the connection is kept.

Pacing is a `ReconnectPolicy`. The default is `BackoffPolicy`: unlimited
attempts, jittered exponential, base 500ms, capped at 30s, first attempt
immediate.

```php
use Queuey\RabbitMq\BackoffPolicy;
use Queuey\RabbitMq\RabbitMqOptions;

$options = (new RabbitMqOptions())
    ->withConnectionName('orders-worker')       // what `rabbitmqctl list_connections` shows
    ->withReconnect((new BackoffPolicy())->withMaxAttempts(20));

$backend = RabbitMqBackend::connect('amqp://guest:guest@localhost:5672/%2f', $options);
```

`->withReconnect(false)` turns it off: a lost connection then ends the worker
loop with an error, which is what a process supervised by systemd or Kubernetes
usually wants.

Two things reconnection deliberately does *not* do:

* The **first** `connect()` never retries. A process that cannot reach its broker
  at startup should fail rather than limp.
* A **publish is attempted once**. A connection already known to be dead is
  rebuilt before anything is written, but a drop during the publish or while
  waiting for its confirm is an error for that publish, never a silent retry: the
  broker may already hold the message, and the caller's own unacked original is
  what gets redelivered.

Only the connection is rebuilt wholesale. When the broker closes one consumer's
channel — a deleted queue, for instance — that single consumer resubscribes,
paced by the same policy.

## Interoperability with Rust queuey

This is the part worth reading twice if PHP and Rust share a broker. The wire
contract is [`docs/INTEROP-SPEC.md`](docs/INTEROP-SPEC.md); what follows is what
actually goes wrong in practice.

### Name your jobs explicitly

The `job_type` on the wire is the handler dispatch key. Rust's default is
`module_path!()::Type`, PHP's is the fully-qualified class name. **They will
never coincide by accident**, so a job consumed by both languages states its name
explicitly on at least one side:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Job)]
#[job(queue = InteropQueues::Jobs, name = "interop::Echo")]
pub struct Echo {
    pub seq: u32,
    pub text: String,
}
```

```php
// `Echo` is a PHP reserved word, so the class is `EchoJob` and the wire name is
// stated explicitly: the class name never travels.
#[OnQueue(InteropQueues::Jobs, name: 'interop::Echo')]
final class EchoJob implements Job
{
    public function __construct(
        public readonly int $seq,
        public readonly string $text,
    ) {}

    public function toPayload(): \stdClass
    {
        // snake_case on the wire, whatever the PHP property is called.
        return (object) ['seq' => $this->seq, 'text' => $this->text];
    }

    public static function fromPayload(mixed $payload): static
    {
        $fields = $payload instanceof \stdClass ? get_object_vars($payload) : (array) $payload;

        return new self(seq: (int) $fields['seq'], text: (string) $fields['text']);
    }
}
```

Reserved-word class names are the reason `name:` exists on the PHP side at all:
`Echo`, `List`, `Match`, `Print`, `Class` and friends cannot be PHP class names,
and the wire never sees the class name anyway.

### Both sides must agree on the topology

Queue **names**, the **prefix**, `maxPriority`, `durable`, `messageTtl`, and both
suffixes (`.dead`, `.deferred`) are compared by the broker, not by either
library. A disagreement is a `PRECONDITION_FAILED` on the second declare, or —
worse, because it is silent — a hold queue one side never dead-letters into. Keep
them in one place and change them in one commit. The PHP defaults match the Rust
defaults exactly: prefetch 16, `maxPriority` 10, `durable` true, suffixes `.dead`
and `.deferred`, granularities 1s.

### Payload shapes

* **Keys are whatever the other side serializes.** Rust `serde` writes struct
  fields verbatim, so a `fail_times: u32` is `"fail_times"`. PHP property names
  are usually `camelCase`, so a shared job maps them by hand in `toPayload()` /
  `fromPayload()` instead of using `PayloadFromProperties`.
* **`{}` is not `[]`.** An empty PHP array encodes as `[]`, which a Rust worker
  will not decode into a unit struct. `PayloadFromProperties` casts to `object`
  for exactly this reason; a hand-written `toPayload()` must return a `stdClass`
  or a non-empty associative array, never `[]`.
* **Integers stay integers.** The envelope is encoded with
  `JSON_PRESERVE_ZERO_FRACTION`, so a PHP `1.0` stays `1.0` and decodes into a
  Rust `f64`; a PHP `1` is `1` and decodes into an `i64`/`u32`. A field typed
  `f64` in Rust and written as an `int` in PHP fails to decode, and the reverse
  is just as true. Cast in `toPayload()` rather than hoping.
* **`u64` does not fit.** PHP integers are signed 64-bit, so a Rust `u64` above
  `PHP_INT_MAX` (9223372036854775807) arrives from `json_decode` as a `float` and
  has already lost precision. A hand-written `fromPayload()` should reject a
  non-`int` rather than truncate; for values that genuinely need the full `u64`
  range, send them as strings.

### `AMQP_STRICT_FLD_TYPES`

php-amqplib supports two AMQP field-type tables and picks between them with a
global constant. **Do not define `AMQP_STRICT_FLD_TYPES`.** Its default (the
RabbitMQ table) is what this library's queue arguments and headers are written
against; defining it switches to the strict 0-9-1 table, where 64-bit integers go
out as `L` instead of `l` and 16-bit ones as `U` instead of `s`. The hold-queue
arguments a PHP process then declares stop matching the ones a Rust process
declares, and the broker answers `PRECONDITION_FAILED` on whichever declare comes
second. It is a process-global constant, so one stray `define()` anywhere in an
application is enough.

### URL query parameters

`RabbitMqBackend::connect()` reads the same query parameters `lapin` does, spelled
and scaled the same way, and applies them over the `RabbitMqOptions` passed in —
so one `AMQP_URL` configures a PHP process and a Rust one identically:

| parameter | unit | note |
|---|---|---|
| `heartbeat` | **seconds** | |
| `connection_timeout` | **milliseconds** | as lapin reads it, not seconds |
| `channel_max` | — | parsed, logged at `debug`, otherwise ignored |

Anything else is an `InvalidConfiguration` rather than a silent no-op, because a
misspelled `heartbeat` that does nothing is discovered during an outage.

```
amqp://guest:guest@localhost:5672/%2f?heartbeat=30&connection_timeout=10000
```

The vhost is percent-decoded, so `/%2f` is the default vhost `/`. Credentials
fall back to `guest`/`guest` only when the URL carries no userinfo at all.

### Running the harness

[`interop/`](interop) is the proof rather than the claim: a Rust binary and a PHP
script speaking the same five subcommands, and a script that runs every scenario
in **both** directions against a live broker, asserting on the JSON event lines
both emit.

```sh
AMQP_URL=amqp://guest:guest@localhost:5672/%2f interop/run.sh
interop/run.sh --only retry                  # one scenario, both directions
interop/run.sh --direction php2rust          # every scenario, one direction
```

It needs a RabbitMQ, a Rust toolchain (the binary is built on first use) and
`composer install`. `jq` is used for the assertions when present and a PHP
one-liner otherwise. The eight scenarios cover success, retry-then-success,
exhausted retries with the full AMQP shape of the dead letter, fatal failure,
handler deferral, `enqueueAfter`, producer-side `defer` and correlation ids —
between them every field of the envelope, both headers and properties, the
topology, the retry policy and all four dead-letter reason strings. See
[`interop/README.md`](interop/README.md).

## MemoryBackend for tests

`Queuey\Memory\MemoryBackend` is deterministic on purpose: no threads, no timers.
Delays are deadlines on an injected `Clock`, so a `FrozenClock` decides when a
held envelope is released.

```php
use Queuey\Duration;
use Queuey\Exception\RetryableFailure;
use Queuey\FnHandler;
use Queuey\FrozenClock;
use Queuey\Memory\MemoryBackend;

$clock = new FrozenClock(1_700_000_000_000);
// Nothing sleeps: an idle wait() moves the clock instead, so a ten-second
// backoff costs the test nothing and the retry still happens when it should.
$backend = new MemoryBackend($clock, sleeper: static fn(float $seconds) => $clock->advance(Duration::seconds($seconds)));

$producer = new Producer(AppQueues::class, $backend, clock: $clock);
$producer->enqueue(new SendEmail('a@b.c', 'hi'));

$attempts = [];
$worker = Worker::builder(AppQueues::class, $backend)
    ->handler(FnHandler::for(SendEmail::class, function (SendEmail $job, JobContext $ctx) use (&$attempts): void {
        $attempts[] = $ctx->attempt;
        if ($ctx->attempt === 1) {
            throw RetryableFailure::withMessage('smtp timeout');
        }
    }))
    ->clock($clock)
    ->maxJobs(2)
    ->build();

$worker->run();

$attempts;                                 // [1, 2]
$backend->retried();                       // 1
$backend->succeeded();                     // 1
$backend->acked();                         // 2 — success, retry and deferral all ack
$backend->settled();                       // 2 — every settle, dead letters included
$backend->pending('myapp.emails');         // list<Envelope> still queued
$backend->held('myapp.emails');            // list<Envelope> waiting out a delay
$backend->dead('myapp.emails');            // list<array{envelope, reason}>
$worker->handle()->processed();            // 2
$worker->handle()->settleFailures();       // 0
```

`maxJobs()`, `runFor()` and `runUntil()` are the three ways to end a run loop
without a signal. A `JobContext` and a `DeadLetter` both have public
constructors, so a handler or a hook can be unit-tested with a plain function
call and no backend at all.

Differences from a broker worth knowing about: `prefetch` is ignored, an unsettled
delivery is simply gone rather than redelivered, and dead letters are kept in an
inspectable list instead of on a `q.dead` queue.

## Logging

Everything takes an optional PSR-3 `LoggerInterface` (`Producer`,
`WorkerBuilder::logger()`, `RabbitMqBackend::connect()`), defaulting to
`NullLogger`. The context array carries `job_id`, `job_type`, `queue` and
`attempt`, so a structured logger needs no message parsing. Levels match the Rust
implementation:

| level | events |
|---|---|
| `debug` | job enqueued, topology declared, running a job, inert retry overrides |
| `info` | worker starting/stopped, job succeeded, **job deferred**, connected, reconnected, shutdown by signal |
| `warning` | **job failed, retrying**, no handler registered, malformed body dropped, connection lost and rebuilding |
| `error` | **gave up after the last attempt**, fatal failure, decode failure, dead-letter hook threw, **failed to settle a delivery**, gave up reconnecting |

A deferral is `info` and not a warning: nothing failed. A settle failure is
`error` and counted on `WorkerHandle::settleFailures()`, because it means
duplicate processing ahead.

## Limits

* **Concurrency is one job per process.** Synchronous PHP runs one handler at a
  time; scale with more worker processes. `prefetch` still shapes how much the
  broker buffers ahead for a consumer, which matters for throughput and for how
  far ahead of the backlog a deferred job actually lands.
* **Delivery is at-least-once.** A job in flight when the connection drops is
  redelivered; publish-before-ack means a failed publish leaves the original
  unacked. Handlers and dead-letter hooks must tolerate being run twice.
* **Classic queues only.** Deferral's overtake rides `x-max-priority`, which
  quorum queues do not support.
* **No job timeout without ext-pcntl**, and even with it, not inside a C call
  that never returns to the VM.
* **Delays cap at about 24.8 days.**

## Testing

```sh
composer check                       # style + static analysis + unit tests
composer test                        # the unit tests on their own

docker run --rm -d -p 5672:5672 -p 15672:15672 rabbitmq:4-management
AMQP_URL=amqp://guest:guest@localhost:5672/%2f composer test:integration
AMQP_URL=amqp://guest:guest@localhost:5672/%2f interop/run.sh
```

Integration tests skip themselves with a notice when `AMQP_URL` is not set, so a
broker is never a prerequisite for `composer check`. Both CI jobs — the PHP
8.3/8.4 matrix and the cross-language interop run — do exactly the above; see
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Stability

1.0 means the public surface is committed to, and semantic versioning applies
from here.

* **The envelope is frozen.** A queue drained after a deploy holds messages
  written by the version before it, so fields are only ever added, never renamed,
  retyped or removed, and every addition decodes to a default when absent. This
  is the same frozen envelope the Rust implementation writes: the two versions
  move independently.
* **The wire contract is the compatibility boundary**, not the PHP API. Anything
  in [`docs/INTEROP-SPEC.md`](docs/INTEROP-SPEC.md) marked **MUST** changes only
  in a coordinated major release of both libraries.
* 1.x is **additive**: new methods on `Producer`, `WorkerBuilder` and the option
  objects are minor releases; a new required method on `Backend`, `Delivery`,
  `Job`, `JobHandler` or `DeadLetterHook` is not. A backend written against 1.0
  keeps working for the whole of 1.x.
* **Your handlers and hooks are unit-testable.** `JobContext` and `DeadLetter`
  have public constructors, so testing one is a function call rather than a
  backend and a worker.
* **PHP 8.3 is the floor**, on a 64-bit build. A raise is a minor version bump,
  never a patch.

## Roadmap

Laravel and Symfony wrappers, as separate packages: this one stays framework-free.

## Releasing

Releases are published by pushing a tag; `.github/workflows/release.yml` does the rest.

1. Move the `[Unreleased]` entries in `CHANGELOG.md` under a new heading
   `## [X.Y.Z] - YYYY-MM-DD` and commit that on `main`.
2. Tag the commit and push the tag:

   ```sh
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

The workflow rejects a tag that is not `vX.Y.Z` or does not match the newest
`CHANGELOG.md` release, runs the full CI, creates a GitHub release with that
`CHANGELOG.md` section as its notes and asks Packagist to update `queuey/queuey`,
registering it first if it is not there yet. It needs the repository secrets
`PACKAGIST_USERNAME` and `PACKAGIST_TOKEN`: the main API token for the first
release, which registers the package, and the safe token after that.

## License

MIT. See [LICENSE](LICENSE).
