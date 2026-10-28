# queuey-php: wire-compatibility spec (v1)

This document is the contract queuey-php implements. It is distilled from the Rust
reference implementation in `../queuey` (crates `queuey-core` and `queuey-rabbitmq`,
version 1.0.0). A PHP producer must be consumable by a Rust worker and vice versa,
byte for byte where the broker compares bytes (queue arguments) and semantically
everywhere else.

Sections marked **MUST** are interop-critical. Everything else is design guidance.
The user-facing summary of what this means in practice — explicit job names, the
topology both sides have to agree on, payload shapes, `AMQP_STRICT_FLD_TYPES` —
is the "Interoperability with Rust queuey" section of the [README](../README.md).

---

## 1. Envelope (JSON body) — MUST

Every message body on every queuey queue (`q`, `q.dead`, `q.deferred.<ttl>`) is one
JSON object:

```json
{
  "job_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
  "job_type": "myapp::jobs::SendEmail",
  "queue": "myapp.emails",
  "attempt": 2,
  "enqueued_at_ms": 1700000000000,
  "deferrals": 1,
  "priority": 10,
  "correlation_id": "trace-abc",
  "payload": { "to": "a@b.c" }
}
```

| field | type | notes |
|---|---|---|
| `job_id` | string, UUID (v4 on creation), canonical hyphenated lowercase | stable across retries/deferrals |
| `job_type` | string | handler dispatch key; see §7 naming |
| `queue` | string | fully qualified queue name (`prefix.name`) |
| `attempt` | uint32, **1-based** | `next_attempt()` increments |
| `enqueued_at_ms` | uint64 | unix epoch millis at first enqueue; **unchanged** by retry/defer |
| `deferrals` | uint32, default `0` when absent | times deferred; retry keeps it |
| `priority` | uint8, default `0` when absent | AMQP priority the message is published with |
| `correlation_id` | string, optional | **omit the key entirely when null** (Rust: `skip_serializing_if = None`). Never interpreted by the library |
| `payload` | any JSON value | the serialized job. Usually an object |

Decoding rules:
- Unknown fields are ignored (serde default behaviour). Missing `deferrals`/`priority`/`correlation_id` default as above. Missing any other field = malformed.
- A body that is not valid JSON or not a valid envelope is **malformed** → §6.5.

Transformations (pure functions on an envelope):
- `next_attempt()`: `attempt + 1`, `priority = 0`, everything else (incl. `deferrals`, `job_id`, `enqueued_at_ms`, `correlation_id`) unchanged.
- `deferred(priority)`: `deferrals + 1`, `priority = priority`, everything else unchanged (**attempt unchanged**).

Encoding: PHP `json_encode` with `JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION`. Emit keys in the order shown above (not required by Rust, but lets tests pin bytes).

**Payload object fidelity — MUST.** A worker re-publishes the *same* payload on retry/defer. An empty JSON object `{}` must not turn into `[]`. Therefore the Envelope keeps the payload in a form that round-trips objects (decode with `json_decode($body, false)` i.e. `stdClass` for objects, or keep raw JSON). Hand the handler an array form if desired, but re-encode from the faithful form. `Job::toPayload()` returning an empty PHP array would encode as `[]`; the provided reflection trait must cast to `object` so an empty job encodes as `{}`.

Fixtures to pin in unit tests (from the Rust test-suite; both must decode):

```json
{"job_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","job_type":"myapp::jobs::SendEmail","queue":"myapp.emails","attempt":2,"enqueued_at_ms":1700000000000,"deferrals":1,"priority":10,"payload":{"to":"a@b.c"}}
```
```json
{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"test::Greet","queue":"test.alpha","attempt":2,"enqueued_at_ms":1700000000000,"payload":{"name":"ada"}}
```
The second decodes with `deferrals = 0`, `priority = 0`, `correlation_id = null`, and re-encodes with `"deferrals":0,"priority":0` present and no `correlation_id` key.

---

## 2. AMQP message properties and headers — MUST

Every publish of an envelope (to `q`, a hold queue, or `q.dead`) sets:

| property | value |
|---|---|
| `content_type` | `application/json` |
| `delivery_mode` | `2` (persistent) |
| `message_id` | `job_id` (truncated to 255 bytes at a UTF-8 boundary; never exceeded in practice) |
| `type` | `job_type` (same truncation rule) |
| `priority` | `envelope.priority` |
| `expiration` | **never set** (the hold queue's TTL does the timing) |
| headers `x-attempt` | `attempt`, AMQP unsigned 32-bit (`i` / php-amqplib `AMQPTable::T_INT_LONG_U`) |
| headers `x-deferrals` | `deferrals`, unsigned 32-bit |

Dead-letter publishes (to `q.dead`) add to the same properties:

| header | type | value |
|---|---|---|
| `x-death-reason` | long string (`S`) | free text, see §6 for the exact strings |
| `x-original-queue` | long string | `envelope.queue` |
| `x-attempts` | unsigned 32-bit | `envelope.attempt` |

Malformed-body forwarding to `q.dead` (§6.5) sets **only**: `delivery_mode = 2`, headers `x-death-reason = "malformed envelope"` and `x-original-queue = <queue consumed from>`. No `message_id`, no `type`, no `x-attempt`. The body is forwarded verbatim.

Neither side reads `x-attempt`/`x-deferrals`/`x-death` back; the JSON body is the source of truth. RabbitMQ's own `x-death` header (added on TTL expiry) is ignored.

**Never define `AMQP_STRICT_FLD_TYPES`.** php-amqplib chooses its field-type table from that global constant; the default (RabbitMQ's table) is what the types above and the queue arguments in §3 are written against. Defining it switches to the strict 0-9-1 table, where a 64-bit integer goes out as `L` instead of `l` and a 16-bit one as `U` instead of `s` — so the hold-queue arguments a PHP process declares stop matching the ones a Rust process declares, and the second declare is answered with `PRECONDITION_FAILED`.

---

## 3. Topology — MUST

All queues live on the **default exchange** (`""`), routing key = queue name. Nothing declares an exchange. All declares: `passive=false, exclusive=false, auto_delete=false, nowait=false`.

### 3.1 Main queue `q`
- Name: `prefix + "." + name` if a prefix is set, else `name`. The separator is added by the library; a prefix ending in `.` is a configuration error.
- `durable` = `QueueConfig.durable` (default **true**).
- Arguments (only when applicable, nothing else):
  - `x-max-priority` = `max_priority` as **unsigned 8-bit** (`B` / `T_INT_SHORTSHORT_U`). Default `10`. `max_priority = 0` means *omit the argument entirely* (not a priority queue).
  - `x-message-ttl` = `message_ttl` in ms as **signed 64-bit** (`l` / `T_INT_LONGLONG`), clamped to `[1, 4294967295]`. Omitted when no TTL configured.
- Consumers: `basic_qos(prefetch_count = prefetch, global = false)`, default prefetch **16**; `basic_consume(no_ack = false, exclusive = false, no_local = false)`.

RabbitMQ compares argument *values* with integer type-class equivalence, so `10` sent as `B` by Rust and `I` by PHP would still match. Match Rust's types anyway.

### 3.2 Dead-letter queue `q.dead`
- Name: `q + dead_suffix`, default suffix `".dead"`.
- Always `durable = true`, **no arguments**.
- Declared alongside `q` unless `declare_dead_letter_queues = false`, in which case nothing is published there and dead-lettering becomes `basic_reject(requeue = false)` on the original.

### 3.3 Hold queue `q.deferred.<ttl_ms>` (retries, deferrals, `enqueue_after`)
- Name: `q + deferred_suffix + "." + ttl_ms`, default suffix `".deferred"`, e.g. `myapp.emails.deferred.30000`.
- `durable` = `q`'s durable.
- Arguments, exactly these four:
  - `x-message-ttl` = `ttl_ms` (signed 64-bit `l`)
  - `x-dead-letter-exchange` = `""` (long string `S`)
  - `x-dead-letter-routing-key` = `q` (long string)
  - `x-expires` = `min(ttl_ms * 2, 4294967295)` (signed 64-bit)
- Declared **on demand immediately before every publish into it** (idempotent; the redeclare also resets the `x-expires` idle timer, so do not cache). Declared on a **dedicated declaration channel**, never on the publishing channel (a rejected declare closes its channel).
- Never gets `x-max-priority`.
- Requires `q` to have been declared through this backend instance (the hold queue dead-letters into `q` by name). Otherwise → `UnknownQueue` error, nothing published.

### 3.4 TTL rounding — MUST
```
MAX_TTL_MS      = 4294967295          // u32::MAX
MAX_DEFERRAL_MS = 2147483647          // MAX_TTL_MS / 2 (integer division)

ttl_ms(delay, granularity):
    step = clamp(ceil_ms(granularity), 1, MAX_DEFERRAL_MS)
    ttl  = max(ceil(ceil_ms(delay) / step) * step, step)
    if ttl > MAX_DEFERRAL_MS: error DelayTooLong (never clamp, never release early)
    return ttl
```
`ceil_ms` rounds sub-millisecond durations **up**. Granularity: `retry_granularity` (default 1s) for retries and `enqueue_after`; `deferred_granularity` (default 1s) for deferrals. Examples: `29.2s → 30000`, `1.001s → 2000`, `5ms → 1000`, `0 → 1000`; `30 days → error`.

At `declare` time, validate that the worst-case hold-queue name `q + suffix + "." + MAX_DEFERRAL_MS` fits in 255 bytes; refuse otherwise.

### 3.5 Upgrading note
RabbitMQ refuses to change arguments of an existing queue (`PRECONDITION_FAILED`, closes the channel). Surface that as an error, never hang.

---

## 4. Publishing — MUST

- `basic_publish(exchange = "", routing_key = queue, mandatory = true, immediate = false)`.
- **Publisher confirms** on every publishing channel (`confirm.select`). A publish is complete only when the broker `basic.ack`s it. A `basic.return` (unroutable: nobody declared the queue) or `basic.nack` is an **error** for that publish.
- **One publish in flight per channel.** (In Rust this exists because returns cannot be attributed otherwise; in synchronous PHP it falls out naturally, but never pipeline publishes on one confirm channel.)
- **One attempt per publish.** A connection already known to be dead is rebuilt before the publish, but a connection lost during a publish or while waiting for its confirm is an error for that publish and never a silent retry: the broker may already hold the message, and the caller's own unacked original is what gets redelivered.
- Fresh enqueue: `attempt = 1, deferrals = 0, priority = 0`, publish to `q`.
- `enqueue_after(delay)`: same envelope, published into the hold queue for `ttl_ms(delay, retry_granularity)`; returns at priority 0.
- `defer(delay)` (producer-side): `priority = queue.max_priority ?? 0`, `deferrals = 0`, published into the hold queue for `ttl_ms(delay, deferred_granularity)`.

---

## 5. Retry policy — MUST (semantics)

```
RetryPolicy { max_attempts: uint32 >= 1, backoff: Backoff }
Backoff = None | Fixed(delay) | Exponential { base, factor: float, max, jitter: bool }
```
Defaults: `RetryPolicy::none()` = `{ max_attempts: 1, backoff: None }` (no retries). `Backoff::exponential()` defaults: `base = 1s, factor = 2.0, max = 5m, jitter = true`.

```
decide(failed_attempt):
    if failed_attempt >= max_attempts: GiveUp
    else Retry { delay: backoff.delay_for(failed_attempt) }

delay_for(attempt):
    None        -> 0
    Fixed(d)    -> d
    Exponential -> if base == 0 or max == 0: 0
                   f = factor if finite and > 0 else 1.0
                   secs = base_secs * f ** (max(attempt,1) - 1)   // saturate: non-finite -> max
                   d = clamp(secs, 0, max_secs)
                   jitter ? uniform_random(0 .. d) : d          // "full jitter"
```

Precedence, highest first (resolved per delivery, after decode, before `handle`):
1. `JobHandler::setRetryPolicy($job)` (sees the payload)
2. `WorkerBuilder::jobRetryOverride(JobClass, policy)`
3. `WorkerBuilder::retryOverride(queue, policy)`
4. the job's own `retry` (from its `#[OnQueue(retry: ...)]`)
5. the queue's `retry` (from `#[Queue(retry: ...)]`)
6. `RetryPolicy::none()`

Overrides live in one worker process; a producer never consults a policy. `JobContext.max_attempts` reflects the resolved policy.

---

## 6. Worker dispatch — MUST (semantics and dead-letter reason strings)

For every delivery, with envelope `e`:

1. **No handler** for `e.job_type` → dead-letter with reason
   `` no handler for job type `<job_type>` `` (backticks included). Hook cause `NoHandler`, `max_attempts = null`.
2. **Decode failure** (payload does not fit the job class) → dead-letter, reason `decode error: <message>`. Cause `Decode`.
3. Build `JobContext { job_id, job_type, queue, attempt, max_attempts, deferrals, priority, age = now_ms - enqueued_at_ms (saturating at 0), correlation_id }` and call `handle`.
4. Outcomes:
   - **Success** → `ack`.
   - **Fatal** → dead-letter, reason `fatal error: <message>`. Cause `Fatal`.
   - **Deferred { delay, reason }** → publish `e.deferred(priority = queue.max_priority ?? 0)` into the hold queue (`deferred_granularity`), then `ack`. Attempt unchanged, policy not consulted, log at INFO. No built-in deferral cap.
   - **Retryable** (includes any unexpected `Throwable` from the handler and a job timeout) → `policy.decide(e.attempt)`:
     - `Retry { delay }` → publish `e.next_attempt()` into the hold queue (`retry_granularity`), then `ack`.
     - `GiveUp` → dead-letter, reason
       `max attempts (<max_attempts>) exhausted after attempt <attempt>: <message>`. Cause `Exhausted`.

**Publish before ack, always.** `retry`, `defer` and `dead_letter` publish (confirmed) first and `basic_ack` the original only afterwards. A failed publish leaves the original unacked (redelivered later: at-least-once). A failed settle is logged at ERROR and counted (`settle_failures`), never thrown out of the loop.

**Dead-letter hook** (`onDeadLetter(DeadLetter { envelope, cause, reason, max_attempts })`) is invoked **before** the delivery is settled, for all four causes. A throwing hook is logged at ERROR and dead-lettering proceeds. Hooks must tolerate duplicate invocation.

### 6.5 Malformed bodies
A delivery whose body is not a valid envelope: if `declare_dead_letter_queues` → forward the raw body to `q.dead` with the malformed properties (§2), then `ack`; if that publish fails, or DLQs are disabled → `basic_reject(requeue = false)`. Log at WARN.

### 6.6 Consumer tags
`queuey-php.<queue truncated to 160 bytes>.<hex nanos>.<seq>`. (Rust uses `queuey.`; the `-php` lets an operator tell them apart.)

---

## 7. Job naming — interop requirement

Rust's default `job_type` is `module_path!()::Type`, e.g. `myapp::jobs::SendEmail`; PHP's default is the FQCN, e.g. `App\Jobs\SendEmail`. They will never coincide by accident. **For cross-language jobs, set the name explicitly on at least one side** so both agree:

```rust
#[job(queue = AppQueues::Emails, name = "myapp::jobs::SendEmail")]
```
```php
#[OnQueue(AppQueues::Emails, name: 'myapp::jobs::SendEmail')]
```
A job name that is a PHP reserved word (`Echo`, `List`, `Match`, ...) cannot be a PHP class
name; `name:` is how the class name and the wire `job_type` are kept apart (the interop harness
uses `final class EchoJob` with `name: 'interop::Echo'`).

Any PHP process that speaks a stdout-based protocol (like the interop CLI) must route PHP
diagnostics away from stdout (`ini_set('display_errors', 'stderr')`), or a warning corrupts the stream.

Queue names, prefixes, `max_priority`, `durable`, `message_ttl` and both suffixes must also agree between the two applications, or declares hit `PRECONDITION_FAILED` and hold queues strand messages. This is spelled out for users in the README's "Interoperability with Rust queuey" section.

---

## 8. Reconnection (guidance, semantics from Rust)

- A dropped connection is a pause, not a failure: publishes wait, consumers resubscribe, `Worker::run()` keeps going.
- Single-flight reconnect paced by a `ReconnectPolicy` (`nextDelay(Attempt): ?Duration`; `null` = give up). Default `BackoffPolicy`: unlimited attempts, jittered exponential, base 500ms, cap 30s.
- On reconnect, **replay every `QueueConfig` ever passed to `declare`** on the new connection (a restarted broker lost non-durable queues and `q.dead`s). A `PRECONDITION_FAILED` during replay is logged at ERROR, connection kept.
- `close()` is final: mark closing first so consumers stop instead of reconnecting.
- The first `connect()` does not retry: a process that cannot reach its broker at startup should fail.
- In-flight jobs are redelivered by the broker after a drop; their late settle fails and is counted. Reconnect policy `null`/disabled → the worker loop returns an error instead.

---

## 9. Numbers to pin in tests

| constant | value |
|---|---|
| default prefetch | 16 |
| default `max_priority` | 10 |
| default `durable` | true |
| default retry | `max_attempts 1`, no backoff |
| exponential defaults | base 1s, factor 2.0, max 300s, jitter on |
| `MAX_TTL_MS` | 4294967295 |
| `MAX_DEFERRAL_MS` | 2147483647 |
| default granularities | 1000 ms (both) |
| default suffixes | `.dead`, `.deferred` |
| default reconnect | base 500ms, cap 30s, unlimited, jitter |
| AMQP short-string max | 255 bytes |
| consumer-tag queue truncation | 160 bytes |
