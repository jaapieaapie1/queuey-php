# queuey-interop (Rust half)

The Rust side of the cross-language interop harness for [queuey](../../../queuey)
and its PHP port, `queuey-php`. One binary, `queuey-interop`, that a test can
drive from either language: Rust produces, PHP consumes; PHP produces, Rust
consumes; and either side can look at what the broker actually holds.

The PHP half mirrors this **exactly** — same queue names, same job name, same
field names, same handler branching, same JSON event lines. Nothing in this
crate is a tuning knob. If a name or a value changes here it changes in PHP in
the same commit, or the cross-language tests stop meaning anything.

## Building

A standalone cargo project: the empty `[workspace]` table in `Cargo.toml` keeps
it out of both repositories' workspaces, and it depends on queuey by path
(`../../../queuey/crates/queuey`), so it always builds against the working copy
next door rather than a published crate.

```sh
cargo build --release
cargo clippy -- -D warnings
```

Rust edition 2024, MSRV 1.88 (built and tested on 1.96).

## Running

```sh
export AMQP_URL=amqp://guest:guest@localhost:5672/%2f   # this is also the default
./target/release/queuey-interop <command>
```

`RUST_LOG` picks the log level (`info` by default).

**stdout is JSON lines and nothing else**: one object per line, no pretty
printing, no banner, no summary. Tracing output, error text and timeout messages
all go to stderr. A test reads stdout with a line-by-line JSON parser and never
has to filter anything out.

## The protocol

### Topology

| | |
|---|---|
| queue set | `#[queues(prefix = "interop")] enum InteropQueues { Jobs }` |
| queue | `interop.jobs` — `prefetch = 4`, `durable`, default `max_priority` (10) |
| dead queue | `interop.jobs.dead` — durable, no arguments |
| retry policy | `max_attempts = 3`, `backoff = "fixed"`, `delay = "1s"` |
| hold queues | `interop.jobs.deferred.<ttl_ms>`, created on demand by queuey, self-expiring |

### Job

`job_type` is `interop::Echo`, set explicitly with `#[job(name = ...)]` on this
side and with `name:` on the PHP side, because the two languages' defaults
(`module_path!()::Type` vs the FQCN) would never coincide — see
[INTEROP-SPEC.md §7](../../docs/INTEROP-SPEC.md).

| field | type | meaning |
|---|---|---|
| `seq` | `u32` | sequence number within a `produce` run; how a test follows one job |
| `text` | `string` | opaque, carried through untouched |
| `fail_times` | `u32` | fail retryably while `attempt <= fail_times` |
| `fatal` | `bool` | fail fatally on the first attempt |
| `defer_once_ms` | `u64` | defer once, for this long, when non-zero |

All five keys are always present in the payload JSON: no `Option`, no
`skip_serializing_if`.

### Handler semantics

Checked in this order, and the order matters (a job that is both `fatal` and
`defer_once_ms` dies rather than waiting):

1. `fatal` → `JobError::fatal_msg("fatal by request")`
2. `defer_once_ms > 0 && ctx.deferrals == 0` → `JobError::deferred_msg(defer_once_ms, "deferred by request")`
3. `ctx.attempt <= fail_times` → `JobError::retryable_msg("flaky by request")`
4. otherwise → success

## Commands

### `produce`

Declares the topology (via `Producer::new`, which is what creates
`interop.jobs` and `interop.jobs.dead`) and enqueues `--count` jobs with
`seq` running from `--start-seq`. Closes the broker connection and exits 0.

```sh
queuey-interop produce --count N \
  [--start-seq 0] [--text hello] \
  [--fail-times 0] [--fatal] [--defer-once-ms 0] \
  [--correlation-id X] [--after-ms MS | --defer-ms MS]
```

`--after-ms` uses `Producer::enqueue_after` (waits, then joins the **back** of
the queue at priority 0); `--defer-ms` uses `Producer::defer` (waits, then
arrives at the **front**, at the queue's `max_priority`). They are mutually
exclusive. With `--correlation-id` the same three schedules go through
`Producer::enqueue_with` + `EnqueueOptions`, which is the only form that takes a
correlation id.

Prints, per job:

```json
{"event":"enqueued","seq":0,"job_id":"5de37318-83d5-49bc-ae99-71b2a2b48d32"}
```

### `consume`

Builds a `Worker` with `close_backend_on_shutdown(true)` and a dead-letter hook,
runs it until `--expect` jobs have reached a **terminal** state, then shuts down
gracefully and exits 0. Terminal means "will never be seen again": a success or
a dead letter of any cause. Retries and deferrals are reported but not counted.

On timeout it still shuts down cleanly, writes a message to stderr and exits 1.
Ctrl-C stops it cleanly with exit 0.

```sh
queuey-interop consume --expect N [--timeout-secs 60]
```

Three event shapes:

```json
{"event":"handled","seq":200,"job_id":"…","attempt":3,"max_attempts":3,"deferrals":0,"priority":0,"correlation_id":"trace-abc","age_ms":2015}
{"event":"attempt","seq":300,"attempt":2,"deferrals":0,"outcome":"retryable"}
{"event":"dead","job_id":"…","job_type":"interop::Echo","cause":"Exhausted","reason":"max attempts (3) exhausted after attempt 3: flaky by request","attempts":3,"max_attempts":3,"seq":300}
```

* `handled` — one per successful run. `correlation_id` is a string or `null`.
* `attempt` — one per attempt that did **not** succeed. `outcome` is
  `"retryable"`, `"fatal"` or `"deferred"`.
* `dead` — one per dead-lettered job, from the hook, so it covers the causes no
  handler ever sees. `cause` is `"NoHandler"`, `"Decode"`, `"Fatal"` or
  `"Exhausted"`; `max_attempts` is `null` for `NoHandler`; `seq` is read out of
  `envelope.payload["seq"]` when it is there and `null` when it is not.

### `purge`

Deletes `interop.jobs` and `interop.jobs.dead` through `lapin` directly
(`if_unused = false, if_empty = false`), each on its own channel, so the harness
can start from a broker that has never heard of either. A queue that is not
there is reported, not an error. Exits 0.

```json
{"event":"purged","name":"interop.jobs","messages":0}
{"event":"missing","name":"interop.jobs"}
```

Hold queues (`interop.jobs.deferred.*`) are left alone: they carry `x-expires`
and delete themselves.

### `drain-dead`

Consumes and acks `--expect` messages off `interop.jobs.dead` through `lapin`
directly (prefetch 50, `no_ack = false`), reporting the full AMQP shape of each
— which is the point: it shows what queuey (or queuey-php) actually put on the
wire, not what the library says it did. Exits 0, or 1 on timeout.

```sh
queuey-interop drain-dead --expect N [--timeout-secs 30]
```

```json
{"event":"dead_message","job_id":"…","type":"interop::Echo","content_type":"application/json","delivery_mode":2,"priority":0,"headers":{"x-death-reason":"fatal error: fatal by request","x-original-queue":"interop.jobs","x-attempts":1,"x-attempt":1,"x-deferrals":0},"body":{"job_id":"…","job_type":"interop::Echo","queue":"interop.jobs","attempt":1,"enqueued_at_ms":1790073284453,"deferrals":0,"priority":0,"payload":{"seq":100,"text":"hello","fail_times":0,"fatal":true,"defer_once_ms":0}}}
```

`job_id` is the AMQP `message_id`, `type` is the AMQP `type` property; both are
`null` when absent (a malformed-envelope forward sets neither). The five headers
are always present as keys, `null` when the message does not carry them. `body`
is the message body parsed as JSON, or the raw bytes as a JSON string when it is
not JSON at all.

### `inspect`

Passive `queue_declare` of both queues through `lapin` — asks, never creates.
Each queue gets its own channel, because a passive declare of a missing queue is
a `404 NOT_FOUND` that closes the channel it ran on. Exits 0.

```json
{"event":"queue","name":"interop.jobs","messages":0,"consumers":0}
{"event":"missing","name":"interop.jobs.dead"}
```

## A note on JSON key order

`serde_json` is built with `preserve_order`, so every line above keeps the order
it was written in (`event` first, then the fields in the order the tables
document) and a body read back off the wire is reported in the order the
producer actually wrote it. That is what makes Rust and PHP harness output
diffable line by line, and it is how the `drain-dead` output above shows the
envelope in exactly the field order INTEROP-SPEC.md §1 pins.

The side effect is that queuey serializes *this binary's* payload objects in
struct-declaration order rather than sorted, which is what PHP emits too. Key
order is not part of the contract (§1: "not required by Rust"), so this is a
convenience for reading the output, not a claim about the wire.

## Smoke test

```sh
B=./target/release/queuey-interop

$B purge
$B consume --expect 2 --timeout-secs 30 &
$B produce --count 2
wait

$B produce --count 1 --start-seq 100 --fatal
$B consume --expect 1
$B drain-dead --expect 1
$B inspect
$B purge
```

## What is deliberately not here

* No assertions. The harness reports; the test on either side decides.
* No queuey types in the `lapin` commands and no `lapin` types in the queuey
  commands. `queuey-rabbitmq` exposes no `lapin` type at all, on purpose, so
  `purge` / `drain-dead` / `inspect` open their own connection. That separation
  is what lets them be trusted as an independent view of the broker.
