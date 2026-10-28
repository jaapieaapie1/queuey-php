# Cross-language interop harness

`queuey-php` claims to be wire-compatible with the Rust
[queuey](../../queuey). This directory is where that claim is tested against a
real broker instead of a fixture: two binaries that speak the same protocol, and
a script that runs every scenario in both directions.

```
interop/
  rust/        the Rust half — a cargo project, `queuey-interop` (see rust/README.md)
  php/         the PHP half  — `queuey-interop`, the same five subcommands
  run.sh       the harness: every scenario, both directions, PASS/FAIL per line
```

## Running it

Needs a RabbitMQ (4.x is what this was developed against), a Rust toolchain for
the first build, and PHP 8.3+ with the package's `composer install` already done.
`jq` is used for the assertions when it is there; the script falls back to `php`
one-liners when it is not.

```sh
AMQP_URL=amqp://guest:guest@localhost:5672/%2f interop/run.sh
```

That is also the default `AMQP_URL`, so a local broker on the standard port needs
no environment at all. The Rust binary is built on first use.

```sh
interop/run.sh --only retry                 # one scenario, both directions
interop/run.sh --direction php2rust         # every scenario, one direction
interop/run.sh --only exhausted --direction rust2php
QUEUEY_INTEROP_KEEP=1 interop/run.sh        # keep the captured JSON for inspection
QUEUEY_INTEROP_NO_JQ=1 interop/run.sh       # exercise the php assertion fallback
```

Scenarios can be named by letter (`a`…`h`) or by name (`success`, `retry`,
`exhausted`, `fatal`, `deferral`, `after`, `defer`, `correlation`). The script
exits non-zero if anything fails and prints a `PASS`/`FAIL` line per scenario and
direction plus a final summary.

`RUST_LOG` and `QUEUEY_LOG` (both default to `warn` inside the harness) control
how much the two binaries say on stderr. Neither ever writes anything but JSON
lines to stdout — that is what makes the assertions possible.

## What each half is

Both binaries expose the same five subcommands with the same flags, defaults,
exit codes and — this is the point — **identical JSON event lines**:

| command | goes through | what it does |
|---|---|---|
| `produce` | the library under test | declares the topology and enqueues N `interop::Echo` jobs |
| `consume` | the library under test | runs a worker until N jobs reach a terminal state |
| `purge` | raw AMQP | deletes both interop queues |
| `drain-dead` | raw AMQP | consumes `interop.jobs.dead`, reporting the full AMQP shape |
| `inspect` | raw AMQP | passive declare of both queues |

The three raw commands deliberately do not use queuey at all. They are the
harness's independent view of the broker: what a library *actually left on the
wire*, not what it says it did. That is why a `drain-dead` run by one language
against a dead letter the other language wrote is the strongest assertion here.

The fixed protocol — queue set, job fields, handler branching, event shapes — is
documented once, in [rust/README.md](rust/README.md), and mirrored exactly by
[php/src/InteropQueues.php](php/src/InteropQueues.php),
[php/src/EchoJob.php](php/src/EchoJob.php) and
[php/queuey-interop](php/queuey-interop). Nothing in either half is a tuning
knob: a name or a value that changes on one side changes on the other in the same
commit, or the harness stops meaning anything.

The PHP half is **not** part of the package. It lives under `Queuey\Interop\`
and is autoloaded by [php/bootstrap.php](php/bootstrap.php), so test scaffolding
never ships to a user of the library.

## What it proves

Each scenario runs twice: once with a PHP producer and a Rust worker
(`php2rust`), once the other way round (`rust2php`). A scenario that behaves
differently depending on which language wrote the message is a
wire-compatibility bug.

| scenario | asserts |
|---|---|
| `a` `success` | three jobs enqueued and handled, `attempt 1`, `max_attempts 3` — the envelope, the queue arguments, the `job_type` dispatch and the payload keys all agree |
| `b` `retry` | `--fail-times 2` produces two `attempt` lines with outcome `retryable` and a `handled` on **attempt 3** — the hold queue's TTL, the `next_attempt()` transformation and the `max_attempts 3 / fixed 1s` policy agree |
| `c` `exhausted` | `--fail-times 5` gives up after three attempts with cause `Exhausted` and reason `max attempts (3) exhausted after attempt 3: flaky by request`, then `drain-dead` (run by the *other* language) checks the AMQP shape: `type`, `content_type`, `delivery_mode 2` and all five headers, `x-attempts` and `x-attempt` as **numbers** and the reasons as strings |
| `d` `fatal` | a fatal failure dies on the first attempt with reason `fatal error: fatal by request`, cause `Fatal`, `attempts 1` |
| `e` `deferral` | a handler deferral comes back at `attempt 1, deferrals 1, priority 10` — `deferred()` bumps the deferral counter, leaves the attempt alone and republishes at the queue's `max_priority` |
| `f` `after` | `enqueue_after` lands behind the backlog: `priority 0`, `deferrals 0` |
| `g` `defer` | producer-side `defer` lands at the front (`priority 10`) with `deferrals` still `0` — a producer deferral has never run |
| `h` `correlation` | `correlation_id` survives the envelope and reaches the handler's context |

Between them they cover every field of the envelope (INTEROP-SPEC §1), the AMQP
properties and headers (§2), the topology and its hold queues (§3), publishing
and both schedules (§4), the retry policy (§5), all the worker outcomes and the
dead-letter reason strings (§6), and the explicit `job_type` naming (§7).

Every scenario purges both queues first, through **both** binaries, so a run
always starts from a broker that has never heard of `interop.jobs` — whichever
library declared it last time. A mismatch in the queue arguments would surface
as a `PRECONDITION_FAILED` on the second declare rather than hiding behind a
queue that happened to exist already.
