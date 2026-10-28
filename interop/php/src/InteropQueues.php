<?php

declare(strict_types=1);

namespace Queuey\Interop;

use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\Attributes\Retry;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/**
 * The queue set both halves of the harness declare, mirroring the Rust
 * `#[queues(prefix = "interop")] enum InteropQueues { Jobs }`.
 *
 * One queue, `interop.jobs`, with `prefetch = 4`, the default `max_priority`
 * (10) and the default `durable = true`; its dead-letter companion is
 * `interop.jobs.dead`. The retry policy is three attempts one second apart —
 * fixed rather than exponential so a PHP run and a Rust run take the same
 * wall-clock time and a test can assert on it.
 *
 * Nothing here is a tuning knob: every value has to match
 * `interop/rust/src/interop.rs` exactly or the declares collide with
 * `PRECONDITION_FAILED` and the cross-language assertions stop meaning anything.
 */
#[Queues(prefix: 'interop')]
enum InteropQueues implements QueueSet
{
    use QueueSetFromAttributes;

    /** The main work queue, `interop.jobs`. */
    #[Queue(prefetch: 4, retry: new Retry(maxAttempts: 3, backoff: 'fixed', delay: '1s'))]
    case Jobs;
}
