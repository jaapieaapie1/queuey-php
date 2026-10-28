<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\Attributes\Retry;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** The queue set the rest of the fixtures live on. */
#[Queues(prefix: 'test')]
enum TestQueues implements QueueSet
{
    use QueueSetFromAttributes;

    /** Every default, and the snake_case name `test.alpha`. */
    case Alpha;

    /** An explicit name wins over the snake_case form. */
    #[Queue(name: 'beta-queue', prefetch: 4, durable: false, messageTtl: '30s', maxPriority: 0)]
    case Beta;

    /** The snake_case form of a multi-word case: `test.send_emails`. */
    #[Queue(retry: new Retry(maxAttempts: 3, backoff: 'fixed', delay: '2s'))]
    case SendEmails;

    /** A capital run splits before its last capital: `test.http_calls`. */
    case HTTPCalls;
}
