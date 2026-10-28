<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\OnQueue;
use Queuey\Attributes\Retry;
use Queuey\Job;
use Queuey\PayloadFromProperties;

/** A job with its own retry policy, on a different queue of the same set. */
#[OnQueue(TestQueues::Beta, retry: new Retry(maxAttempts: 5, backoff: 'exponential', base: '250ms'))]
final class Ping implements Job
{
    use PayloadFromProperties;

    public function __construct(public readonly int $seq) {}
}
