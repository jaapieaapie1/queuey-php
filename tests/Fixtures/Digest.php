<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\OnQueue;
use Queuey\Job;
use Queuey\PayloadFromProperties;

/** A job with no retry of its own, on a queue that has one: `test.send_emails`. */
#[OnQueue(TestQueues::SendEmails)]
final class Digest implements Job
{
    use PayloadFromProperties;

    public function __construct(public readonly string $to = 'all') {}
}
