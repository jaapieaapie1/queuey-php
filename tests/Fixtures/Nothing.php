<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\OnQueue;
use Queuey\Job;
use Queuey\PayloadFromProperties;

/** A job with no fields: its payload must encode as `{}`, never `[]`. */
#[OnQueue(TestQueues::Alpha)]
final class Nothing implements Job
{
    use PayloadFromProperties;
}
