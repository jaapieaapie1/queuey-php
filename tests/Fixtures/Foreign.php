<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\OnQueue;
use Queuey\Job;
use Queuey\PayloadFromProperties;

/** A job of another application: its queue belongs to {@see BackedQueues}. */
#[OnQueue(BackedQueues::Emails)]
final class Foreign implements Job
{
    use PayloadFromProperties;
}
