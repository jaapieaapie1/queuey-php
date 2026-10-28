<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queue;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** Two cases that resolve to the same broker queue. */
enum CollidingQueues implements QueueSet
{
    use QueueSetFromAttributes;

    case Alpha;

    #[Queue(name: 'alpha')]
    case Beta;
}
