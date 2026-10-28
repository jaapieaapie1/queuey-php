<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** A backed enum works exactly like a pure one; the backing value is ignored. */
#[Queues(prefix: 'backed')]
enum BackedQueues: string implements QueueSet
{
    use QueueSetFromAttributes;

    case Emails = 'e';

    #[Queue(name: 'img')]
    case Images = 'i';
}
