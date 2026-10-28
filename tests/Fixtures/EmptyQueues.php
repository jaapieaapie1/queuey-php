<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queues;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/**
 * A queue set with no cases at all, which names no queue and must be refused —
 * Rust's derive says "requires at least one variant".
 */
#[Queues(prefix: 'empty')]
enum EmptyQueues implements QueueSet
{
    use QueueSetFromAttributes;
}
