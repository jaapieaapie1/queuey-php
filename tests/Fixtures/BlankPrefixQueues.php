<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queues;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** A prefix that is nothing but whitespace. */
#[Queues(prefix: '   ')]
enum BlankPrefixQueues implements QueueSet
{
    use QueueSetFromAttributes;

    case Alpha;
}
