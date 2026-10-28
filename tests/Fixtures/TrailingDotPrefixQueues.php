<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queues;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** A prefix that already ends in the separator the library adds. */
#[Queues(prefix: 'a.')]
enum TrailingDotPrefixQueues implements QueueSet
{
    use QueueSetFromAttributes;

    case Alpha;
}
