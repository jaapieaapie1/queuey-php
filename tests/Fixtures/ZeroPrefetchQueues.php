<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queue;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** A prefetch the broker cannot express. */
enum ZeroPrefetchQueues implements QueueSet
{
    use QueueSetFromAttributes;

    #[Queue(prefetch: 0)]
    case Alpha;
}
