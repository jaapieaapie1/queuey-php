<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queue;
use Queuey\Attributes\Retry;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** A retry whose `delay` belongs to a backoff it did not pick. */
enum DelayWithExponentialQueues implements QueueSet
{
    use QueueSetFromAttributes;

    #[Queue(retry: new Retry(maxAttempts: 3, backoff: 'exponential', delay: '1s'))]
    case Alpha;
}
