<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\QueueSet;
use Queuey\QueueSetFromAttributes;

/** A `messageTtl` no broker can store in the unsigned 32-bit `x-message-ttl`. */
#[Queues(prefix: 'huge')]
enum HugeTtlQueues implements QueueSet
{
    use QueueSetFromAttributes;

    #[Queue(messageTtl: '60d')]
    case Alpha;
}
