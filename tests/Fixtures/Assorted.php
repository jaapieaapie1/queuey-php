<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\OnQueue;
use Queuey\Job;
use Queuey\PayloadFromProperties;

/** Scalar, nullable, defaulted and array fields, to exercise the reflection trait. */
#[OnQueue(TestQueues::Alpha)]
final class Assorted implements Job
{
    use PayloadFromProperties;

    /**
     * @param list<string> $tags
     */
    public function __construct(
        public readonly string $to,
        public readonly ?int $count = null,
        public readonly bool $urgent = false,
        public readonly array $tags = [],
    ) {}
}
