<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Attributes\OnQueue;
use Queuey\Job;
use Queuey\PayloadFromProperties;

/** The Rust test-suite's `test::Greet`, spelled in PHP. */
#[OnQueue(TestQueues::Alpha, name: 'test::Greet')]
final class Greet implements Job
{
    use PayloadFromProperties;

    public function __construct(public readonly string $name) {}
}
