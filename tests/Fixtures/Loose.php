<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Job;
use Queuey\PayloadFromProperties;

/**
 * Constructor parameters `ReflectionParameter::allowsNull()` is true for even
 * though nothing declared them nullable: an untyped one and a `mixed` one.
 */
final class Loose implements Job
{
    use PayloadFromProperties;

    public mixed $untyped;

    /**
     * @param mixed $untyped
     */
    public function __construct(
        public readonly mixed $anything,
        $untyped,
        public readonly ?string $maybe,
    ) {
        $this->untyped = $untyped;
    }
}
