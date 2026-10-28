<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** The worker or backend has been shut down. */
final class ShutDown extends \RuntimeException implements QueueyException
{
    public static function create(): self
    {
        return new self('worker is shut down');
    }
}
