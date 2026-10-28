<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A queue name that the backend never declared was used. */
final class UnknownQueue extends \RuntimeException implements QueueyException
{
    public static function named(string $queue): self
    {
        return new self(\sprintf('unknown queue `%s`', $queue));
    }
}
