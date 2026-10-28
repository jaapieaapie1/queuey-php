<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** No handler is registered for a job type. */
final class NoHandler extends \RuntimeException implements QueueyException
{
    public static function forJobType(string $jobType): self
    {
        return new self(\sprintf('no handler registered for job type `%s`', $jobType));
    }
}
