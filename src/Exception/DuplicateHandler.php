<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** Two handlers claimed the same job type. */
final class DuplicateHandler extends \RuntimeException implements QueueyException
{
    public static function forJobType(string $jobType): self
    {
        return new self(\sprintf('handler for job type `%s` registered twice', $jobType));
    }
}
