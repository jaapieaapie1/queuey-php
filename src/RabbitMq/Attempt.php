<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

/**
 * One question put to a {@see ReconnectPolicy}: may the backend try again, and
 * how long should it wait first?
 */
final class Attempt
{
    /**
     * @param int             $failures    how many attempts have already failed; `0` is the first try
     * @param \Throwable|null $error       what the last attempt failed with, or `null` on the first try
     * @param Rebuilding      $rebuilding  what is being rebuilt
     */
    public function __construct(
        public readonly int $failures,
        public readonly ?\Throwable $error,
        public readonly Rebuilding $rebuilding,
    ) {}

    /** The first attempt: nothing has failed yet. */
    public static function first(Rebuilding $rebuilding): self
    {
        return new self(0, null, $rebuilding);
    }

    /** The attempt that follows `$failures` failures, the last of which was `$error`. */
    public static function after(Rebuilding $rebuilding, int $failures, \Throwable $error): self
    {
        return new self($failures, $error, $rebuilding);
    }
}
