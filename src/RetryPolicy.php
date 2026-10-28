<?php

declare(strict_types=1);

namespace Queuey;

use Queuey\Exception\InvalidConfiguration;

/** Retry configuration attached to a queue, a job or a handler. */
final class RetryPolicy
{
    /** Largest `maxAttempts` the wire contract can carry (Rust's `u32`). */
    public const MAX_ATTEMPTS_LIMIT = 4294967295;

    /**
     * @param int $maxAttempts total attempts including the first; `1` means no retries
     */
    public function __construct(
        public readonly int $maxAttempts,
        public readonly Backoff $backoff,
    ) {
        if ($maxAttempts < 1 || $maxAttempts > self::MAX_ATTEMPTS_LIMIT) {
            throw new InvalidConfiguration(\sprintf(
                '`maxAttempts` must be between 1 and %d; 1 means no retries, got %d',
                self::MAX_ATTEMPTS_LIMIT,
                $maxAttempts,
            ));
        }
    }

    /** `$maxAttempts` total attempts with `$backoff` between them. */
    public static function new(int $maxAttempts, Backoff $backoff): self
    {
        return new self($maxAttempts, $backoff);
    }

    /** No retries: a single attempt, no delay. This is the default everywhere. */
    public static function none(): self
    {
        return new self(1, Backoff::none());
    }

    /** `$maxAttempts` attempts with a constant `$delay` between them. */
    public static function fixed(int $maxAttempts, Duration $delay): self
    {
        return new self($maxAttempts, Backoff::fixed($delay));
    }

    /** `$maxAttempts` attempts with the default exponential backoff. */
    public static function exponential(int $maxAttempts): self
    {
        return new self($maxAttempts, Backoff::exponential());
    }

    /** What to do after the 1-based `$failedAttempt` failed. */
    public function decide(int $failedAttempt): RetryDecision
    {
        if ($failedAttempt >= $this->maxAttempts) {
            return RetryDecision::giveUp();
        }

        return RetryDecision::retry($this->backoff->delayFor($failedAttempt));
    }

    public function equals(self $other): bool
    {
        return $this->maxAttempts === $other->maxAttempts && $this->backoff->equals($other->backoff);
    }
}
