<?php

declare(strict_types=1);

namespace Queuey;

/** What a {@see RetryPolicy} decided after an attempt failed. */
final class RetryDecision
{
    private function __construct(private readonly ?Duration $delay) {}

    /** Re-publish after `$delay`. */
    public static function retry(Duration $delay): self
    {
        return new self($delay);
    }

    /** Attempts exhausted; dead-letter. */
    public static function giveUp(): self
    {
        return new self(null);
    }

    public function isRetry(): bool
    {
        return $this->delay !== null;
    }

    public function isGiveUp(): bool
    {
        return $this->delay === null;
    }

    /** How long to wait, or `null` when the decision was to give up. */
    public function delay(): ?Duration
    {
        return $this->delay;
    }

    public function equals(self $other): bool
    {
        if ($this->delay === null || $other->delay === null) {
            return $this->delay === $other->delay;
        }

        return $this->delay->equals($other->delay);
    }
}
