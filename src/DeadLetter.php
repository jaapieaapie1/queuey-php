<?php

declare(strict_types=1);

namespace Queuey;

/** A job that has been given up on, as handed to a {@see DeadLetterHook}. */
final class DeadLetter
{
    /**
     * @param Envelope        $envelope    as delivered; `attempt` is the attempt that failed
     * @param string          $reason      the same text the backend writes to `x-death-reason`
     * @param int|null        $maxAttempts of the effective policy, or `null` when none could be resolved
     */
    public function __construct(
        public readonly Envelope $envelope,
        public readonly DeadLetterCause $cause,
        public readonly string $reason,
        public readonly ?int $maxAttempts = null,
    ) {}

    /** Attempts this job made, including the one that just failed. */
    public function attempts(): int
    {
        return $this->envelope->attempt;
    }
}
