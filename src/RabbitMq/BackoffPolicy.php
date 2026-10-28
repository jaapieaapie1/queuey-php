<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use Queuey\Backoff;
use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;

/**
 * The built-in {@see ReconnectPolicy}: retry forever with a jittered
 * exponential backoff, base 500ms, capped at 30 seconds.
 *
 * Jitter is on by default because a fleet that loses its broker must not
 * reconnect in lockstep. The first attempt after a drop is immediate.
 */
final class BackoffPolicy implements ReconnectPolicy
{
    public const DEFAULT_BASE_MS = 500;
    public const DEFAULT_MAX_MS = 30_000;

    public readonly Backoff $backoff;

    /**
     * @param int|null $maxAttempts how many attempts to make in total, or `null` for unlimited
     */
    public function __construct(
        public readonly ?int $maxAttempts = null,
        ?Backoff $backoff = null,
    ) {
        if ($maxAttempts !== null && $maxAttempts < 0) {
            throw new InvalidConfiguration(\sprintf('`maxAttempts` must not be negative, got %d', $maxAttempts));
        }

        $this->backoff = $backoff ?? Backoff::exponential(
            base: Duration::ms(self::DEFAULT_BASE_MS),
            factor: 2.0,
            max: Duration::ms(self::DEFAULT_MAX_MS),
            jitter: true,
        );
    }

    /** The documented default: unlimited, jittered, 500ms base, 30s cap. */
    public static function new(): self
    {
        return new self();
    }

    /** A copy that gives up after `$attempts` tries; `null` means unlimited. */
    public function withMaxAttempts(?int $attempts): self
    {
        return new self($attempts, $this->backoff);
    }

    /** A copy pacing its attempts with `$backoff`. */
    public function withBackoff(Backoff $backoff): self
    {
        return new self($this->maxAttempts, $backoff);
    }

    public function nextDelay(Attempt $attempt): ?Duration
    {
        if ($this->maxAttempts !== null && $attempt->failures >= $this->maxAttempts) {
            return null;
        }
        if ($attempt->failures === 0) {
            return Duration::zero();
        }

        return $this->backoff->delayFor($attempt->failures);
    }
}
