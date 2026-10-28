<?php

declare(strict_types=1);

namespace Queuey;

/**
 * How long to wait between attempts.
 *
 * The math is pinned by INTEROP-SPEC §5 and never throws: a degenerate `factor`
 * (zero, negative, `NAN`, `INF`) collapses to a constant `base`, and a `max`
 * below `base` simply caps at `max`, because a policy may have come out of a
 * configuration file. Attribute parsing rejects those values instead.
 */
final class Backoff
{
    private function __construct(
        private readonly BackoffKind $kind,
        private readonly Duration $delay,
        private readonly Duration $base,
        private readonly float $factor,
        private readonly Duration $max,
        private readonly bool $jitter,
    ) {}

    /** Retry immediately. */
    public static function none(): self
    {
        $zero = Duration::zero();

        return new self(BackoffKind::None, $zero, $zero, 1.0, $zero, false);
    }

    /** A constant `$delay` between attempts. */
    public static function fixed(Duration $delay): self
    {
        $zero = Duration::zero();

        return new self(BackoffKind::Fixed, $delay, $zero, 1.0, $zero, false);
    }

    /**
     * An exponential curve. The defaults are the documented ones: 1s base, x2,
     * capped at 5 minutes, jittered.
     *
     * The duration defaults are spelled `null` because PHP only allows constant
     * expressions in parameter defaults.
     */
    public static function exponential(
        ?Duration $base = null,
        float $factor = 2.0,
        ?Duration $max = null,
        bool $jitter = true,
    ): self {
        return new self(
            BackoffKind::Exponential,
            Duration::zero(),
            $base ?? Duration::seconds(1),
            $factor,
            $max ?? Duration::minutes(5),
            $jitter,
        );
    }

    public function kind(): BackoffKind
    {
        return $this->kind;
    }

    /** The constant delay of a fixed curve; zero for the other kinds. */
    public function delay(): Duration
    {
        return $this->delay;
    }

    /** The first-retry delay of an exponential curve; zero for the other kinds. */
    public function base(): Duration
    {
        return $this->base;
    }

    /** The multiplier of an exponential curve; `1.0` for the other kinds. */
    public function factor(): float
    {
        return $this->factor;
    }

    /** The cap of an exponential curve; zero for the other kinds. */
    public function max(): Duration
    {
        return $this->max;
    }

    /** Whether an exponential curve draws its delay uniformly from `[0, computed]`. */
    public function jitter(): bool
    {
        return $this->jitter;
    }

    /**
     * Delay before the *next* attempt, given the 1-based attempt that just failed.
     *
     * Never throws, and for the exponential kind never exceeds `max()`.
     */
    public function delayFor(int $failedAttempt): Duration
    {
        return match ($this->kind) {
            BackoffKind::None => Duration::zero(),
            BackoffKind::Fixed => $this->delay,
            BackoffKind::Exponential => $this->jitter
                ? self::fullJitter($this->exponentialDelay($failedAttempt))
                : $this->exponentialDelay($failedAttempt),
        };
    }

    public function equals(self $other): bool
    {
        return $this->kind === $other->kind
            && $this->delay->equals($other->delay)
            && $this->base->equals($other->base)
            && ($this->factor === $other->factor || (is_nan($this->factor) && is_nan($other->factor)))
            && $this->max->equals($other->max)
            && $this->jitter === $other->jitter;
    }

    /** `min(max, base * factor^(failed_attempt - 1))`, overflow-safe. */
    private function exponentialDelay(int $failedAttempt): Duration
    {
        if ($this->base->isZero() || $this->max->isZero()) {
            return Duration::zero();
        }

        // Attempt 0 is nonsensical but must not underflow; treat it like attempt 1.
        $exponent = (float) (max($failedAttempt, 1) - 1);
        // A non-finite or non-positive factor degenerates to a constant `base`.
        $factor = (is_finite($this->factor) && $this->factor > 0.0) ? $this->factor : 1.0;

        $maxMs = $this->max->toMs();
        $ms = $this->base->toMs() * ($factor ** $exponent);
        if (!is_finite($ms)) {
            // Overflowed to +INF (or produced a NAN); the cap is the only sane answer.
            return $this->max;
        }

        $capped = min(max($ms, 0.0), (float) $maxMs);
        // `(float) $maxMs` rounds up to 2^63 for a `max` near `PHP_INT_MAX`, and
        // casting that to `int` wraps negative — which `Duration::ms()` would
        // reject, breaking the "never throws" contract. Clamp before casting.
        // `ceil`, not `round`: a sub-millisecond delay rounds **up** everywhere
        // else in this library (Rust keeps nanosecond precision), so a computed
        // 0.49ms is 1ms rather than "retry immediately".
        $rounded = $capped >= 9.2233720368547758E+18 ? $maxMs : (int) ceil($capped);

        return Duration::ms(min($rounded, $maxMs));
    }

    /** Full jitter: uniformly distributed in `[0, $delay]`. */
    private static function fullJitter(Duration $delay): Duration
    {
        if ($delay->isZero()) {
            return $delay;
        }

        return Duration::ms(random_int(0, $delay->toMs()));
    }
}
