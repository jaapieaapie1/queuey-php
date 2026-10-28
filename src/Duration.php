<?php

declare(strict_types=1);

namespace Queuey;

use Queuey\Exception\InvalidConfiguration;

/**
 * A non-negative span of time, stored as whole milliseconds.
 *
 * Milliseconds are the resolution the wire contract works in (hold-queue TTLs,
 * `enqueued_at_ms`), so anything finer is rounded **up**: nothing is ever
 * released early because of a rounding decision.
 */
final class Duration
{
    private const MS_PER_SECOND = 1_000;
    private const MS_PER_MINUTE = 60 * self::MS_PER_SECOND;
    private const MS_PER_HOUR = 60 * self::MS_PER_MINUTE;
    private const MS_PER_DAY = 24 * self::MS_PER_HOUR;

    private function __construct(private readonly int $ms) {}

    /** Exactly `$ms` milliseconds. */
    public static function ms(int $ms): self
    {
        if ($ms < 0) {
            throw new InvalidConfiguration(\sprintf('duration must not be negative, got %d ms', $ms));
        }

        return new self($ms);
    }

    /** `$seconds` seconds; a fractional value is rounded up to the next millisecond. */
    public static function seconds(int|float $seconds): self
    {
        return self::fromUnit($seconds, self::MS_PER_SECOND, 'seconds');
    }

    public static function minutes(int|float $minutes): self
    {
        return self::fromUnit($minutes, self::MS_PER_MINUTE, 'minutes');
    }

    public static function hours(int|float $hours): self
    {
        return self::fromUnit($hours, self::MS_PER_HOUR, 'hours');
    }

    public static function days(int|float $days): self
    {
        return self::fromUnit($days, self::MS_PER_DAY, 'days');
    }

    public static function zero(): self
    {
        return new self(0);
    }

    /**
     * Parse the humantime-ish grammar shared with the Rust macros.
     *
     * `<integer><unit>` where the unit is one of `ms`, `s`, `m`, `h`, `d`.
     * Whitespace around the value and between value and unit is ignored, and a
     * bare integer means seconds. `"500ms"`, `"1s"`, `"2m"`, `"1h"`, `"7d"`, `"30"`.
     */
    public static function parse(string $text): self
    {
        $trimmed = self::trimUnicode($text);
        $digits = '';
        $length = \strlen($trimmed);
        for ($i = 0; $i < $length; ++$i) {
            $char = $trimmed[$i];
            if ($char < '0' || $char > '9') {
                break;
            }
            $digits .= $char;
        }

        if ($digits === '') {
            throw self::invalid($text);
        }

        $unit = self::trimUnicode(substr($trimmed, \strlen($digits)));
        $factor = match ($unit) {
            '', 's' => self::MS_PER_SECOND,
            'ms' => 1,
            'm' => self::MS_PER_MINUTE,
            'h' => self::MS_PER_HOUR,
            'd' => self::MS_PER_DAY,
            default => throw self::invalid($text),
        };

        $value = ltrim($digits, '0');
        if ($value === '') {
            return new self(0);
        }
        $limit = (string) \PHP_INT_MAX;
        if (\strlen($value) > \strlen($limit) || (\strlen($value) === \strlen($limit) && strcmp($value, $limit) > 0)) {
            throw self::outOfRange($text);
        }

        $number = (int) $value;
        if ($number > intdiv(\PHP_INT_MAX, $factor)) {
            throw self::outOfRange($text);
        }

        return new self($number * $factor);
    }

    public function toMs(): int
    {
        return $this->ms;
    }

    public function toSeconds(): float
    {
        return $this->ms / 1000;
    }

    public function isZero(): bool
    {
        return $this->ms === 0;
    }

    public function equals(self $other): bool
    {
        return $this->ms === $other->ms;
    }

    /** `-1`, `0` or `1` as this duration is shorter than, equal to or longer than `$other`. */
    public function compare(self $other): int
    {
        return $this->ms <=> $other->ms;
    }

    /** The shorter of the two durations. */
    public function min(self $other): self
    {
        return $this->ms <= $other->ms ? $this : $other;
    }

    /** The longer of the two durations. */
    public function max(self $other): self
    {
        return $this->ms >= $other->ms ? $this : $other;
    }

    /**
     * The two durations added together.
     *
     * @throws InvalidConfiguration when the sum overflows a millisecond count
     */
    public function plus(self $other): self
    {
        if ($other->ms > \PHP_INT_MAX - $this->ms) {
            throw new InvalidConfiguration(\sprintf(
                'duration %dms + %dms does not fit in a millisecond count',
                $this->ms,
                $other->ms,
            ));
        }

        return new self($this->ms + $other->ms);
    }

    public function __toString(): string
    {
        return $this->ms . 'ms';
    }

    private static function fromUnit(int|float $value, int $factor, string $unit): self
    {
        if (\is_float($value) && !is_finite($value)) {
            throw new InvalidConfiguration(\sprintf('duration in %s must be a finite number', $unit));
        }
        if ($value < 0) {
            throw new InvalidConfiguration(\sprintf('duration must not be negative, got %s %s', (string) $value, $unit));
        }

        $ms = ceil($value * $factor);
        // `(float) PHP_INT_MAX` rounds *up* to 2^63, so `$ms > (float) PHP_INT_MAX`
        // lets the boundary through and `(int) $ms` then wraps negative. Compare
        // against 2^63 itself, and spell it as a negated `<` so a NAN — which is
        // false for every comparison — lands in the error branch too.
        if (!($ms < 9223372036854775808.0)) {
            throw new InvalidConfiguration(\sprintf('duration of %s %s does not fit in a millisecond count', (string) $value, $unit));
        }

        return new self((int) $ms);
    }

    /**
     * Trim like Rust's `str::trim()`: every `White_Space` character, not just the
     * ASCII five, so a non-breaking space around a duration literal is ignored
     * on both sides of the port.
     */
    private static function trimUnicode(string $text): string
    {
        // PHP's `u` modifier turns on PCRE_UCP, so `\s` is the Unicode class.
        return preg_replace('/^\s+|\s+$/u', '', $text) ?? trim($text);
    }

    private static function invalid(string $input): InvalidConfiguration
    {
        return new InvalidConfiguration(\sprintf(
            'invalid duration `%s`: expected an integer with an optional unit (`ms`, `s`, `m`, `h`, `d`), for example "500ms", "30s" or "2m"',
            $input,
        ));
    }

    private static function outOfRange(string $input): InvalidConfiguration
    {
        return new InvalidConfiguration(\sprintf('duration `%s` does not fit in a millisecond count', $input));
    }
}
