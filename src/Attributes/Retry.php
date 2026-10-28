<?php

declare(strict_types=1);

namespace Queuey\Attributes;

use Queuey\Backoff;
use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;
use Queuey\RetryPolicy;

/**
 * A retry policy spelled with constant expressions, so it can sit inside an
 * attribute argument: `#[Queue(retry: new Retry(maxAttempts: 5, backoff: 'exponential'))]`.
 *
 * Durations use the same grammar as {@see Duration::parse()}: `"500ms"`, `"30s"`,
 * `"2m"`, and a bare integer means seconds. Validation mirrors the Rust derive
 * macros and happens here, in the constructor, so a bad attribute fails the
 * moment it is read rather than the first time a job fails.
 */
final class Retry
{
    public const NONE = 'none';
    public const FIXED = 'fixed';
    public const EXPONENTIAL = 'exponential';

    /**
     * @param int         $maxAttempts total attempts including the first; `1` means no retries
     * @param string      $backoff     one of `none`, `fixed`, `exponential`
     * @param string|null $delay       the constant delay; only with `backoff: 'fixed'`
     * @param string|null $base        first-retry delay; only with `backoff: 'exponential'`
     * @param float|null  $factor      multiplier; only with `backoff: 'exponential'`
     * @param string|null $max         cap; only with `backoff: 'exponential'`
     * @param bool|null   $jitter      full jitter; only with `backoff: 'exponential'`
     */
    public function __construct(
        public readonly int $maxAttempts = 1,
        public readonly string $backoff = self::NONE,
        public readonly ?string $delay = null,
        public readonly ?string $base = null,
        public readonly ?float $factor = null,
        public readonly ?string $max = null,
        public readonly ?bool $jitter = null,
    ) {
        $this->validate();
    }

    /** The policy this attribute describes. */
    public function toPolicy(): RetryPolicy
    {
        return new RetryPolicy($this->maxAttempts, match ($this->backoff) {
            self::NONE => Backoff::none(),
            self::FIXED => Backoff::fixed(self::duration($this->delay ?? '', 'delay')),
            default => Backoff::exponential(
                base: self::duration($this->base ?? '1s', 'base'),
                factor: $this->factor ?? 2.0,
                max: self::duration($this->max ?? '5m', 'max'),
                jitter: $this->jitter ?? true,
            ),
        });
    }

    private function validate(): void
    {
        if ($this->maxAttempts < 1 || $this->maxAttempts > RetryPolicy::MAX_ATTEMPTS_LIMIT) {
            throw new InvalidConfiguration(\sprintf(
                '`maxAttempts` must be between 1 and %d; 1 means no retries, got %d',
                RetryPolicy::MAX_ATTEMPTS_LIMIT,
                $this->maxAttempts,
            ));
        }

        match ($this->backoff) {
            self::NONE => $this->validateNone(),
            self::FIXED => $this->validateFixed(),
            self::EXPONENTIAL => $this->validateExponential(),
            default => throw new InvalidConfiguration(\sprintf(
                'unknown backoff `%s`, expected "none", "fixed" or "exponential"',
                $this->backoff,
            )),
        };
    }

    private function validateNone(): void
    {
        self::reject($this->delay, 'delay', self::FIXED);
        self::reject($this->base, 'base', self::EXPONENTIAL);
        self::reject($this->factor, 'factor', self::EXPONENTIAL);
        self::reject($this->max, 'max', self::EXPONENTIAL);
        self::reject($this->jitter, 'jitter', self::EXPONENTIAL);
    }

    private function validateFixed(): void
    {
        self::reject($this->base, 'base', self::EXPONENTIAL);
        self::reject($this->factor, 'factor', self::EXPONENTIAL);
        self::reject($this->max, 'max', self::EXPONENTIAL);
        self::reject($this->jitter, 'jitter', self::EXPONENTIAL);

        if ($this->delay === null) {
            throw new InvalidConfiguration('`backoff: \'fixed\'` requires `delay: "..."`');
        }
        self::duration($this->delay, 'delay');
    }

    private function validateExponential(): void
    {
        self::reject($this->delay, 'delay', self::FIXED);

        if ($this->factor !== null && (!is_finite($this->factor) || $this->factor <= 0.0)) {
            throw new InvalidConfiguration('`factor` must be a finite number greater than 0');
        }

        $base = self::duration($this->base ?? '1s', 'base');
        $max = self::duration($this->max ?? '5m', 'max');
        if ($base->compare($max) > 0) {
            throw new InvalidConfiguration(\sprintf('`base` must be <= `max`, got %s and %s', (string) $base, (string) $max));
        }
    }

    /** Parse a duration key, rejecting zero the way the Rust macros do. */
    private static function duration(string $text, string $key): Duration
    {
        $duration = Duration::parse($text);
        if ($duration->isZero()) {
            throw new InvalidConfiguration(\sprintf(
                '`%s` must be greater than zero: express "no backoff" as `backoff: \'none\'` instead',
                $key,
            ));
        }

        return $duration;
    }

    private static function reject(mixed $value, string $key, string $requiredKind): void
    {
        if ($value !== null) {
            throw new InvalidConfiguration(\sprintf('`%s` is only valid with `backoff: \'%s\'`', $key, $requiredKind));
        }
    }
}
