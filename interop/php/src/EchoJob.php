<?php

declare(strict_types=1);

namespace Queuey\Interop;

use Queuey\Attributes\OnQueue;
use Queuey\Exception\DecodeError;
use Queuey\Job;

/**
 * The one job type on the wire, the PHP twin of the Rust `Echo` struct.
 *
 * `name: 'interop::Echo'` is the point: PHP's default `job_type` would be the
 * FQCN and Rust's would be `queuey_interop::interop::Echo`, so the name is
 * stated explicitly on both sides (INTEROP-SPEC.md §7). The PHP class is called
 * `EchoJob` rather than `Echo` only because `echo` is a reserved word that PHP
 * refuses as a class name; the wire never sees the class name.
 *
 * Both payload methods are written by hand rather than taken from
 * {@see \Queuey\PayloadFromProperties}, because the keys on the wire are Rust's
 * `snake_case` field names and the PHP properties are `camelCase`. All five keys
 * are always present — no optionals — and the casts pin the JSON types: three
 * integers, a string and a boolean, whatever a loose producer sent.
 */
#[OnQueue(InteropQueues::Jobs, name: 'interop::Echo')]
final class EchoJob implements Job
{
    /**
     * @param int    $seq          sequence number within a `produce` run; how a test follows one job
     * @param string $text         opaque, carried through untouched
     * @param int    $failTimes    fail retryably while `ctx.attempt <= failTimes`
     * @param bool   $fatal        fail fatally on the first attempt
     * @param int    $deferOnceMs  defer once, for this many milliseconds, when non-zero
     */
    public function __construct(
        public readonly int $seq,
        public readonly string $text,
        public readonly int $failTimes = 0,
        public readonly bool $fatal = false,
        public readonly int $deferOnceMs = 0,
    ) {}

    public function toPayload(): \stdClass
    {
        // An object, in the field order `interop.rs` declares, so the bytes a
        // PHP producer writes are diffable against the ones Rust writes.
        return (object) [
            'seq' => $this->seq,
            'text' => $this->text,
            'fail_times' => $this->failTimes,
            'fatal' => $this->fatal,
            'defer_once_ms' => $this->deferOnceMs,
        ];
    }

    public static function fromPayload(mixed $payload): static
    {
        if ($payload instanceof \stdClass) {
            $payload = get_object_vars($payload);
        }
        if (!\is_array($payload)) {
            throw new DecodeError(\sprintf(
                'decoding %s: payload must be a JSON object, got %s',
                self::class,
                get_debug_type($payload),
            ));
        }

        return new self(
            seq: self::int($payload, 'seq'),
            text: self::string($payload, 'text'),
            failTimes: self::int($payload, 'fail_times'),
            fatal: self::bool($payload, 'fatal'),
            deferOnceMs: self::int($payload, 'defer_once_ms'),
        );
    }

    /**
     * @param array<array-key, mixed> $payload
     */
    private static function int(array $payload, string $key): int
    {
        $value = self::field($payload, $key);
        // A `u64` that lost precision on the way in comes back as a float; Rust
        // would refuse it, so PHP does too rather than silently truncating.
        if (!\is_int($value)) {
            throw self::wrongType($key, 'an integer', $value);
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $payload
     */
    private static function string(array $payload, string $key): string
    {
        $value = self::field($payload, $key);
        if (!\is_string($value)) {
            throw self::wrongType($key, 'a string', $value);
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $payload
     */
    private static function bool(array $payload, string $key): bool
    {
        $value = self::field($payload, $key);
        if (!\is_bool($value)) {
            throw self::wrongType($key, 'a boolean', $value);
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $payload
     */
    private static function field(array $payload, string $key): mixed
    {
        if (!\array_key_exists($key, $payload)) {
            throw new DecodeError(\sprintf('decoding %s: missing field `%s`', self::class, $key));
        }

        return $payload[$key];
    }

    private static function wrongType(string $key, string $expected, mixed $value): DecodeError
    {
        return new DecodeError(\sprintf(
            'decoding %s: field `%s` must be %s, got %s',
            self::class,
            $key,
            $expected,
            get_debug_type($value),
        ));
    }
}
