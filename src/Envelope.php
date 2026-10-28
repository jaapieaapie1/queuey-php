<?php

declare(strict_types=1);

namespace Queuey;

use Queuey\Exception\DecodeError;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\MalformedEnvelope;
use Queuey\Internal\JobMetadata;
use Queuey\Internal\Json;

/**
 * The wire format every job travels in; the JSON body of every queuey message.
 *
 * This is the one type that cannot change compatibly once messages exist: a
 * queue drained after a deploy holds envelopes written by the version before it.
 * See INTEROP-SPEC §1 for the field-by-field contract.
 */
final class Envelope
{
    /**
     * Largest value the `attempt` and `deferrals` fields accept (`u32::MAX`).
     *
     * `enqueued_at_ms` is a `u64` on the Rust side and is bounded by
     * `PHP_INT_MAX` here instead. That, and the millisecond arithmetic
     * throughout, assume a **64-bit PHP build** (`PHP_INT_SIZE >= 8`); a 32-bit
     * build cannot represent a unix millisecond timestamp at all.
     */
    public const MAX_U32 = 4294967295;

    private const UUID_PATTERN = '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i';

    /**
     * @param string $jobId        canonical hyphenated lowercase UUID, stable across retries and deferrals
     * @param string $jobType      handler dispatch key
     * @param string $queue        fully qualified broker queue name
     * @param int    $attempt      1-based; the first delivery is attempt 1
     * @param int    $enqueuedAtMs unix epoch milliseconds of the *first* enqueue
     * @param int    $deferrals    how often the job deferred itself; independent of `$attempt`
     * @param int    $priority     AMQP priority (0..255) this envelope is published with
     * @param mixed  $payload      the serialized job, in object-faithful form
     */
    /**
     * The verbatim `"payload"` bytes this envelope was parsed from, when it came
     * off the wire.
     *
     * `json_decode` hands integers above `PHP_INT_MAX` to PHP as floats — a Rust
     * `u64` payload field reaches a handler as `1.2345678901234567e+19`, which
     * is a PHP limitation the library cannot undo. What it *can* guarantee is
     * that the bytes are never altered: INTEROP-SPEC §1 requires a worker to
     * re-publish the **same** payload on retry or deferral, so {@see toJson()}
     * splices the original span back in instead of re-encoding `$payload`.
     * Envelopes built by {@see new()} or {@see raw()} have no such span and are
     * encoded from `$payload` as usual.
     */
    private ?string $payloadJson = null;

    public function __construct(
        public readonly string $jobId,
        public readonly string $jobType,
        public readonly string $queue,
        public readonly int $attempt,
        public readonly int $enqueuedAtMs,
        public readonly int $deferrals = 0,
        public readonly int $priority = 0,
        public readonly ?string $correlationId = null,
        public readonly mixed $payload = null,
    ) {
        if (preg_match(self::UUID_PATTERN, $jobId) !== 1) {
            throw new InvalidConfiguration(\sprintf('job_id `%s` is not a UUID', $jobId));
        }
        self::assertRange($attempt, 1, self::MAX_U32, 'attempt');
        self::assertRange($enqueuedAtMs, 0, \PHP_INT_MAX, 'enqueued_at_ms');
        self::assertRange($deferrals, 0, self::MAX_U32, 'deferrals');
        self::assertRange($priority, 0, 255, 'priority');
    }

    /**
     * A first-attempt envelope for `$job`, using its `#[OnQueue]` metadata.
     *
     * @throws InvalidConfiguration when the job class carries no usable `#[OnQueue]`
     * @throws DecodeError          when `toPayload()` is not JSON-encodable
     */
    public static function new(Job $job, ?Clock $clock = null): self
    {
        $meta = JobMetadata::for($job::class);

        return self::raw($meta->name, $meta->queue->queueName(), $job->toPayload(), $clock);
    }

    /**
     * A first-attempt envelope built from parts, for backends and tests that
     * cannot name the job class.
     *
     * @throws DecodeError when `$payload` is not JSON-encodable
     */
    public static function raw(string $jobType, string $queue, mixed $payload, ?Clock $clock = null): self
    {
        return new self(
            jobId: self::uuidV4(),
            jobType: $jobType,
            queue: $queue,
            attempt: 1,
            enqueuedAtMs: ($clock ?? new SystemClock())->nowMs(),
            deferrals: 0,
            priority: 0,
            correlationId: null,
            payload: self::normalizePayload($payload),
        );
    }

    /** A copy carrying `$correlationId`, which the library never interprets. */
    public function withCorrelationId(string $correlationId): self
    {
        return $this->copy(correlationId: $correlationId);
    }

    /**
     * A copy for the next attempt: `attempt + 1`, `priority` back to `0`.
     *
     * `deferrals` rides along untouched; a retry is scheduled like any other
     * failed attempt and must not jump the backlog.
     */
    public function nextAttempt(): self
    {
        return $this->copy(attempt: $this->attempt + 1, priority: 0);
    }

    /**
     * A copy for a deferral: `deferrals + 1` and `priority` set. `attempt` is
     * unchanged, because a deferral is not a failed attempt.
     */
    public function deferred(int $priority): self
    {
        return $this->copy(deferrals: $this->deferrals + 1, priority: $priority);
    }

    /**
     * The same envelope at `$priority`, with every counter left alone.
     *
     * What a producer-side deferral needs: the job jumps its queue without ever
     * having run, so `deferrals` must stay where it is (INTEROP-SPEC §1).
     */
    public function withPriority(int $priority): self
    {
        return $this->copy(priority: $priority);
    }

    /** Time since the job was first enqueued; never negative. */
    public function age(Clock $clock): Duration
    {
        return Duration::ms(max(0, $clock->nowMs() - $this->enqueuedAtMs));
    }

    /**
     * Decode the payload into `$jobClass`. Does not check `job_type`.
     *
     * Note that PHP hands integers larger than `PHP_INT_MAX` to the job as
     * floats; the wire bytes are unaffected (see `$payloadJson`).
     *
     * @template J of Job
     *
     * @param class-string<J> $jobClass
     *
     * @return J
     *
     * @throws DecodeError
     */
    public function decode(string $jobClass): Job
    {
        return $jobClass::fromPayload(Json::toArray($this->payload));
    }

    /** The JSON message body, with the key order INTEROP-SPEC §1 pins. */
    public function toJson(): string
    {
        $fields = [
            'job_id' => $this->jobId,
            'job_type' => $this->jobType,
            'queue' => $this->queue,
            'attempt' => $this->attempt,
            'enqueued_at_ms' => $this->enqueuedAtMs,
            'deferrals' => $this->deferrals,
            'priority' => $this->priority,
        ];
        if ($this->correlationId !== null) {
            $fields['correlation_id'] = $this->correlationId;
        }

        $raw = $this->payloadJson;
        if ($raw === null) {
            $fields['payload'] = $this->payload;
        }

        try {
            $json = Json::encode($fields);
        } catch (\JsonException $e) {
            throw new MalformedEnvelope(\sprintf('envelope is not encodable: %s', $e->getMessage()), 0, $e);
        }

        if ($raw === null) {
            return $json;
        }

        // `$fields` is never empty, so `$json` always ends in the closing brace.
        return substr($json, 0, -1) . ',"payload":' . $raw . '}';
    }

    /**
     * Parse a message body. Unknown fields are ignored; `deferrals`, `priority`
     * and `correlation_id` default when absent.
     *
     * @throws MalformedEnvelope
     */
    public static function fromJson(string $json): self
    {
        try {
            $decoded = Json::decode($json);
        } catch (\JsonException $e) {
            throw new MalformedEnvelope(\sprintf('envelope is not valid JSON: %s', $e->getMessage()), 0, $e);
        }

        if (!$decoded instanceof \stdClass) {
            throw new MalformedEnvelope('envelope is not a JSON object');
        }

        $fields = get_object_vars($decoded);
        if (!\array_key_exists('payload', $fields)) {
            throw new MalformedEnvelope('envelope is missing the field `payload`');
        }

        try {
            $envelope = new self(
                jobId: strtolower(self::requireString($fields, 'job_id')),
                jobType: self::requireString($fields, 'job_type'),
                queue: self::requireString($fields, 'queue'),
                attempt: self::requireInt($fields, 'attempt'),
                enqueuedAtMs: self::requireInt($fields, 'enqueued_at_ms'),
                deferrals: self::optionalInt($fields, 'deferrals'),
                priority: self::optionalInt($fields, 'priority'),
                correlationId: self::optionalString($fields, 'correlation_id'),
                payload: $fields['payload'],
            );
        } catch (InvalidConfiguration $e) {
            throw new MalformedEnvelope($e->getMessage(), 0, $e);
        }

        // Keep the bytes, not just the decoded value: see `$payloadJson`.
        $envelope->payloadJson = self::rawPayloadWorthKeeping($json, $envelope->payload);

        return $envelope;
    }

    private function copy(
        ?int $attempt = null,
        ?int $deferrals = null,
        ?int $priority = null,
        ?string $correlationId = null,
    ): self {
        $copy = new self(
            jobId: $this->jobId,
            jobType: $this->jobType,
            queue: $this->queue,
            attempt: $attempt ?? $this->attempt,
            enqueuedAtMs: $this->enqueuedAtMs,
            deferrals: $deferrals ?? $this->deferrals,
            priority: $priority ?? $this->priority,
            correlationId: $correlationId ?? $this->correlationId,
            payload: $this->payload,
        );
        // A retry or a deferral re-publishes the payload it was handed, verbatim.
        $copy->payloadJson = $this->payloadJson;

        return $copy;
    }

    /**
     * The verbatim `payload` span of `$json`, or `null` when re-encoding the
     * decoded value reproduces it exactly.
     *
     * Re-encoding is faithful for everything PHP can represent, so almost every
     * envelope keeps nothing; only a payload PHP cannot round-trip (an integer
     * beyond `PHP_INT_MAX`) or one spelled differently from this library's
     * encoder (escaped slashes, `\uXXXX` escapes, whitespace) carries its
     * original bytes along.
     */
    private static function rawPayloadWorthKeeping(string $json, mixed $payload): ?string
    {
        $raw = Json::rawTopLevelValue($json, 'payload');
        if ($raw === null) {
            return null;
        }

        try {
            return Json::encode($payload) === $raw ? null : $raw;
        } catch (\JsonException) {
            return $raw;
        }
    }

    private static function normalizePayload(mixed $payload): mixed
    {
        try {
            return Json::normalize($payload);
        } catch (\JsonException $e) {
            throw new DecodeError(\sprintf('payload is not JSON-encodable: %s', $e->getMessage()), 0, $e);
        }
    }

    /** RFC 4122 version 4, from `random_bytes`. */
    private static function uuidV4(): string
    {
        $bytes = random_bytes(16);
        $bytes[6] = \chr((\ord($bytes[6]) & 0x0F) | 0x40);
        $bytes[8] = \chr((\ord($bytes[8]) & 0x3F) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }

    /**
     * @param array<array-key, mixed> $fields
     */
    private static function requireString(array $fields, string $key): string
    {
        if (!\array_key_exists($key, $fields)) {
            throw new MalformedEnvelope(\sprintf('envelope is missing the field `%s`', $key));
        }
        if (!\is_string($fields[$key])) {
            throw new MalformedEnvelope(\sprintf('envelope field `%s` must be a string', $key));
        }

        return $fields[$key];
    }

    /**
     * @param array<array-key, mixed> $fields
     */
    private static function optionalString(array $fields, string $key): ?string
    {
        if (!\array_key_exists($key, $fields) || $fields[$key] === null) {
            return null;
        }

        return self::requireString($fields, $key);
    }

    /**
     * @param array<array-key, mixed> $fields
     */
    private static function requireInt(array $fields, string $key): int
    {
        if (!\array_key_exists($key, $fields)) {
            throw new MalformedEnvelope(\sprintf('envelope is missing the field `%s`', $key));
        }
        if (!\is_int($fields[$key])) {
            throw new MalformedEnvelope(\sprintf('envelope field `%s` must be an integer', $key));
        }

        return $fields[$key];
    }

    /**
     * @param array<array-key, mixed> $fields
     */
    private static function optionalInt(array $fields, string $key): int
    {
        if (!\array_key_exists($key, $fields) || $fields[$key] === null) {
            return 0;
        }

        return self::requireInt($fields, $key);
    }

    private static function assertRange(int $value, int $min, int $max, string $field): void
    {
        if ($value < $min || $value > $max) {
            throw new InvalidConfiguration(\sprintf('envelope field `%s` must be between %d and %d, got %d', $field, $min, $max, $value));
        }
    }
}
