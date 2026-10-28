<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Wire\AMQPTable;
use Queuey\Envelope;

/**
 * Pure mapping between an {@see Envelope} and AMQP properties and headers.
 *
 * Nothing here touches the broker, so every function is unit-testable without a
 * running RabbitMQ. See INTEROP-SPEC §2.
 */
final class Codec
{
    /** `content-type` set on every published envelope. */
    public const CONTENT_TYPE_JSON = 'application/json';

    /** `delivery-mode` for a persistent message. */
    public const DELIVERY_MODE_PERSISTENT = 2;

    /** Reason recorded on bodies that could not be decoded as an envelope. */
    public const REASON_MALFORMED = 'malformed envelope';

    private function __construct() {}

    /**
     * AMQP properties for publishing `$envelope`.
     *
     * `priority` is always set: normal work carries `0`, a deferred envelope its
     * queue's top level so it overtakes the backlog. A queue declared without
     * `x-max-priority` ignores the property and a priority above the queue's
     * maximum is treated as that maximum, so setting it unconditionally is safe.
     *
     * `expiration` is **never** set, not even for a message bound for a hold
     * queue: the wait is the hold queue's queue-wide `x-message-ttl`, and a
     * per-message expiration on top of it would bring back exactly the
     * mixed-TTL head-of-line blocking hold queues exist to avoid.
     *
     * @return array<string, mixed>
     */
    public static function propsFor(Envelope $envelope): array
    {
        return [
            'content_type' => self::CONTENT_TYPE_JSON,
            'delivery_mode' => self::DELIVERY_MODE_PERSISTENT,
            'message_id' => self::shortString($envelope->jobId),
            'type' => self::shortString($envelope->jobType),
            'priority' => $envelope->priority,
            'application_headers' => self::baseHeaders($envelope),
        ];
    }

    /**
     * AMQP properties for publishing `$envelope` to its dead-letter queue: the
     * same as {@see self::propsFor()} plus the three `x-death-*` headers.
     *
     * @return array<string, mixed>
     */
    public static function deadLetterProps(Envelope $envelope, string $reason): array
    {
        $props = self::propsFor($envelope);
        $props['application_headers'] = self::deadLetterHeaders($envelope, $reason);

        return $props;
    }

    /**
     * AMQP properties for a body that could not be decoded as an envelope.
     *
     * The original bytes are forwarded verbatim, so there is no attempt counter
     * and no job metadata to carry — only where it came from and why it was
     * rejected.
     *
     * @return array<string, mixed>
     */
    public static function malformedProps(string $originalQueue, string $reason = self::REASON_MALFORMED): array
    {
        $headers = new AMQPTable();
        $headers->set(Topology::HEADER_DEATH_REASON, $reason, AMQPTable::T_STRING_LONG);
        $headers->set(Topology::HEADER_ORIGINAL_QUEUE, $originalQueue, AMQPTable::T_STRING_LONG);

        return [
            'delivery_mode' => self::DELIVERY_MODE_PERSISTENT,
            'application_headers' => $headers,
        ];
    }

    /**
     * Headers carried by every published envelope.
     *
     * `x-attempt` and `x-deferrals` mirror the envelope so an operator can read
     * both counters in the management UI without decoding the body. The body
     * stays the source of truth: nothing here reads them back.
     */
    public static function baseHeaders(Envelope $envelope): AMQPTable
    {
        $headers = new AMQPTable();
        $headers->set(Topology::HEADER_ATTEMPT, $envelope->attempt, AMQPTable::T_INT_LONG_U);
        $headers->set(Topology::HEADER_DEFERRALS, $envelope->deferrals, AMQPTable::T_INT_LONG_U);

        return $headers;
    }

    /** {@see self::baseHeaders()} plus `x-death-reason`, `x-original-queue` and `x-attempts`. */
    public static function deadLetterHeaders(Envelope $envelope, string $reason): AMQPTable
    {
        $headers = self::baseHeaders($envelope);
        $headers->set(Topology::HEADER_DEATH_REASON, $reason, AMQPTable::T_STRING_LONG);
        $headers->set(Topology::HEADER_ORIGINAL_QUEUE, $envelope->queue, AMQPTable::T_STRING_LONG);
        $headers->set(Topology::HEADER_ATTEMPTS, $envelope->attempt, AMQPTable::T_INT_LONG_U);

        return $headers;
    }

    /**
     * The longest prefix of `$value` that is at most `$max` bytes and never
     * splits a multi-byte UTF-8 character.
     */
    public static function truncateAtBoundary(string $value, int $max): string
    {
        if (\strlen($value) <= $max) {
            return $value;
        }

        $end = $max;
        while ($end > 0 && !self::isCharBoundary($value, $end)) {
            --$end;
        }

        return substr($value, 0, $end);
    }

    /**
     * An AMQP short string: truncated rather than rejected, because library code
     * must never blow up on a user-supplied job type.
     */
    private static function shortString(string $value): string
    {
        return self::truncateAtBoundary($value, Topology::MAX_SHORT_STRING);
    }

    /** Whether byte `$offset` starts a UTF-8 character (i.e. is not a continuation byte). */
    private static function isCharBoundary(string $value, int $offset): bool
    {
        if ($offset >= \strlen($value)) {
            return true;
        }

        return (\ord($value[$offset]) & 0xC0) !== 0x80;
    }
}
