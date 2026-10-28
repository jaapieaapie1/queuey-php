<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Wire\AMQPTable;
use Queuey\Duration;
use Queuey\Exception\BackendException;
use Queuey\Exception\DelayTooLong;
use Queuey\QueueConfig;

/**
 * Pure helpers describing the RabbitMQ topology this backend maintains.
 *
 * For a logical queue `q` there are two long-lived broker queues plus one
 * short-lived *hold* queue per distinct delay:
 *
 * | queue | role | arguments |
 * |---|---|---|
 * | `q` | main work queue | `x-message-ttl` when configured, `x-max-priority` when the queue has priorities |
 * | `q.dead` | dead-letter queue | none |
 * | `q.deferred.<ttlMs>` | hold queue for one delay | `x-message-ttl`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-expires` |
 *
 * Every wait — a retry backoff, a delayed enqueue, a deferral — goes through a
 * hold queue rather than a per-message `expiration` on one shared wait queue. A
 * classic queue only ever expires the message at its *head*, so a five-minute
 * wait at the head would hold back every one-second wait behind it. Baking the
 * delay into the queue *name* gives every message in one hold queue the same
 * TTL, so they expire in publish order and a short wait can never be stuck
 * behind a long one.
 *
 * A hold queue's arguments are a pure function of its name, deliberately: two
 * processes running different builds must compute byte-identical arguments for
 * `q.deferred.30000`, or RabbitMQ locks one of them out of it for ever with
 * `PRECONDITION_FAILED`. That is why `x-expires` is fixed at `2 * ttl` and is
 * not a tunable.
 *
 * Nothing here touches a connection, so all of it is unit-testable without a
 * broker. See INTEROP-SPEC §3.
 */
final class Topology
{
    /** Default suffix appended to a queue name to build its dead-letter queue. */
    public const DEFAULT_DEAD_SUFFIX = '.dead';

    /**
     * Default infix between a queue name and a hold queue's TTL.
     *
     * The name says "deferred" because deferrals were the first user; retries
     * and delayed enqueues share the very same queues.
     */
    public const DEFAULT_DEFERRED_SUFFIX = '.deferred';

    /** Header carrying the dead-lettering reason on messages routed to `q.dead`. */
    public const HEADER_DEATH_REASON = 'x-death-reason';

    /** Header carrying the originating queue name on messages routed to `q.dead`. */
    public const HEADER_ORIGINAL_QUEUE = 'x-original-queue';

    /** Header carrying the number of attempts made before dead-lettering. */
    public const HEADER_ATTEMPTS = 'x-attempts';

    /** Header mirroring `Envelope::$attempt` on every publish. */
    public const HEADER_ATTEMPT = 'x-attempt';

    /**
     * Header mirroring `Envelope::$deferrals` on every publish.
     *
     * Purely informational, like {@see self::HEADER_ATTEMPT}: the JSON body is
     * the source of truth and nothing in this backend reads these back.
     */
    public const HEADER_DEFERRALS = 'x-deferrals';

    public const ARG_DEAD_LETTER_EXCHANGE = 'x-dead-letter-exchange';
    public const ARG_DEAD_LETTER_ROUTING_KEY = 'x-dead-letter-routing-key';
    public const ARG_MESSAGE_TTL = 'x-message-ttl';

    /**
     * How many priority levels a queue supports.
     *
     * Set on the main queue only. A hold queue must stay FIFO, and a priority
     * queue costs the broker an index per level.
     */
    public const ARG_MAX_PRIORITY = 'x-max-priority';

    /** Makes the broker delete a hold queue once it has been unused that long. */
    public const ARG_EXPIRES = 'x-expires';

    /**
     * Largest time-to-live RabbitMQ accepts, in milliseconds (~49.7 days).
     *
     * `x-message-ttl` and `x-expires` are parsed as unsigned 32-bit millisecond
     * counts; a larger value is answered with `PRECONDITION_FAILED`, which
     * closes the declaring channel.
     */
    public const MAX_TTL_MS = 4294967295;

    /**
     * Longest delay a hold queue can express, in milliseconds (~24.8 days).
     *
     * A hold queue carries `x-expires = 2 * x-message-ttl`, and `x-expires` has
     * the same 32-bit domain, so half of {@see self::MAX_TTL_MS} is the largest
     * TTL whose doubled expiry still fits *and* still stays strictly above the
     * TTL. A longer delay is refused, never clamped: clamping would release the
     * job early, and "never early" is the one timing guarantee a hold makes.
     */
    public const MAX_DEFERRAL_MS = 2147483647;

    /** AMQP short strings — queue names, consumer tags, `message-id` — cap at this. */
    public const MAX_SHORT_STRING = 255;

    /** How much of a queue name a consumer tag carries. */
    public const CONSUMER_TAG_QUEUE_LIMIT = 160;

    private function __construct() {}

    /** Name of the dead-letter queue for `$queue`. */
    public static function deadQueueName(string $queue, string $suffix = self::DEFAULT_DEAD_SUFFIX): string
    {
        return $queue . $suffix;
    }

    /**
     * Name of the hold queue holding `$ttlMs`-long waits of `$queue`.
     *
     * The TTL is part of the name on purpose: one queue per distinct delay is
     * what makes a hold queue drain strictly in order.
     */
    public static function deferredQueueName(
        string $queue,
        string $suffix = self::DEFAULT_DEFERRED_SUFFIX,
        int $ttlMs = 0,
    ): string {
        return $queue . $suffix . '.' . $ttlMs;
    }

    /**
     * The hold-queue TTL for `$delay`: rounded **up** to a whole multiple of
     * `$granularity`, at least one whole step.
     *
     * Rounding up is what bounds the number of hold queues, and rounding *up*
     * rather than to nearest guarantees the job never comes back early. A delay
     * that lands past {@see self::MAX_DEFERRAL_MS} is refused for the same
     * reason — the rounding itself can push a delay over the cap, so a delay
     * just under it may still be refused.
     *
     * `$granularity` is clamped to `[1ms, MAX_DEFERRAL_MS]`, so a zero or absurd
     * value out of a config file is corrected rather than dividing by zero.
     *
     * @throws DelayTooLong when the rounded delay is longer than a hold queue can express
     */
    public static function deferredTtlMs(Duration $delay, Duration $granularity): int
    {
        $step = min(max($granularity->toMs(), 1), self::MAX_DEFERRAL_MS);
        $delayMs = $delay->toMs();

        // Rounding only ever grows the delay, so anything already past the cap
        // is refused before the multiplication below could overflow.
        if ($delayMs > self::MAX_DEFERRAL_MS) {
            throw self::tooLong($delayMs);
        }

        $ttl = max((int) ceil($delayMs / $step) * $step, $step);
        if ($ttl > self::MAX_DEFERRAL_MS) {
            throw self::tooLong($delayMs);
        }

        return $ttl;
    }

    /**
     * Declaration arguments for the main queue `q`.
     *
     * Both are *declaration* arguments, which RabbitMQ refuses to change on an
     * existing queue: adding `x-max-priority` to a queue that predates it means
     * deleting the queue or setting `maxPriority` back to `0`.
     */
    public static function queueArgs(QueueConfig $config): AMQPTable
    {
        $args = new AMQPTable();
        if ($config->messageTtl !== null) {
            $args->set(self::ARG_MESSAGE_TTL, self::ttlMillis($config->messageTtl), AMQPTable::T_INT_LONGLONG);
        }
        if ($config->maxPriority !== null) {
            // RabbitMQ validates `x-max-priority` as an integer of any width; an
            // unsigned byte is the exact domain of `QueueConfig::$maxPriority`.
            $args->set(self::ARG_MAX_PRIORITY, $config->maxPriority, AMQPTable::T_INT_SHORTSHORT_U);
        }

        return $args;
    }

    /**
     * Declaration arguments for the hold queue `q.deferred.<ttlMs>`: exactly four.
     *
     * `x-expires` is `2 * ttl` and nothing else, so an idle hold queue deletes
     * itself one TTL after the last publish to it while never deleting a queue
     * that still owes a message. Every declare resets that timer, which is why
     * the queue is redeclared before every publish. Never carries
     * `x-max-priority`.
     */
    public static function deferredQueueArgs(QueueConfig $config, int $ttlMs): AMQPTable
    {
        $args = new AMQPTable();
        $args->set(self::ARG_MESSAGE_TTL, $ttlMs, AMQPTable::T_INT_LONGLONG);
        $args->set(self::ARG_DEAD_LETTER_EXCHANGE, '', AMQPTable::T_STRING_LONG);
        $args->set(self::ARG_DEAD_LETTER_ROUTING_KEY, $config->name, AMQPTable::T_STRING_LONG);
        // `$ttlMs` comes from `deferredTtlMs` and is at most `MAX_DEFERRAL_MS`,
        // so the double fits. The clamp guards a hand-rolled call: an
        // `x-expires` past `MAX_TTL_MS` is a `PRECONDITION_FAILED`, not a long
        // timeout.
        $args->set(self::ARG_EXPIRES, min($ttlMs * 2, self::MAX_TTL_MS), AMQPTable::T_INT_LONGLONG);

        return $args;
    }

    /**
     * Declaration arguments for `q.dead`: deliberately empty.
     *
     * Dead-lettered messages are terminal and must not expire or be routed
     * onwards without an operator looking at them.
     */
    public static function deadQueueArgs(): AMQPTable
    {
        return new AMQPTable();
    }

    /**
     * Refuse a queue name that leaves no room for its own hold queues.
     *
     * Checked once, up front, at declare time: otherwise the name declares fine
     * and then fails one retry at a time, in production, on the day someone
     * picks a long delay.
     *
     * @throws BackendException
     */
    public static function checkDeferrableName(
        string $queue,
        string $deferredSuffix = self::DEFAULT_DEFERRED_SUFFIX,
    ): void {
        $hold = self::deferredQueueName($queue, $deferredSuffix, self::MAX_DEFERRAL_MS);
        $length = \strlen($hold);
        if ($length <= self::MAX_SHORT_STRING) {
            return;
        }

        throw new BackendException(\sprintf(
            'queue `%s` leaves no room for its hold queues: `%s` is %d bytes, past the %d-byte AMQP limit',
            $queue,
            $hold,
            $length,
            self::MAX_SHORT_STRING,
        ));
    }

    /**
     * Whole milliseconds for a queue-wide TTL, clamped to `[1, MAX_TTL_MS]`.
     *
     * The upper bound is RabbitMQ's, not ours: a TTL past `MAX_TTL_MS` closes
     * the declaring channel with `PRECONDITION_FAILED`.
     */
    private static function ttlMillis(Duration $ttl): int
    {
        return min(max($ttl->toMs(), 1), self::MAX_TTL_MS);
    }

    private static function tooLong(int $delayMs): DelayTooLong
    {
        return new DelayTooLong(\sprintf(
            'a delay of %d ms is longer than a hold queue can express (at most %d ms); nothing was published',
            $delayMs,
            self::MAX_DEFERRAL_MS,
        ));
    }
}
