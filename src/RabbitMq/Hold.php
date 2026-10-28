<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use Queuey\Duration;

/**
 * Why a message is going into a hold queue.
 *
 * Retries and deferrals share the hold queues — the arguments depend only on
 * the delay — and differ only in the granularity their delay is rounded to and
 * in the priority the message carries back onto the main queue.
 */
enum Hold
{
    /** A failed attempt, a delayed enqueue: comes back at priority 0. */
    case Retry;

    /** A job that cannot run yet: comes back at the queue's top priority. */
    case Deferral;

    public function granularity(RabbitMqOptions $options): Duration
    {
        return $options->granularityFor($this);
    }

    public function logMessage(): string
    {
        return match ($this) {
            self::Retry => 'holding job for a delayed redelivery',
            self::Deferral => 'deferring job',
        };
    }
}
