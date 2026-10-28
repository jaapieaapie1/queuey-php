<?php

declare(strict_types=1);

namespace Queuey;

/**
 * One message taken from the broker. Settled exactly once.
 *
 * Every settling method that publishes does so **before** it acks the original:
 * if the publish fails the original stays unacked and the broker redelivers it,
 * so a crash duplicates work rather than losing it.
 */
interface Delivery
{
    public function envelope(): Envelope;

    /** Processed successfully; remove from the broker. */
    public function ack(): void;

    /** Route to `q.dead` with `$reason`, then ack. */
    public function deadLetter(string $reason): void;

    /**
     * Schedule `$next` (already `attempt + 1`) to reappear on its queue after
     * `$delay`, rounded to the backend's retry granularity, then ack.
     */
    public function retry(Envelope $next, Duration $delay): void;

    /**
     * Schedule `$next` (already `deferrals + 1`, priority set, attempt
     * unchanged) after `$delay`, rounded to the backend's deferral granularity,
     * then ack.
     */
    public function defer(Envelope $next, Duration $delay): void;
}
