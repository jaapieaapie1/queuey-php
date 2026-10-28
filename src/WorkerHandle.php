<?php

declare(strict_types=1);

namespace Queuey;

/**
 * Remote control for a {@see Worker}, and where it keeps its counters.
 *
 * Handed out by {@see Worker::handle()} before `run()` is called, so a signal
 * handler, a supervisor or a test can ask the loop to stop from outside it.
 */
final class WorkerHandle
{
    private bool $shutdown = false;

    private int $settleFailures = 0;

    private int $processed = 0;

    /**
     * Ask the worker to stop after the delivery it is on.
     *
     * Calling this more than once, or before {@see Worker::run()} starts, is fine.
     */
    public function shutdown(): void
    {
        $this->shutdown = true;
    }

    /** Whether {@see self::shutdown()} has been called. */
    public function isShutdown(): bool
    {
        return $this->shutdown;
    }

    /**
     * How many times the worker failed to ack, retry, defer or dead-letter a delivery.
     *
     * Each failure is also logged at `error`. The job itself ran; only telling
     * the broker about the outcome failed, so the message is still owned by the
     * broker and will be redelivered. A non-zero — and especially a growing —
     * count means duplicate processing ahead, and usually a sick connection.
     */
    public function settleFailures(): int
    {
        return $this->settleFailures;
    }

    /** How many deliveries the worker has dispatched, whatever their outcome. */
    public function processed(): int
    {
        return $this->processed;
    }

    /**
     * @internal to {@see Worker}
     */
    public function recordSettleFailure(): void
    {
        ++$this->settleFailures;
    }

    /**
     * @internal to {@see Worker}
     */
    public function recordProcessed(): void
    {
        ++$this->processed;
    }
}
