<?php

declare(strict_types=1);

namespace Queuey;

/**
 * A synchronous transport. Implementations: `MemoryBackend`, `RabbitMqBackend`.
 *
 * Two rules decide correctness: publish before ack, and never release a message
 * early.
 */
interface Backend
{
    /** Idempotently create every queue plus its dead-letter infrastructure. */
    public function declare(QueueConfig ...$queues): void;

    /** Publish to `$envelope->queue`, optionally held for `$delay` first (retry granularity). */
    public function publish(Envelope $envelope, ?Duration $delay = null): void;

    /**
     * Hold `$envelope` for `$delay` (deferral granularity) and then release it
     * onto `$envelope->queue`.
     */
    public function defer(Envelope $envelope, Duration $delay): void;

    /**
     * Register a consumer for `$queue`. Deliveries are handed to `$onDelivery`
     * from inside {@see self::wait()}, never before.
     *
     * @param callable(Delivery): void $onDelivery
     */
    public function consume(QueueConfig $queue, callable $onDelivery): void;

    /**
     * Block until at least one delivery was dispatched or `$timeoutSeconds`
     * elapsed. `null` means no limit.
     */
    public function wait(?float $timeoutSeconds = null): void;

    /** Stop receiving new deliveries; deliveries already in hand stay settleable. */
    public function cancelConsumers(): void;

    /** Final shutdown: stop consumers, flush, close connections. */
    public function close(): void;
}
