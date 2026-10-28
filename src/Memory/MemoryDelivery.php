<?php

declare(strict_types=1);

namespace Queuey\Memory;

use Queuey\Delivery;
use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\AlreadySettled;

/**
 * One message handed out by a {@see MemoryBackend}, settleable exactly once.
 *
 * Every settling method that publishes does so before it records the ack, so a
 * backend that refuses the publish (because it was closed) leaves the delivery
 * unsettled — the in-memory stand-in for "the broker still owns the message".
 */
final class MemoryDelivery implements Delivery
{
    private bool $settled = false;

    public function __construct(
        private readonly MemoryBackend $backend,
        private readonly Envelope $envelope,
    ) {}

    public function envelope(): Envelope
    {
        return $this->envelope;
    }

    /** Whether this delivery has been settled, by any of the four outcomes. */
    public function isSettled(): bool
    {
        return $this->settled;
    }

    public function ack(): void
    {
        $this->assertUnsettled();
        $this->settle(AckKind::Succeeded);
    }

    /**
     * Records the dead letter and settles, but records **no ack**: a dead letter
     * is not one, here or in the Rust backend, so it never shows up in
     * {@see MemoryBackend::acked()}. {@see MemoryBackend::settled()} counts it.
     */
    public function deadLetter(string $reason): void
    {
        $this->assertUnsettled();
        $this->backend->recordDead($this->envelope, $reason);
        $this->settled = true;
    }

    public function retry(Envelope $next, Duration $delay): void
    {
        $this->assertUnsettled();
        $this->backend->publish($next, $delay);
        $this->settle(AckKind::Retried);
    }

    public function defer(Envelope $next, Duration $delay): void
    {
        $this->assertUnsettled();
        $this->backend->defer($next, $delay);
        $this->settle(AckKind::Deferred);
    }

    private function settle(AckKind $kind): void
    {
        $this->settled = true;
        $this->backend->recordAck($kind);
    }

    private function assertUnsettled(): void
    {
        if ($this->settled) {
            throw new AlreadySettled(\sprintf('delivery of job %s was already settled', $this->envelope->jobId));
        }
    }
}
