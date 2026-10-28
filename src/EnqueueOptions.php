<?php

declare(strict_types=1);

namespace Queuey;

/**
 * When and how a job is published: the options {@see Producer::enqueue()} takes.
 *
 * The default is "publish now, no correlation id", which is what a plain
 * `enqueue()` does. The three constructors are the whole scheduling vocabulary
 * and compose with the setter:
 *
 * ```php
 * // Hold for 30s, then release ahead of the backlog, tagged with a trace id.
 * $options = EnqueueOptions::deferred(Duration::seconds(30))->withCorrelationId('trace-abc');
 * ```
 */
final class EnqueueOptions
{
    /**
     * @param Duration|null $delay    how long the job waits before it becomes visible; `null` is "now"
     * @param bool          $deferred whether the delay is a deferral (returns at the queue's top priority)
     */
    private function __construct(
        private readonly ?Duration $delay,
        private readonly bool $deferred,
        public readonly ?string $correlationId = null,
    ) {}

    /** Publish as soon as the call reaches the broker. */
    public static function now(): self
    {
        return new self(null, false);
    }

    /** Publish after `$delay`, at normal priority. See {@see Producer::enqueueAfter()}. */
    public static function after(Duration $delay): self
    {
        return new self($delay, false);
    }

    /** Hold for `$delay`, then release at the queue's top priority. See {@see Producer::defer()}. */
    public static function deferred(Duration $delay): self
    {
        return new self($delay, true);
    }

    /**
     * Tag the envelope with a correlation id, carried through every retry and
     * deferral and handed to the handler as {@see JobContext::$correlationId}.
     */
    public function withCorrelationId(string $correlationId): self
    {
        return new self($this->delay, $this->deferred, $correlationId);
    }

    /** How long the job waits before it becomes visible; `null` means immediately. */
    public function delay(): ?Duration
    {
        return $this->delay;
    }

    /**
     * Whether the wait is a deferral: the envelope comes back at its queue's
     * highest priority instead of behind the backlog.
     */
    public function isDeferred(): bool
    {
        return $this->deferred;
    }
}
