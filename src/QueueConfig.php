<?php

declare(strict_types=1);

namespace Queuey;

use Queuey\Exception\InvalidConfiguration;

/**
 * Static configuration of a single queue, with the prefix already applied.
 *
 * Immutable: every setter is a wither returning a new config.
 */
final class QueueConfig
{
    /** RabbitMQ recommends at most 10 priority levels. */
    public const DEFAULT_MAX_PRIORITY = 10;
    public const DEFAULT_PREFETCH = 16;
    public const MAX_PREFETCH = 65535;

    /**
     * Largest `messageTtl` a broker can be asked for, in milliseconds (`u32::MAX`).
     *
     * RabbitMQ stores `x-message-ttl` as an unsigned 32-bit millisecond count, so
     * anything longer cannot be expressed and would be silently clamped. The Rust
     * derives reject it at compile time (`crates/macros`, `MAX_MESSAGE_TTL_MS`);
     * PHP rejects it here. This is *not* `MAX_DEFERRAL_MS`, which is half as
     * large and bounds a hold queue's delay rather than a queue's message TTL.
     */
    public const MAX_MESSAGE_TTL_MS = 4294967295;

    /** Shared by the check and its error message. */
    public const MESSAGE_TTL_RANGE = '`messageTtl` must be between 1ms and 4294967295ms (~49.7 days)'
        . ': RabbitMQ stores x-message-ttl as an unsigned 32-bit millisecond count';

    /** Default policy for jobs on this queue; a job may override it. */
    public readonly RetryPolicy $retry;

    /**
     * Number of priority levels (`x-max-priority`). `null` means the queue is not
     * a priority queue and the broker ignores message priorities.
     */
    public readonly ?int $maxPriority;

    /**
     * @param string   $name        fully qualified broker queue name
     * @param int      $prefetch    max unacknowledged messages per consumer, 1..65535
     * @param bool     $durable     whether the queue survives a broker restart
     * @param int|null $maxPriority `0` and `null` both mean "not a priority queue"
     */
    public function __construct(
        public readonly string $name,
        public readonly int $prefetch = self::DEFAULT_PREFETCH,
        ?RetryPolicy $retry = null,
        public readonly bool $durable = true,
        public readonly ?Duration $messageTtl = null,
        ?int $maxPriority = self::DEFAULT_MAX_PRIORITY,
    ) {
        if (trim($name) === '') {
            throw new InvalidConfiguration('queue name must not be empty or only whitespace');
        }
        if ($prefetch < 1 || $prefetch > self::MAX_PREFETCH) {
            throw new InvalidConfiguration(\sprintf('`prefetch` must be between 1 and %d, got %d', self::MAX_PREFETCH, $prefetch));
        }
        if ($maxPriority !== null && ($maxPriority < 0 || $maxPriority > 255)) {
            throw new InvalidConfiguration(\sprintf('`maxPriority` must be in 0..=255, got %d', $maxPriority));
        }
        if ($messageTtl !== null && $messageTtl->isZero()) {
            throw new InvalidConfiguration('`messageTtl` must be greater than zero: a zero TTL discards every message the moment it is published');
        }
        if ($messageTtl !== null && $messageTtl->toMs() > self::MAX_MESSAGE_TTL_MS) {
            throw new InvalidConfiguration(self::MESSAGE_TTL_RANGE);
        }

        $this->retry = $retry ?? RetryPolicy::none();
        $this->maxPriority = ($maxPriority !== null && $maxPriority > 0) ? $maxPriority : null;
    }

    /** Config for `$name` with the documented defaults: prefetch 16, durable, no retries, 10 priority levels. */
    public static function new(string $name): self
    {
        return new self($name);
    }

    public function withPrefetch(int $prefetch): self
    {
        return $this->with(prefetch: $prefetch);
    }

    public function withRetry(RetryPolicy $retry): self
    {
        return $this->with(retry: $retry);
    }

    public function withDurable(bool $durable): self
    {
        return $this->with(durable: $durable);
    }

    public function withMessageTtl(Duration $messageTtl): self
    {
        return $this->with(messageTtl: $messageTtl);
    }

    /**
     * Set the number of priority levels; `0` turns priorities off.
     *
     * Changing this for a queue that already exists on the broker is refused by
     * RabbitMQ (`PRECONDITION_FAILED`): delete the queue first.
     */
    public function withMaxPriority(int $levels): self
    {
        return $this->with(maxPriority: $levels, maxPrioritySet: true);
    }

    private function with(
        ?int $prefetch = null,
        ?RetryPolicy $retry = null,
        ?bool $durable = null,
        ?Duration $messageTtl = null,
        ?int $maxPriority = null,
        bool $maxPrioritySet = false,
    ): self {
        return new self(
            name: $this->name,
            prefetch: $prefetch ?? $this->prefetch,
            retry: $retry ?? $this->retry,
            durable: $durable ?? $this->durable,
            messageTtl: $messageTtl ?? $this->messageTtl,
            maxPriority: $maxPrioritySet ? $maxPriority : $this->maxPriority,
        );
    }
}
