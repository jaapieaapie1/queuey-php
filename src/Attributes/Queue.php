<?php

declare(strict_types=1);

namespace Queuey\Attributes;

use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;
use Queuey\QueueConfig;

/**
 * Per-case configuration on a queue-set enum:
 * `#[Queue(name: 'emails', prefetch: 4, retry: new Retry(maxAttempts: 5))]`.
 *
 * Everything is optional; what is left out keeps the {@see QueueConfig} default.
 * The name defaults to the `snake_case` form of the case name.
 */
#[\Attribute(\Attribute::TARGET_CLASS_CONSTANT)]
final class Queue
{
    public function __construct(
        public readonly ?string $name = null,
        public readonly ?int $prefetch = null,
        public readonly ?bool $durable = null,
        public readonly ?string $messageTtl = null,
        public readonly ?int $maxPriority = null,
        public readonly ?Retry $retry = null,
    ) {
        if ($name !== null && trim($name) === '') {
            throw new InvalidConfiguration('`name` must not be empty or only whitespace');
        }
        if ($prefetch !== null && ($prefetch < 1 || $prefetch > QueueConfig::MAX_PREFETCH)) {
            throw new InvalidConfiguration(\sprintf('`prefetch` must be between 1 and %d, got %d', QueueConfig::MAX_PREFETCH, $prefetch));
        }
        if ($maxPriority !== null && ($maxPriority < 0 || $maxPriority > 255)) {
            throw new InvalidConfiguration(\sprintf('`maxPriority` must be in 0..=255, got %d', $maxPriority));
        }
        if ($messageTtl !== null) {
            $ttl = $this->messageTtlDuration();
            if ($ttl->isZero()) {
                throw new InvalidConfiguration('`messageTtl` must be greater than zero: a zero TTL discards every message the moment it is published');
            }
            if ($ttl->toMs() > QueueConfig::MAX_MESSAGE_TTL_MS) {
                throw new InvalidConfiguration(QueueConfig::MESSAGE_TTL_RANGE);
            }
        }
    }

    /** Build the config for `$qualifiedName`, applying every key that was set. */
    public function toConfig(string $qualifiedName): QueueConfig
    {
        $config = new QueueConfig($qualifiedName);

        if ($this->prefetch !== null) {
            $config = $config->withPrefetch($this->prefetch);
        }
        if ($this->durable !== null) {
            $config = $config->withDurable($this->durable);
        }
        if ($this->messageTtl !== null) {
            $config = $config->withMessageTtl($this->messageTtlDuration());
        }
        if ($this->maxPriority !== null) {
            $config = $config->withMaxPriority($this->maxPriority);
        }
        if ($this->retry !== null) {
            $config = $config->withRetry($this->retry->toPolicy());
        }

        return $config;
    }

    private function messageTtlDuration(): Duration
    {
        return Duration::parse($this->messageTtl ?? '');
    }
}
