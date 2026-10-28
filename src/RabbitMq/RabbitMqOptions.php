<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;

/**
 * Tunables for {@see RabbitMqBackend}.
 *
 * Immutable: every setter is a wither returning a new instance. Start from
 * `new RabbitMqOptions()` and chain.
 */
final class RabbitMqOptions
{
    /** Default for both granularities. */
    public const DEFAULT_GRANULARITY_MS = 1000;

    public const DEFAULT_HEARTBEAT = 30;
    public const DEFAULT_CONNECTION_TIMEOUT = 10.0;
    public const DEFAULT_READ_WRITE_TIMEOUT = 60.0;
    public const DEFAULT_PUBLISH_CONFIRM_TIMEOUT = 30.0;

    public readonly Duration $retryGranularity;
    public readonly Duration $deferredGranularity;

    /**
     * @param string|null           $connectionName          name reported to the broker, shown in the management UI; purely descriptive
     * @param string                $deadSuffix              suffix appended to a queue name to name its dead-letter queue
     * @param bool                  $declareDeadLetterQueues whether `declare()` also declares the `q.dead` queues; `false` turns
     *                                                       dead-lettering into `basic_reject(requeue = false)` on the original,
     *                                                       because this backend never publishes to a queue it does not own
     * @param string                $deferredSuffix          infix between a queue name and a hold queue's TTL
     * @param Duration|null         $retryGranularity        step retry backoffs and delayed enqueues are rounded **up** to
     * @param Duration|null         $deferredGranularity     step deferral delays are rounded **up** to
     * @param ReconnectPolicy|false $reconnect               how a lost connection is recovered; `false` disables reconnection
     * @param int                   $heartbeat               AMQP heartbeat in seconds; `0` disables it
     * @param float                 $connectionTimeout       seconds allowed for the TCP connect and handshake
     * @param float                 $readWriteTimeout        socket read/write timeout; must be at least twice the heartbeat
     * @param float                 $publishConfirmTimeout   seconds to wait for a publisher confirm before giving up
     */
    public function __construct(
        public readonly ?string $connectionName = null,
        public readonly string $deadSuffix = Topology::DEFAULT_DEAD_SUFFIX,
        public readonly bool $declareDeadLetterQueues = true,
        public readonly string $deferredSuffix = Topology::DEFAULT_DEFERRED_SUFFIX,
        ?Duration $retryGranularity = null,
        ?Duration $deferredGranularity = null,
        public readonly ReconnectPolicy|false $reconnect = new BackoffPolicy(),
        public readonly int $heartbeat = self::DEFAULT_HEARTBEAT,
        public readonly float $connectionTimeout = self::DEFAULT_CONNECTION_TIMEOUT,
        public readonly float $readWriteTimeout = self::DEFAULT_READ_WRITE_TIMEOUT,
        public readonly float $publishConfirmTimeout = self::DEFAULT_PUBLISH_CONFIRM_TIMEOUT,
    ) {
        if ($heartbeat < 0) {
            throw new InvalidConfiguration(\sprintf('`heartbeat` must not be negative, got %d', $heartbeat));
        }
        if ($connectionTimeout <= 0) {
            throw new InvalidConfiguration('`connectionTimeout` must be greater than zero');
        }
        if ($publishConfirmTimeout <= 0) {
            throw new InvalidConfiguration('`publishConfirmTimeout` must be greater than zero');
        }
        // Checked whatever the heartbeat is: with heartbeats off there is no
        // lower bound left to catch it, and php-amqplib would take a zero or
        // negative timeout as "never time out", so a dead peer would hang a
        // read forever instead of failing.
        if ($readWriteTimeout <= 0) {
            throw new InvalidConfiguration('`readWriteTimeout` must be greater than zero');
        }
        // php-amqplib refuses the socket otherwise: a read timeout shorter than
        // two heartbeat intervals tears the connection down on an idle queue.
        if ($heartbeat > 0 && $readWriteTimeout < $heartbeat * 2) {
            throw new InvalidConfiguration(\sprintf(
                '`readWriteTimeout` must be at least twice the heartbeat (%d s), got %s s',
                $heartbeat,
                (string) $readWriteTimeout,
            ));
        }

        $this->retryGranularity = $retryGranularity ?? Duration::ms(self::DEFAULT_GRANULARITY_MS);
        $this->deferredGranularity = $deferredGranularity ?? Duration::ms(self::DEFAULT_GRANULARITY_MS);
    }

    /** The documented defaults. */
    public static function new(): self
    {
        return new self();
    }

    /** Name this backend's connection for the broker's benefit. */
    public function withConnectionName(?string $name): self
    {
        return $this->with(connectionName: $name, connectionNameSet: true);
    }

    public function withDeadSuffix(string $suffix): self
    {
        return $this->with(deadSuffix: $suffix);
    }

    public function withDeclareDeadLetterQueues(bool $declare): self
    {
        return $this->with(declareDeadLetterQueues: $declare);
    }

    public function withDeferredSuffix(string $suffix): self
    {
        return $this->with(deferredSuffix: $suffix);
    }

    /**
     * Replace the step retry backoffs and delayed enqueues are rounded up to.
     *
     * A zero or sub-millisecond value is *clamped* when the TTL is computed,
     * not rejected here: this is a builder, and library code does not blow up
     * on a configuration value.
     */
    public function withRetryGranularity(Duration $granularity): self
    {
        return $this->with(retryGranularity: $granularity);
    }

    /** Replace the step deferral delays are rounded up to. */
    public function withDeferredGranularity(Duration $granularity): self
    {
        return $this->with(deferredGranularity: $granularity);
    }

    /** Replace the reconnect policy, or pass `false` to let a drop surface as an error. */
    public function withReconnect(ReconnectPolicy|false $reconnect): self
    {
        return $this->with(reconnect: $reconnect, reconnectSet: true);
    }

    public function withHeartbeat(int $seconds): self
    {
        return $this->with(heartbeat: $seconds);
    }

    public function withConnectionTimeout(float $seconds): self
    {
        return $this->with(connectionTimeout: $seconds);
    }

    public function withReadWriteTimeout(float $seconds): self
    {
        return $this->with(readWriteTimeout: $seconds);
    }

    public function withPublishConfirmTimeout(float $seconds): self
    {
        return $this->with(publishConfirmTimeout: $seconds);
    }

    /** The granularity `$hold` rounds its delays to. */
    public function granularityFor(Hold $hold): Duration
    {
        return match ($hold) {
            Hold::Retry => $this->retryGranularity,
            Hold::Deferral => $this->deferredGranularity,
        };
    }

    private function with(
        ?string $connectionName = null,
        bool $connectionNameSet = false,
        ?string $deadSuffix = null,
        ?bool $declareDeadLetterQueues = null,
        ?string $deferredSuffix = null,
        ?Duration $retryGranularity = null,
        ?Duration $deferredGranularity = null,
        ReconnectPolicy|false|null $reconnect = null,
        bool $reconnectSet = false,
        ?int $heartbeat = null,
        ?float $connectionTimeout = null,
        ?float $readWriteTimeout = null,
        ?float $publishConfirmTimeout = null,
    ): self {
        return new self(
            connectionName: $connectionNameSet ? $connectionName : $this->connectionName,
            deadSuffix: $deadSuffix ?? $this->deadSuffix,
            declareDeadLetterQueues: $declareDeadLetterQueues ?? $this->declareDeadLetterQueues,
            deferredSuffix: $deferredSuffix ?? $this->deferredSuffix,
            retryGranularity: $retryGranularity ?? $this->retryGranularity,
            deferredGranularity: $deferredGranularity ?? $this->deferredGranularity,
            reconnect: $reconnectSet ? ($reconnect ?? false) : $this->reconnect,
            heartbeat: $heartbeat ?? $this->heartbeat,
            connectionTimeout: $connectionTimeout ?? $this->connectionTimeout,
            readWriteTimeout: $readWriteTimeout ?? $this->readWriteTimeout,
            publishConfirmTimeout: $publishConfirmTimeout ?? $this->publishConfirmTimeout,
        );
    }
}
