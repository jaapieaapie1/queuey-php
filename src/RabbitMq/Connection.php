<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Queuey\Exception\BackendException;
use Queuey\Exception\ConnectionLost;
use Queuey\Exception\ShutDown;
use Queuey\QueueConfig;

/**
 * The reconnecting connection every channel in this backend is opened on.
 *
 * A php-amqplib connection is dead once the socket goes: channels on it fail,
 * consumers on it stop, and nothing recovers by itself. This holds one behind a
 * replaceable slot, so the rest of the backend asks for "the connection" and
 * gets a live one, dialling again first if it has to.
 *
 * Three properties the rest of the backend relies on:
 *
 * * **Reconnection is single-flight.** PHP is synchronous, so this is a
 *   re-entrancy guard rather than a lock, but the effect is the same: a
 *   publish that fails *during* a reconnect does not start a second one.
 * * **A generation counts successful connections.** A consumer that subscribed
 *   at generation 4 can tell "the connection I was using is the one that just
 *   died" from "somebody has already replaced it".
 * * **A closing handle never reconnects.** {@see self::markClosing()} is final,
 *   so shutting a backend down actually shuts it down.
 *
 * Internal to the RabbitMQ backend; not part of the public API.
 */
final class Connection
{
    private const REPLY_SUCCESS = 200;

    private AbstractConnection $current;

    private int $generation = 1;

    private bool $closing = false;

    private bool $reconnecting = false;

    /** @var array<string, QueueConfig> every config ever declared, replayed after a reconnect */
    private array $configs = [];

    private LoggerInterface $logger;

    private function __construct(
        private readonly AmqpUrl $url,
        private readonly RabbitMqOptions $options,
        AbstractConnection $connection,
        LoggerInterface $logger,
    ) {
        $this->current = $connection;
        $this->logger = $logger;
    }

    /**
     * Dial `$url` once.
     *
     * The first connect deliberately does not retry: a process that cannot
     * reach its broker at startup should fail rather than limp.
     *
     * @throws BackendException
     */
    public static function open(string $url, RabbitMqOptions $options, ?LoggerInterface $logger = null): self
    {
        $parsed = AmqpUrl::parse($url);
        $logger ??= new NullLogger();

        return new self($parsed, $options, self::dial($parsed, $options), $logger);
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    public function logger(): LoggerInterface
    {
        return $this->logger;
    }

    public function options(): RabbitMqOptions
    {
        return $this->options;
    }

    /** The parsed URL this connection dials. */
    public function url(): AmqpUrl
    {
        return $this->url;
    }

    /** Whether a dropped connection would be rebuilt rather than surfaced. */
    public function reconnects(): bool
    {
        return $this->options->reconnect !== false;
    }

    public function policy(): ?ReconnectPolicy
    {
        return $this->options->reconnect === false ? null : $this->options->reconnect;
    }

    public function isConnected(): bool
    {
        return !$this->closing && $this->current->isConnected();
    }

    public function isClosing(): bool
    {
        return $this->closing;
    }

    /** How many connections have been established, the first included. */
    public function generation(): int
    {
        return $this->generation;
    }

    /** The connection as it stands, live or not. */
    public function raw(): AbstractConnection
    {
        return $this->current;
    }

    /**
     * Remember `$config` so a reconnect can put its queues back.
     *
     * A restarted broker lost every non-durable queue, and an operator who
     * deleted `q.dead` by hand lost that too.
     */
    public function remember(QueueConfig $config): void
    {
        $this->configs[$config->name] = $config;
    }

    public function configFor(string $queue): ?QueueConfig
    {
        return $this->configs[$queue] ?? null;
    }

    /**
     * A live connection, reconnecting first if the current one is gone.
     *
     * @throws ShutDown       when the backend is closing
     * @throws ConnectionLost when reconnection is disabled
     * @throws BackendException when the policy declined another attempt
     */
    public function ensureConnected(): AbstractConnection
    {
        $this->checkOpen();
        if ($this->current->isConnected()) {
            return $this->current;
        }

        if ($this->reconnecting) {
            // Reached from inside a reconnect (a logger, a policy). Whatever we
            // have is what there is; starting a second dial here would be a
            // recursion, not a recovery.
            throw new ConnectionLost('the connection to rabbitmq is being rebuilt');
        }

        $policy = $this->policy();
        if ($policy === null) {
            throw new ConnectionLost('the connection to rabbitmq went away and reconnection is disabled');
        }

        $this->reconnecting = true;

        try {
            return $this->reconnect($policy);
        } finally {
            $this->reconnecting = false;
        }
    }

    /** Stop reconnecting. Called before anything is actually torn down. */
    public function markClosing(): void
    {
        $this->closing = true;
    }

    public function close(): void
    {
        $this->markClosing();
        if (!$this->current->isConnected()) {
            return;
        }

        try {
            $this->current->close(self::REPLY_SUCCESS, 'OK');
        } catch (\Throwable $error) {
            $this->logger->debug('closing the rabbitmq connection failed', ['error' => $error->getMessage()]);
        }
    }

    /**
     * Declare `$config`'s main queue and, unless disabled, its dead-letter queue.
     *
     * Shared by `declare()` and by the post-reconnect replay so the two can never
     * disagree about the flags and provoke a `PRECONDITION_FAILED`.
     */
    public function declareTopology(AMQPChannel $channel, QueueConfig $config): void
    {
        $channel->queue_declare(
            $config->name,
            false,
            $config->durable,
            false,
            false,
            false,
            Topology::queueArgs($config),
        );

        if ($this->options->declareDeadLetterQueues) {
            $channel->queue_declare(
                Topology::deadQueueName($config->name, $this->options->deadSuffix),
                false,
                // Dead-lettered jobs outlive broker restarts by design: they are
                // the record of what went wrong.
                true,
                false,
                false,
                false,
                Topology::deadQueueArgs(),
            );
        }
    }

    /**
     * @throws BackendException
     */
    private function reconnect(ReconnectPolicy $policy): AbstractConnection
    {
        $failures = 0;
        $last = null;

        while (true) {
            $attempt = $last === null
                ? Attempt::first(Rebuilding::Connection)
                : Attempt::after(Rebuilding::Connection, $failures, $last);

            $delay = $policy->nextDelay($attempt);
            if ($delay === null) {
                $this->logger->error('giving up on reconnecting to rabbitmq; the policy declined another attempt', [
                    'attempts' => $failures,
                    'error' => $last?->getMessage(),
                ]);

                throw new ConnectionLost(
                    \sprintf('could not reconnect to rabbitmq after %d attempts', $failures),
                    0,
                    $last,
                );
            }

            if (!$delay->isZero()) {
                $this->logger->debug('waiting before the next reconnect attempt', [
                    'delayMs' => $delay->toMs(),
                    'failures' => $failures,
                ]);
                usleep($delay->toMs() * 1000);
            }
            $this->checkOpen();

            try {
                $connection = self::dial($this->url, $this->options);
            } catch (BackendException $error) {
                ++$failures;
                $this->logger->warning('reconnecting to rabbitmq failed', [
                    'failures' => $failures,
                    'error' => $error->getMessage(),
                ]);
                $last = $error;
                continue;
            }

            $this->current = $connection;
            ++$this->generation;
            $this->logger->info('reconnected to rabbitmq', [
                'generation' => $this->generation,
                'attempts' => $failures + 1,
            ]);
            $this->redeclare($connection);

            return $connection;
        }
    }

    /**
     * Put every remembered queue back on a freshly dialled connection.
     *
     * Best-effort by design: a `PRECONDITION_FAILED` here is logged and the
     * connection kept, because losing the connection over a topology
     * disagreement would turn a recoverable outage into an outage.
     */
    private function redeclare(AbstractConnection $connection): void
    {
        if ($this->configs === []) {
            return;
        }

        try {
            $channel = $connection->channel();
        } catch (\Throwable $error) {
            $this->logger->error(
                'could not open a channel to re-declare the topology after reconnecting; queues lost to a '
                . 'broker restart stay missing until something declares them',
                ['queues' => \count($this->configs), 'error' => $error->getMessage()],
            );

            return;
        }

        $declared = 0;
        foreach ($this->configs as $config) {
            try {
                $this->declareTopology($channel, $config);
            } catch (\Throwable $error) {
                $this->logger->error(
                    're-declaring a queue after reconnecting failed; operations on it will fail until it is '
                    . 'declared successfully',
                    [
                        'queue' => $config->name,
                        'declared' => $declared,
                        'remaining' => \count($this->configs) - $declared,
                        'error' => $error->getMessage(),
                    ],
                );
                // A refused declaration closes the channel it ran on, so the rest
                // of the replay would fail on the dead channel rather than on its
                // own merits. Stop; the next reconnect or an explicit declare can
                // try again.
                break;
            }
            ++$declared;
        }

        if ($declared === \count($this->configs)) {
            $this->logger->debug('topology re-declared after reconnect', ['queues' => $declared]);
        }

        if ($channel->is_open()) {
            try {
                $channel->close();
            } catch (\Throwable $error) {
                $this->logger->debug('closing the re-declaration channel failed', ['error' => $error->getMessage()]);
            }
        }
    }

    /**
     * @throws BackendException
     */
    private static function dial(AmqpUrl $url, RabbitMqOptions $options): AbstractConnection
    {
        $config = new AMQPConnectionConfig();
        $config->setHost($url->host);
        $config->setPort($url->port);
        $config->setUser($url->user);
        $config->setPassword($url->password);
        $config->setVhost($url->vhost);
        $config->setIsSecure($url->secure);
        $config->setHeartbeat($options->heartbeat);
        $config->setConnectionTimeout($options->connectionTimeout);
        $config->setReadTimeout($options->readWriteTimeout);
        $config->setWriteTimeout($options->readWriteTimeout);
        $config->setChannelRPCTimeout($options->readWriteTimeout);
        if ($options->connectionName !== null) {
            $config->setConnectionName($options->connectionName);
        }

        try {
            return AMQPConnectionFactory::create($config);
        } catch (\Throwable $error) {
            throw BackendException::because($error);
        }
    }

    /**
     * @throws ShutDown
     */
    private function checkOpen(): void
    {
        if ($this->closing) {
            throw new ShutDown('the rabbitmq backend is closed');
        }
    }
}
