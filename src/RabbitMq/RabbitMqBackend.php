<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPIOWaitException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Queuey\Backend;
use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\BackendException;
use Queuey\Exception\ConsumerStopped;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\MalformedEnvelope;
use Queuey\Exception\ShutDown;
use Queuey\QueueConfig;

/**
 * The php-amqplib {@see Backend}.
 *
 * Synchronous throughout: deliveries are dispatched from inside
 * {@see self::wait()} and never before, so a handler always runs on the caller's
 * stack. A dropped connection is a pause rather than a failure — publishes wait,
 * consumers resubscribe, and the worker loop keeps going — unless the configured
 * {@see ReconnectPolicy} says otherwise.
 *
 * See INTEROP-SPEC §§3, 4, 6.5, 6.6 and 8.
 */
final class RabbitMqBackend implements Backend, LoggerAwareInterface
{
    /** Longest single blocking read; short enough that a shutdown flag is noticed. */
    private const POLL_SECONDS = 1.0;

    private static int $consumerSequence = 0;

    private readonly Publisher $publisher;

    private LoggerInterface $logger;

    /**
     * @var array<string, array{config: QueueConfig, onDelivery: callable(\Queuey\Delivery): void, channel: AMQPChannel, tag: string}>
     */
    private array $consumers = [];

    /**
     * Channels of cancelled consumers, kept open until {@see self::close()}.
     *
     * A delivery already in hand is settled on the very channel it arrived on,
     * so {@see self::cancelConsumers()} must not close them — but it must not
     * forget them either, or nothing could ever close them.
     *
     * @var list<AMQPChannel>
     */
    private array $retired = [];

    /** How many deliveries the current {@see self::wait()} has dispatched. */
    private int $dispatched = 0;

    /**
     * Whether the last {@see self::drainOnce()} got anything off the socket,
     * handed over or not.
     *
     * A malformed body is disposed of rather than dispatched, which is progress
     * all the same: frames behind it may already be buffered, so blocking on
     * the socket afterwards would wait for traffic that has already arrived.
     */
    private bool $progressed = false;

    private function __construct(
        private readonly Connection $connection,
        private readonly RabbitMqOptions $options,
        LoggerInterface $logger,
    ) {
        $this->publisher = new Publisher($connection);
        $this->logger = $logger;
    }

    /**
     * Dial `$url` and open the publishing channels.
     *
     * The first connect does not retry: a process that cannot reach its broker
     * at startup should fail rather than limp.
     *
     * `$url`'s query parameters are applied over `$options` first, so one
     * `AMQP_URL` can tune `heartbeat` and `connection_timeout` for a PHP and a
     * Rust process alike; see {@see AmqpUrl}.
     *
     * @throws \Queuey\Exception\InvalidConfiguration when the URL is not one this backend can dial
     * @throws BackendException
     */
    public static function connect(string $url, ?RabbitMqOptions $options = null, ?LoggerInterface $logger = null): self
    {
        $logger ??= new NullLogger();

        $parsed = AmqpUrl::parse($url);
        $options = $parsed->applyTo($options ?? new RabbitMqOptions());
        if ($parsed->channelMax !== null) {
            $logger->debug(
                'ignoring the `channel_max` parameter of the AMQP URL; this backend opens a handful of '
                . 'channels and never more',
                ['channelMax' => $parsed->channelMax],
            );
        }

        $connection = Connection::open($url, $options, $logger);

        $backend = new self($connection, $options, $logger);
        $backend->logger->info('rabbitmq backend connected', [
            'host' => $connection->url()->host,
            'reconnects' => $connection->reconnects(),
        ]);

        return $backend;
    }

    /** Route this backend's logging, and its connection's, to `$logger`. */
    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
        $this->connection->setLogger($logger);
    }

    public function options(): RabbitMqOptions
    {
        return $this->options;
    }

    public function isConnected(): bool
    {
        return $this->connection->isConnected();
    }

    public function deadQueueName(string $queue): string
    {
        return Topology::deadQueueName($queue, $this->options->deadSuffix);
    }

    public function deferredQueueName(string $queue, int $ttlMs): string
    {
        return Topology::deferredQueueName($queue, $this->options->deferredSuffix, $ttlMs);
    }

    public function declare(QueueConfig ...$queues): void
    {
        if ($queues === []) {
            return;
        }

        // Up front, before a single queue exists: a name that leaves no room for
        // its hold queues would otherwise declare fine and then fail one retry
        // at a time, in production, on the day someone picks a long delay.
        foreach ($queues as $config) {
            Topology::checkDeferrableName($config->name, $this->options->deferredSuffix);
        }

        $channel = $this->connection->ensureConnected()->channel();

        try {
            foreach ($queues as $config) {
                try {
                    $this->connection->declareTopology($channel, $config);
                } catch (\Throwable $error) {
                    if (AmqpError::isConnectionLost($error)) {
                        throw $error;
                    }

                    // RabbitMQ refuses to change an existing queue's arguments
                    // and closes the channel saying so. Surface it; never hang.
                    throw new BackendException(
                        \sprintf('declaring queue `%s` failed: %s', $config->name, $error->getMessage()),
                        0,
                        $error,
                    );
                }

                // Only once the declarations landed: a retry or deferral onto
                // this queue now knows whether its hold queue has to be durable
                // and how the returning job will be ordered. This is also the
                // record a reconnect replays.
                $this->connection->remember($config);
                $this->logger->debug('topology declared', ['queue' => $config->name]);
            }
        } finally {
            $this->closeQuietly($channel, 'the declaration channel');
        }
    }

    public function publish(Envelope $envelope, ?Duration $delay = null): void
    {
        if ($delay === null) {
            $this->publisher->publishEnvelope($envelope);

            return;
        }

        $this->publisher->publishHeld($envelope, $delay, Hold::Retry);
    }

    public function defer(Envelope $envelope, Duration $delay): void
    {
        $this->publisher->publishHeld($envelope, $delay, Hold::Deferral);
    }

    /**
     * @throws InvalidConfiguration when `$queue` already has a consumer here
     * @throws BackendException
     */
    public function consume(QueueConfig $queue, callable $onDelivery): void
    {
        // One consumer per queue per backend, as in the Rust worker. Overwriting
        // the entry would leave the previous consumer subscribed on a channel
        // nothing owns any more: it would keep taking messages off the queue,
        // hand them to nobody, and never be cancelled or closed.
        if (isset($this->consumers[$queue->name])) {
            throw new InvalidConfiguration(\sprintf(
                'queue `%s` already has a consumer on this backend; cancel it before consuming again',
                $queue->name,
            ));
        }

        // Deliberately *not* remembered here: a hold queue dead-letters into `q`
        // by name, and INTEROP-SPEC §3.3 makes `declare()` the one thing that
        // says this backend owns that name. Consuming a queue somebody else
        // declared must still refuse to retry onto it.
        [$channel, $tag] = $this->subscribe($queue, $onDelivery);

        $this->consumers[$queue->name] = [
            'config' => $queue,
            'onDelivery' => $onDelivery,
            'channel' => $channel,
            'tag' => $tag,
        ];
    }

    public function wait(?float $timeoutSeconds = null): void
    {
        if ($this->consumers === []) {
            // There is no socket to wait on, so this is a plain sleep. Without
            // it, a worker looping on `wait(null)` between `cancelConsumers()`
            // and `close()` would spin at full speed.
            $sleep = $timeoutSeconds ?? self::POLL_SECONDS;
            if ($sleep > 0) {
                usleep((int) ($sleep * 1_000_000));
            }

            return;
        }

        $deadline = $timeoutSeconds === null ? null : microtime(true) + $timeoutSeconds;
        $this->dispatched = 0;

        while (true) {
            if ($this->connection->isClosing()) {
                return;
            }

            try {
                $this->progressed = false;
                $this->drainOnce();
                if ($this->dispatched > 0) {
                    return;
                }

                $remaining = $deadline === null ? self::POLL_SECONDS : $deadline - microtime(true);
                if ($deadline !== null && $remaining <= 0) {
                    return;
                }

                if ($this->progressed) {
                    // Something came off the socket but was not handed over (a
                    // malformed body). More frames may already be buffered, so
                    // go round again rather than wait for traffic that arrived.
                    continue;
                }

                $this->block(min($remaining, self::POLL_SECONDS));
            } catch (\Throwable $error) {
                // A channel-level refusal is the caller's problem; a lost
                // connection is ours, and the loop picks up where it left off.
                if (!AmqpError::isConnectionLost($error)) {
                    throw $error;
                }
                $this->recoverConsumers($error);
            }
        }
    }

    public function cancelConsumers(): void
    {
        foreach ($this->consumers as $name => $consumer) {
            try {
                if ($consumer['channel']->is_open()) {
                    $consumer['channel']->basic_cancel($consumer['tag'], true, true);
                }
            } catch (\Throwable $error) {
                $this->logger->debug('cancelling a consumer failed', [
                    'queue' => $name,
                    'error' => $error->getMessage(),
                ]);
            }

            // Cancelled, not closed: a delivery the caller is still holding is
            // settled on this very channel, and closing it would requeue the
            // job that is being worked on. Kept here so `close()` can still
            // reach it — forgetting it would leak a channel per cancel.
            $this->retired[] = $consumer['channel'];
        }

        $this->consumers = [];
    }

    public function close(): void
    {
        // Marked first: a consumer that notices the socket going now stops
        // instead of dialling the broker again.
        $this->connection->markClosing();

        foreach ($this->consumers as $consumer) {
            $this->closeQuietly($consumer['channel'], 'a consumer channel');
        }
        $this->consumers = [];

        foreach ($this->retired as $channel) {
            $this->closeQuietly($channel, 'a cancelled consumer channel');
        }
        $this->retired = [];

        $this->publisher->close();
        $this->connection->close();
        $this->logger->info('rabbitmq backend closed');
    }

    /**
     * One non-blocking pass over every consumer channel.
     *
     * Every channel gets its turn on every pass, so one busy queue cannot starve
     * another: php-amqplib parks frames it reads for a different channel on that
     * channel's own queue, and the next pass picks them up.
     *
     * A channel the *broker* closed — its queue was deleted, or it refused
     * something on it — is rebuilt here rather than waited on. php-amqplib nulls
     * a closed channel's connection reference, so `wait()` on one would raise a
     * bare `\Error` from a call on null instead of anything catchable.
     *
     * @throws ConsumerStopped when a consumer cannot be put back
     */
    private function drainOnce(): void
    {
        foreach ($this->consumers as $name => $consumer) {
            $channel = $consumer['channel'];

            if (!$channel->is_open()) {
                $this->recoverConsumer($name, null);

                continue;
            }

            try {
                $channel->wait(null, true);
            } catch (\Throwable $error) {
                // Only the two failures that mean "this consumer is gone but
                // the connection is fine" are handled here. Everything else —
                // a lost connection, and anything the delivery callback threw
                // from inside `wait()` — belongs to the caller.
                if (!AmqpError::isConsumerGone($error)) {
                    throw $error;
                }

                $this->recoverConsumer($name, $error);
            }
        }
    }

    /**
     * Block on the socket until something arrives or `$seconds` pass.
     *
     * Both exits are "nothing arrived": a timeout, and a signal interrupting
     * the `stream_select`, which php-amqplib reports as an
     * {@see AMQPIOWaitException}. The signal case is the one that matters — a
     * worker's `SIGTERM` handler runs through `pcntl_signal_dispatch()`, and
     * letting the exception out would kill the loop before the shutdown flag
     * it was about to set could be read.
     *
     * php-amqplib flags the connection as disconnected on its way out of that
     * one, so the next publish dials again although the socket was never
     * broken. A needless reconnect is a far better outcome than a worker that
     * dies on a signal.
     */
    private function block(float $seconds): void
    {
        $seconds = max($seconds, 0.0);
        $whole = (int) $seconds;
        $micro = (int) round(($seconds - $whole) * 1_000_000);

        try {
            $this->connection->raw()->select($whole, $micro);
            $this->connection->raw()->checkHeartBeat();
        } catch (AMQPTimeoutException) {
            // Nothing arrived; the caller's deadline decides what happens next.
        } catch (AMQPIOWaitException $error) {
            // Interrupted, most likely by the very signal the worker installed
            // a handler for. php-amqplib dispatches pending signals itself only
            // when the select returns normally, so do it here.
            if (\function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
            $this->logger->debug('the wait for new deliveries was interrupted', [
                'error' => $error->getMessage(),
            ]);
        }
    }

    /**
     * Rebuild the connection and put every consumer back on it.
     *
     * @throws ConsumerStopped when reconnection is disabled or the policy gave up
     */
    private function recoverConsumers(\Throwable $cause): void
    {
        if ($this->connection->isClosing()) {
            throw ShutDown::create();
        }

        $consumers = $this->consumers;

        if (!$this->connection->reconnects()) {
            $this->logger->info('the consumers stopped and reconnection is disabled', [
                'queues' => array_keys($consumers),
                'error' => $cause->getMessage(),
            ]);

            throw new ConsumerStopped(\sprintf(
                'consumers for queues %s stopped: %s',
                self::queueList($consumers),
                $cause->getMessage(),
            ), 0, $cause);
        }

        $this->logger->warning('the connection to rabbitmq went away; rebuilding it and the consumers', [
            'error' => $cause->getMessage(),
            'queues' => array_keys($consumers),
        ]);

        try {
            $this->connection->ensureConnected();
        } catch (ShutDown $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw new ConsumerStopped(\sprintf(
                'consumers for queues %s stopped: %s',
                self::queueList($consumers),
                $error->getMessage(),
            ), 0, $error);
        }

        // The publishing channels belonged to the dead connection.
        $this->publisher->forgetChannels();

        // The consumer channels went with it too, so they are not closed here:
        // a `channel.close` would only be another write to a dead socket. The
        // per-consumer path below is the one that has a live channel to tidy up.
        $policy = $this->connection->policy();
        foreach ($consumers as $name => $consumer) {
            $this->consumers[$name] = $this->resubscribe($consumer, $policy);
        }
    }

    /**
     * Put one consumer back after the *broker* closed its channel.
     *
     * The connection is fine here — a deleted queue, or a channel-level refusal,
     * takes down exactly one channel — so only that consumer is rebuilt, paced
     * by the same {@see ReconnectPolicy} a reconnect uses. Without that pacing a
     * consumer whose queue is gone for good would resubscribe as fast as the
     * broker can refuse.
     *
     * @throws ConsumerStopped when reconnection is disabled or the policy gave up
     * @throws ShutDown        when the backend is closing
     */
    private function recoverConsumer(string $name, ?\Throwable $cause): void
    {
        if ($this->connection->isClosing()) {
            throw ShutDown::create();
        }

        $consumer = $this->consumers[$name];
        $this->logger->warning('a consumer channel was closed by the broker; resubscribing', [
            'queue' => $name,
            'error' => $cause?->getMessage(),
        ]);

        // A broker-side `basic.cancel` (its queue was deleted) leaves the
        // channel open with no consumer on it; a refusal already closed it.
        $this->closeQuietly($consumer['channel'], 'a stopped consumer channel');

        if (!$this->connection->reconnects()) {
            $this->logger->info('a consumer stopped and reconnection is disabled', ['queue' => $name]);

            throw ConsumerStopped::forQueue($name, $cause);
        }

        $this->consumers[$name] = $this->resubscribe($consumer, $this->connection->policy());
    }

    /**
     * @param array<string, mixed> $consumers
     */
    private static function queueList(array $consumers): string
    {
        return $consumers === [] ? '<none>' : implode(', ', array_keys($consumers));
    }

    /**
     * @param array{config: QueueConfig, onDelivery: callable(\Queuey\Delivery): void, channel: AMQPChannel, tag: string} $consumer
     *
     * @return array{config: QueueConfig, onDelivery: callable(\Queuey\Delivery): void, channel: AMQPChannel, tag: string}
     *
     * @throws ConsumerStopped
     */
    private function resubscribe(array $consumer, ?ReconnectPolicy $policy): array
    {
        $failures = 0;

        while (true) {
            try {
                [$channel, $tag] = $this->subscribe($consumer['config'], $consumer['onDelivery']);
                $consumer['channel'] = $channel;
                $consumer['tag'] = $tag;

                return $consumer;
            } catch (\Throwable $error) {
                ++$failures;
                $delay = $policy?->nextDelay(Attempt::after(Rebuilding::Consumer, $failures, $error));
                if ($delay === null) {
                    $this->logger->error('giving up on resubscribing a consumer', [
                        'queue' => $consumer['config']->name,
                        'attempts' => $failures,
                        'error' => $error->getMessage(),
                    ]);

                    throw new ConsumerStopped(\sprintf(
                        'consumer for queue `%s` stopped unexpectedly: %s',
                        $consumer['config']->name,
                        $error->getMessage(),
                    ), 0, $error);
                }

                $this->logger->warning('resubscribing a consumer failed; will try again', [
                    'queue' => $consumer['config']->name,
                    'failures' => $failures,
                    'delayMs' => $delay->toMs(),
                    'error' => $error->getMessage(),
                ]);
                if (!$delay->isZero()) {
                    usleep($delay->toMs() * 1000);
                }
                $this->connection->ensureConnected();
            }
        }
    }

    /**
     * Open a channel for `$queue` and start consuming on it.
     *
     * @param callable(\Queuey\Delivery): void $onDelivery
     *
     * @return array{0: AMQPChannel, 1: string}
     *
     * @throws BackendException
     */
    private function subscribe(QueueConfig $queue, callable $onDelivery): array
    {
        $channel = $this->connection->ensureConnected()->channel();
        $channel->basic_qos(0, $queue->prefetch, false);

        $tag = self::consumerTag($queue->name);
        $channel->basic_consume(
            $queue->name,
            $tag,
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($queue, $onDelivery): void {
                $this->dispatch($queue, $message, $onDelivery);
            },
        );

        $this->logger->info('consuming', [
            'queue' => $queue->name,
            'prefetch' => $queue->prefetch,
            'tag' => $tag,
        ]);

        return [$channel, $tag];
    }

    /**
     * @param callable(\Queuey\Delivery): void $onDelivery
     */
    private function dispatch(QueueConfig $queue, AMQPMessage $message, callable $onDelivery): void
    {
        $channel = $message->getChannel();
        if ($channel === null) {
            throw new BackendException('a delivery arrived without a channel to settle it on');
        }
        $tag = (int) $message->getDeliveryTag();
        // Progress, whether or not this one can be handed over: it came off the
        // socket, so the wait loop must not go back to blocking on it.
        $this->progressed = true;

        try {
            $envelope = Envelope::fromJson($message->getBody());
        } catch (MalformedEnvelope $error) {
            $this->logger->warning('dropping a message whose body is not a valid envelope', [
                'queue' => $queue->name,
                'deliveryTag' => $tag,
                'bytes' => \strlen($message->getBody()),
                'error' => $error->getMessage(),
            ]);
            $this->discardMalformed($queue->name, $message, $channel, $tag);

            return;
        }

        ++$this->dispatched;
        $onDelivery(new RabbitMqDelivery($envelope, $channel, $tag, $this->publisher));
    }

    /**
     * INTEROP-SPEC §6.5: forward the raw body to `q.dead` and ack, or reject it
     * when that publish fails or dead-letter queues are disabled.
     */
    private function discardMalformed(string $queue, AMQPMessage $message, AMQPChannel $channel, int $tag): void
    {
        if ($this->options->declareDeadLetterQueues) {
            try {
                $this->publisher->publishMalformed($queue, $message->getBody());
                $channel->basic_ack($tag);

                return;
            } catch (\Throwable $error) {
                $this->logger->warning('forwarding a malformed message to the dead-letter queue failed', [
                    'queue' => $queue,
                    'error' => $error->getMessage(),
                ]);
            }
        }

        try {
            $channel->basic_reject($tag, false);
        } catch (\Throwable $error) {
            $this->logger->error(
                'rejecting a malformed message failed; it stays unacknowledged and holds a prefetch slot '
                . 'until the consumer channel closes',
                ['queue' => $queue, 'deliveryTag' => $tag, 'error' => $error->getMessage()],
            );
        }
    }

    private function closeQuietly(AMQPChannel $channel, string $what): void
    {
        if (!$channel->is_open()) {
            return;
        }

        try {
            $channel->close();
        } catch (\Throwable $error) {
            $this->logger->debug(\sprintf('closing %s failed', $what), ['error' => $error->getMessage()]);
        }
    }

    /**
     * INTEROP-SPEC §6.6: `queuey-php.<queue>.<hex nanos>.<seq>`.
     *
     * Wall-clock nanoseconds, like Rust's `SystemTime`, rather than the
     * process-relative `hrtime()`: two workers started at once would otherwise
     * produce near-identical tags, which is exactly when a readable tag in the
     * management UI is worth having. The pid is mixed in for the same reason —
     * `microtime()` resolves to microseconds, so its last three hex digits are
     * always zero and add nothing.
     */
    private static function consumerTag(string $queue): string
    {
        $sequence = self::$consumerSequence++;
        $nanos = (int) (microtime(true) * 1e9);
        $pid = getmypid();

        return \sprintf(
            'queuey-php.%s.%x.%d',
            Codec::truncateAtBoundary($queue, Topology::CONSUMER_TAG_QUEUE_LIMIT),
            $pid === false ? $nanos : $nanos ^ ($pid << 40),
            $sequence,
        );
    }
}
