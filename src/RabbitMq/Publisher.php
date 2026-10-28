<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\BackendException;
use Queuey\Exception\UnknownQueue;
use Queuey\QueueConfig;

/**
 * The shared publishing channels: one in confirm mode for publishes, one plain
 * channel for the on-demand hold-queue declarations.
 *
 * The split is not tidiness. A refused declaration closes the channel it ran
 * on, and that must never be the channel every publish shares: one hold queue
 * somebody pre-created with foreign arguments would otherwise take the whole
 * backend's publishing down with it.
 *
 * Exactly one publish is ever in flight on the confirm channel. A `basic.return`
 * cannot be attributed otherwise, and misattributing one means acking an
 * original whose successor went nowhere.
 *
 * **A publish is attempted exactly once.** A connection known to be dead is
 * repaired before anything is written, but a drop *during* a publish or while
 * waiting for its confirm is reported, never retried: the broker may already
 * hold the message, and a second publish would duplicate the job. The caller
 * still holds an unacked original, and its redelivery is what retries the work.
 * The Rust reference behaves the same way; see INTEROP-SPEC §4.
 *
 * Every channel this class abandons is closed first (see
 * {@see self::closeQuietly()}). A channel the client merely forgets stays open
 * on the broker, keeps accumulating frames, and counts against `channel_max`.
 *
 * Internal to the RabbitMQ backend; not part of the public API.
 */
final class Publisher
{
    private ?AMQPChannel $confirmChannel = null;

    private ?AMQPChannel $declareChannel = null;

    /** What the return listener or nack handler saw for the publish in flight. */
    private ?ConfirmOutcome $outcome = null;

    public function __construct(private readonly Connection $connection) {}

    public function options(): RabbitMqOptions
    {
        return $this->connection->options();
    }

    public function deadQueue(string $queue): string
    {
        return Topology::deadQueueName($queue, $this->options()->deadSuffix);
    }

    public function deferredQueue(string $queue, int $ttlMs): string
    {
        return Topology::deferredQueueName($queue, $this->options()->deferredSuffix, $ttlMs);
    }

    /**
     * Publish `$envelope` straight onto its own queue.
     *
     * @throws BackendException
     */
    public function publishEnvelope(Envelope $envelope): void
    {
        $this->publishConfirmed($envelope->queue, $envelope->toJson(), Codec::propsFor($envelope));
    }

    /**
     * Hold `$envelope` for `$delay` before it reappears on its queue.
     *
     * The hold queue is redeclared immediately before every publish: the
     * redeclare is idempotent and it resets the `x-expires` idle timer, so the
     * queue cannot vanish between the declare and the publish.
     *
     * @throws UnknownQueue     when the target queue was never declared through this backend
     * @throws BackendException
     */
    public function publishHeld(Envelope $envelope, Duration $delay, Hold $hold): void
    {
        $config = $this->connection->configFor($envelope->queue);
        if ($config === null) {
            // The hold queue dead-letters into `q` by name, so publishing into
            // one for a queue we never declared would strand the message.
            throw UnknownQueue::named($envelope->queue);
        }

        $ttlMs = Topology::deferredTtlMs($delay, $hold->granularity($this->options()));
        $holdQueue = $this->deferredQueue($envelope->queue, $ttlMs);

        // Before the declare, not just before the publish: the declare is what
        // resets the hold queue's `x-expires` timer (§3.3), and a declare that
        // landed on a connection that is already gone would leave the publish
        // to declare nothing at all.
        $this->connection->ensureConnected();

        $this->declareHold($holdQueue, $config, $ttlMs);

        $this->logger()->debug($hold->logMessage(), [
            'queue' => $envelope->queue,
            'hold' => $holdQueue,
            'ttlMs' => $ttlMs,
            'attempt' => $envelope->attempt,
            'deferrals' => $envelope->deferrals,
            'priority' => $envelope->priority,
        ]);

        $this->publishConfirmed($holdQueue, $envelope->toJson(), Codec::propsFor($envelope));
    }

    /**
     * @throws BackendException
     */
    public function publishDeadLetter(Envelope $envelope, string $reason): void
    {
        $this->publishConfirmed(
            $this->deadQueue($envelope->queue),
            $envelope->toJson(),
            Codec::deadLetterProps($envelope, $reason),
        );
    }

    /**
     * Forward a body that is not a valid envelope, verbatim.
     *
     * @throws BackendException
     */
    public function publishMalformed(string $queue, string $body, string $reason = Codec::REASON_MALFORMED): void
    {
        $this->publishConfirmed($this->deadQueue($queue), $body, Codec::malformedProps($queue, $reason));
    }

    /**
     * One mandatory, confirmed publish onto the default exchange.
     *
     * A publish is complete only when the broker acks it. A `basic.return`
     * (nobody declared the queue) or a `basic.nack` is an error for that
     * publish, never a silent drop.
     *
     * Attempted **once**. A connection already known to be dead is rebuilt
     * first, because failing there means nothing was written; but a drop
     * between the write and the confirm is an error, not a reason to publish
     * again. The broker may have taken the message, so a second attempt risks
     * a duplicate job, while doing nothing costs nothing: whatever asked for
     * this publish still holds an unacked original, and the redelivery of that
     * original is the retry.
     *
     * @param array<string, mixed> $props
     *
     * @throws \Queuey\Exception\PublishReturned when the broker could not route the message
     * @throws \Queuey\Exception\PublishNacked   when the broker refused responsibility for it
     * @throws \Queuey\Exception\ConnectionLost  when the connection went away before or during the publish
     * @throws BackendException
     */
    public function publishConfirmed(string $routingKey, string $body, array $props): void
    {
        // Repairing a connection that is already gone costs nothing: no bytes
        // have been written yet, so there is no message to duplicate.
        $this->connection->ensureConnected();

        $this->publishOnce($routingKey, $body, $props);
    }

    public function close(): void
    {
        $this->closeQuietly($this->confirmChannel, 'the publishing channel');
        $this->closeQuietly($this->declareChannel, 'the declaration channel');

        $this->confirmChannel = null;
        $this->declareChannel = null;
    }

    /**
     * Forget the cached channels; the next publish opens fresh ones.
     *
     * Deliberately *not* closing them: this is called when the connection they
     * lived on died, so they are gone with it and a `channel.close` would only
     * be another write to a dead socket.
     */
    public function forgetChannels(): void
    {
        $this->confirmChannel = null;
        $this->declareChannel = null;
    }

    /**
     * @param array<string, mixed> $props
     *
     * @throws BackendException
     */
    private function publishOnce(string $routingKey, string $body, array $props): void
    {
        $channel = $this->confirmChannel();
        $this->takeOutcome();

        $channel->basic_publish(new AMQPMessage($body, $props), '', $routingKey, true);

        try {
            $channel->wait_for_pending_acks_returns($this->options()->publishConfirmTimeout);
        } catch (\Throwable $error) {
            // A timeout leaves the channel with an unresolved confirm on it, so
            // it is no longer usable under the "one publish in flight" rule.
            // Closed, not just dropped: the broker would otherwise keep it open
            // and keep queueing this publish's confirm frames on it forever.
            $this->closeQuietly($this->confirmChannel, 'a publishing channel with an unresolved confirm');
            $this->confirmChannel = null;
            if (AmqpError::isConnectionLost($error) || $error instanceof BackendException) {
                throw $error;
            }

            throw BackendException::because($error);
        }

        $outcome = $this->takeOutcome();
        if ($outcome !== null) {
            throw $outcome->toException($routingKey);
        }

        $this->logger()->debug('publish confirmed', ['queue' => $routingKey, 'bytes' => \strlen($body)]);
    }

    /**
     * Read and clear what the listeners recorded for the publish in flight.
     *
     * Impure on purpose: the flags belong to *one* publish, and leaving them
     * behind would attribute this publish's return to the next one.
     *
     * @phpstan-impure
     */
    private function takeOutcome(): ?ConfirmOutcome
    {
        $outcome = $this->outcome;
        $this->outcome = null;

        return $outcome;
    }

    /**
     * Declare the hold queue on its own channel.
     *
     * @throws BackendException
     */
    private function declareHold(string $holdQueue, QueueConfig $config, int $ttlMs): void
    {
        $channel = $this->declareChannel();

        try {
            $channel->queue_declare(
                $holdQueue,
                false,
                $config->durable,
                false,
                false,
                false,
                Topology::deferredQueueArgs($config, $ttlMs),
            );
        } catch (\Throwable $error) {
            // Whatever the broker objected to, it usually closed this channel
            // doing so — but a timeout or a client-side refusal leaves it open,
            // and an abandoned open channel is a leak. Close it either way; the
            // next held publish opens a replacement.
            $this->closeQuietly($this->declareChannel, 'a declaration channel that failed');
            $this->declareChannel = null;
            if (AmqpError::isConnectionLost($error)) {
                throw $error;
            }

            throw new BackendException(
                \sprintf('declaring the hold queue `%s` failed: %s', $holdQueue, $error->getMessage()),
                0,
                $error,
            );
        }
    }

    /**
     * @throws BackendException
     */
    private function confirmChannel(): AMQPChannel
    {
        if ($this->confirmChannel !== null && $this->confirmChannel->is_open()) {
            return $this->confirmChannel;
        }

        if ($this->confirmChannel !== null) {
            $this->logger()->warning('publishing channel is closed; opening a replacement in confirm mode');
            $this->closeQuietly($this->confirmChannel, 'the previous publishing channel');
            $this->confirmChannel = null;
        }

        $channel = $this->openChannel();
        $channel->confirm_select();
        $channel->set_return_listener(
            function (int $replyCode, string $replyText, string $exchange, string $routingKey): void {
                $this->outcome = ConfirmOutcome::returned(\sprintf('%d %s', $replyCode, $replyText), $routingKey);
            },
        );
        $channel->set_nack_handler(function (): void {
            $this->outcome = ConfirmOutcome::nacked();
        });

        return $this->confirmChannel = $channel;
    }

    /**
     * @throws BackendException
     */
    private function declareChannel(): AMQPChannel
    {
        if ($this->declareChannel !== null && $this->declareChannel->is_open()) {
            return $this->declareChannel;
        }

        if ($this->declareChannel !== null) {
            $this->logger()->warning('declaration channel is closed; opening a replacement');
            $this->closeQuietly($this->declareChannel, 'the previous declaration channel');
            $this->declareChannel = null;
        }

        return $this->declareChannel = $this->openChannel();
    }

    /**
     * A channel on a connection that is demonstrably alive.
     *
     * php-amqplib learns that the peer hung up only when it next writes, so the
     * first channel opened after an outage is usually where the drop is
     * discovered: the socket looked fine, `channel.open` went out, and the
     * broker's `connection.close` came back instead of `channel.open-ok`. That
     * is the moment lapin would have skipped entirely, because its IO loop
     * already knew, so rebuilding and asking again here is the equivalent of
     * lapin handing out a channel on a reconnected connection.
     *
     * This is emphatically *not* a second publish: not a byte of any message
     * has been written at this point. See {@see self::publishConfirmed()} for
     * where retrying stops.
     *
     * @throws BackendException
     */
    private function openChannel(): AMQPChannel
    {
        try {
            return $this->connection->ensureConnected()->channel();
        } catch (\Throwable $error) {
            if (!AmqpError::isConnectionLost($error)) {
                throw $error;
            }

            $this->logger()->warning('the connection turned out to be gone while opening a channel; rebuilding it', [
                'error' => $error->getMessage(),
            ]);

            // The failed attempt is what told php-amqplib the connection is
            // dead, so this `ensureConnected` dials a new one rather than
            // handing back the same corpse.
            return $this->connection->ensureConnected()->channel();
        }
    }

    /**
     * Close `$channel` if it is still open, swallowing whatever it says.
     *
     * Every channel this class stops using goes through here. A channel the
     * client merely forgets is still open on the *broker*: it keeps its
     * consumers and unresolved confirms, php-amqplib keeps parking its frames
     * on it, and it counts against the connection's `channel_max`. A
     * long-running worker that abandoned one per timed-out confirm would
     * eventually be unable to open another.
     *
     * Failing to close it is not worth reporting: the channel is being
     * discarded either way.
     */
    private function closeQuietly(?AMQPChannel $channel, string $what): void
    {
        if ($channel === null || !$channel->is_open()) {
            return;
        }

        try {
            $channel->close();
        } catch (\Throwable $error) {
            $this->logger()->debug(\sprintf('closing %s failed', $what), ['error' => $error->getMessage()]);
        }
    }

    public function logger(): LoggerInterface
    {
        return $this->connection->logger();
    }
}
