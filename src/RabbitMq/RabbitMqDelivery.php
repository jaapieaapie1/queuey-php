<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use Queuey\Delivery;
use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\AlreadySettled;
use Queuey\Exception\BackendException;

/**
 * One message in flight from RabbitMQ.
 *
 * Every settling method that publishes does so **before** it acks the original:
 * if the publish fails the original stays unacked and the broker redelivers it,
 * so a crash duplicates work rather than losing it. A settle that failed
 * therefore does not count as a settle — nothing was removed from the broker.
 */
final class RabbitMqDelivery implements Delivery
{
    private bool $settled = false;

    public function __construct(
        private readonly Envelope $envelope,
        private readonly AMQPChannel $channel,
        private readonly int $deliveryTag,
        private readonly Publisher $publisher,
    ) {}

    public function envelope(): Envelope
    {
        return $this->envelope;
    }

    public function ack(): void
    {
        $this->assertUnsettled('ack');
        $this->channel->basic_ack($this->deliveryTag);
        $this->settled = true;
    }

    public function deadLetter(string $reason): void
    {
        $this->assertUnsettled('deadLetter');

        if (!$this->publisher->options()->declareDeadLetterQueues) {
            // `q.dead` is not this backend's to write to, and a mandatory
            // publish to a queue nobody declared is an error rather than a
            // silent drop. Hand the message back to the broker instead: its own
            // dead-letter policy on `q` applies if the operator configured one,
            // and otherwise the message is discarded.
            $this->publisher->logger()->warning(
                'dead-letter queues are disabled; rejecting the delivery instead of publishing to the '
                . "dead-letter queue (the broker's own dead-letter policy applies, if any, otherwise the job "
                . 'is dropped)',
                ['jobId' => $this->envelope->jobId, 'queue' => $this->envelope->queue, 'reason' => $reason],
            );
            $this->channel->basic_reject($this->deliveryTag, false);
            $this->settled = true;

            return;
        }

        $this->publishThenAck(fn() => $this->publisher->publishDeadLetter($this->envelope, $reason));
    }

    public function retry(Envelope $next, Duration $delay): void
    {
        $this->assertUnsettled('retry');
        $this->publishThenAck(fn() => $this->publisher->publishHeld($next, $delay, Hold::Retry));
    }

    public function defer(Envelope $next, Duration $delay): void
    {
        $this->assertUnsettled('defer');
        $this->publishThenAck(fn() => $this->publisher->publishHeld($next, $delay, Hold::Deferral));
    }

    /**
     * @param callable(): void $publish
     *
     * @throws BackendException
     */
    private function publishThenAck(callable $publish): void
    {
        $publish();
        $this->channel->basic_ack($this->deliveryTag);
        $this->settled = true;
    }

    /**
     * @throws AlreadySettled
     */
    private function assertUnsettled(string $operation): void
    {
        if (!$this->settled) {
            return;
        }

        throw new AlreadySettled(\sprintf(
            'delivery of job `%s` from queue `%s` was already settled; `%s` came too late',
            $this->envelope->jobId,
            $this->envelope->queue,
            $operation,
        ));
    }
}
