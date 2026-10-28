<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Envelope;
use Queuey\Exception\AlreadySettled;
use Queuey\Exception\BackendException;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\PublishReturned;
use Queuey\QueueConfig;
use Queuey\RabbitMq\RabbitMqOptions;
use Queuey\RabbitMq\Topology;
use Queuey\Tests\Fixtures\RecordingLogger;
use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

it('carries an envelope through the round trip with its properties and headers', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('roundtrip');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $envelope = $this->envelope($queue, 3)->withCorrelationId('trace-abc');
    $backend->publish($envelope);

    // First look at the raw AMQP message, which is what the Rust side reads.
    $raw = $this->awaitRaw($queue);
    expect($raw->get('content_type'))->toBe('application/json');
    expect($raw->get('delivery_mode'))->toBe(2);
    expect($raw->get('message_id'))->toBe($envelope->jobId);
    expect($raw->get('type'))->toBe($envelope->jobType);
    expect($raw->get('priority'))->toBe(0);
    expect($raw->has('expiration'))->toBeFalse('the hold queue times the wait, never the message');
    $this->assertHeaders([Topology::HEADER_ATTEMPT => 3, Topology::HEADER_DEFERRALS => 0], $raw);
    expect($raw->getBody())->toBe($envelope->toJson());

    // Then through the backend, which must decode it back to the same envelope.
    $backend->publish($envelope);
    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);

    expect($delivery->envelope())->toEqual($envelope);
    expect($delivery->envelope()->correlationId)->toBe('trace-abc');
    $delivery->ack();
    $this->awaitCount($queue, 0);
});

it('settles a delivery only once', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('settle-once');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $backend->publish($this->envelope($queue));

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);
    $delivery->ack();

    expect(static function () use ($delivery): void {
        $delivery->ack();
    })->toThrow(AlreadySettled::class);
});

it('limits how much the broker hands over with prefetch', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('prefetch');
    $backend = $this->backend();
    $config = QueueConfig::new($queue)->withPrefetch(1);
    $backend->declare($config);

    $backend->publish($this->envelope($queue, 1));
    $backend->publish($this->envelope($queue, 2));
    $this->awaitCount($queue, 2);

    $this->consumeInto($backend, $config);
    $first = $this->nextDelivery($backend);

    // With prefetch 1 the second message stays on the broker until the first
    // is acked, so nothing else can arrive here.
    $this->expectIdle($backend, 0.5);

    $first->ack();
    $second = $this->nextDelivery($backend);
    expect($second->envelope()->attempt)->toBe(2);
    $second->ack();
});

it('returns a publish to a queue nobody declared rather than losing it', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('unroutable');
    $backend = $this->backend();

    // No declare at all: the mandatory publish has nowhere to go.
    try {
        $backend->publish($this->envelope($queue));
        Assert::fail('an unroutable publish must be an error, not a silent drop');
    } catch (PublishReturned $error) {
        expect($error->getMessage())->toContain($queue);
    }

    // And the publishing channel survives it, so the next publish works.
    $backend->declare(QueueConfig::new($queue));
    $backend->publish($this->envelope($queue));
    $this->awaitCount($queue, 1);
});

it('fails only the unroutable publish of three', function (): void {
    /** @var RabbitMqTestCase $this */
    // The failure has to cost exactly the publish that caused it. A return
    // that took the confirm channel down with it, or one attributed to the
    // next publish, would show up here as a second failure or as a message
    // that never arrived.
    $queue = $this->uniqueQueue('unroutable-middle');
    $missing = $queue . '.nobody-declared-this';
    $this->registerQueue($missing);

    $backend = $this->backend();
    $backend->declare(QueueConfig::new($queue));

    $backend->publish($this->envelope($queue, 1));

    $unroutable = $this->envelope($queue, 2);
    $unroutable = new Envelope(
        jobId: $unroutable->jobId,
        jobType: $unroutable->jobType,
        queue: $missing,
        attempt: 2,
        enqueuedAtMs: $unroutable->enqueuedAtMs,
        payload: $unroutable->payload,
    );

    try {
        $backend->publish($unroutable);
        Assert::fail('a publish to a queue nobody declared must be an error');
    } catch (PublishReturned $error) {
        expect($error->getMessage())->toContain($missing);
    }

    $backend->publish($this->envelope($queue, 3));

    $this->awaitCount($queue, 2, 2.0);
    expect($this->queueExists($missing))->toBeFalse('the returned publish must not have created anything');
});

it('closes the channel of a timed-out confirm instead of leaking it', function (): void {
    /** @var RabbitMqTestCase $this */
    // A microsecond is not enough for any broker to confirm anything, so
    // every publish here times out waiting for its ack. The message itself
    // still reaches the queue: only the confirmation is given up on.
    $queue = $this->uniqueQueue('confirm-timeout');
    $backend = $this->backend((new RabbitMqOptions())->withPublishConfirmTimeout(0.000_001));
    $backend->declare(QueueConfig::new($queue));

    $idle = $this->openChannelCount($backend);
    expect($idle)->toBe(0, 'the declare closes its own channel');

    for ($publish = 1; $publish <= 3; ++$publish) {
        try {
            $backend->publish($this->envelope($queue, $publish));
            Assert::fail('a confirm that cannot have arrived must not pass for an ack');
        } catch (BackendException $error) {
            expect($error->getMessage())->not->toContain('returned');
        }

        // The channel carrying the unresolved confirm is closed, not merely
        // dropped: an abandoned one stays open on the broker and eats into
        // `channel_max`, so leaking one per timeout would end in a worker
        // that cannot open a channel at all.
        expect($this->openChannelCount($backend))->toBe($idle, \sprintf(
            'publish %d left its channel behind',
            $publish,
        ));
    }

    // Every one of them went out on a channel of its own that worked.
    $this->awaitCount($queue, 3);
});

it('resubscribes a consumer once its deleted queue is back', function (): void {
    /** @var RabbitMqTestCase $this */
    // Deleting a queue cancels its consumers from the broker side: the
    // connection stays up, the channel is left with nothing on it, and
    // php-amqplib reports it by throwing out of `wait()`. Waiting on that
    // channel again would raise a bare `\Error` — it has no connection left
    // to read from — so the consumer has to be rebuilt instead.
    $queue = $this->uniqueQueue('deleted-under-consumer');
    $backend = $this->backend();
    $logger = new RecordingLogger();
    $backend->setLogger($logger);
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $this->consumeInto($backend, $config);

    $this->deleteQueue($queue);
    expect($this->queueExists($queue))->toBeFalse();

    // Back again, as an operator or a restarted broker would leave it.
    $backend->declare($config);
    $this->publishRaw($queue, $this->envelope($queue, 7)->toJson());

    $delivery = $this->nextDelivery($backend, 15.0);
    expect($delivery->envelope()->attempt)->toBe(7);
    $delivery->ack();
    $this->awaitCount($queue, 0);

    // The delivery could only reach a consumer that had been put back, and
    // the log says so rather than leaving that to inference.
    expect($logger->messages('warning'))
        ->toContain('a consumer channel was closed by the broker; resubscribing');
});

it('refuses a second consumer for the same queue', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('duplicate-consumer');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $this->consumeInto($backend, $config);

    // Overwriting the first consumer would leave it subscribed with nothing
    // owning it: it would keep taking messages and hand them to nobody.
    expect(function () use ($backend, $config): void {
        $this->consumeInto($backend, $config);
    })->toThrow(InvalidConfiguration::class, 'already has a consumer');
});

it('keeps a delivery in hand settleable after cancelConsumers', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('cancel-settle');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $backend->publish($this->envelope($queue));

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);

    // Cancelling stops new deliveries but must leave the channel this one
    // arrived on open, because that is the only channel it can be acked on.
    $backend->cancelConsumers();
    $delivery->ack();

    $this->awaitCount($queue, 0);

    // And the channel does get closed eventually, by `close()`.
    $backend->close();
    expect($this->openChannelCount($backend))->toBe(0);
});

it('still yields a consumer tag the broker accepts for a very long queue name', function (): void {
    /** @var RabbitMqTestCase $this */
    // INTEROP-SPEC §6.6 truncates the queue part of the tag to 160 bytes so
    // the whole tag stays inside the 255-byte AMQP short string. The broker
    // is the only judge of that, so ask it.
    $queue = 'qphp-test-' . bin2hex(random_bytes(6)) . '.' . str_repeat('x', 200);
    expect(\strlen($queue))->toBe(223);
    $this->registerQueue($queue);
    $this->registerQueue($queue . '.dead');

    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $backend->publish($this->envelope($queue));

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);
    $delivery->ack();
    expect($delivery->envelope()->queue)->toBe($queue);
});

it('stops deliveries but keeps the backend alive on cancelConsumers', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('cancel');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $this->consumeInto($backend, $config);
    $backend->cancelConsumers();

    $backend->publish($this->envelope($queue));
    $this->expectIdle($backend, 0.5);
    $this->awaitCount($queue, 1, 2.0);
});
