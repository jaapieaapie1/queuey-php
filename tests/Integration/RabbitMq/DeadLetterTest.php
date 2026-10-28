<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Exception\PublishReturned;
use Queuey\QueueConfig;
use Queuey\RabbitMq\Codec;
use Queuey\RabbitMq\RabbitMqOptions;
use Queuey\RabbitMq\Topology;
use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

it('moves a dead-lettered message with its headers', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('dead');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $envelope = $this->envelope($queue, 4);
    $backend->publish($envelope);

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);
    $delivery->deadLetter('fatal error: boom');

    // The original is gone from `q` ...
    $this->awaitCount($queue, 0);

    // ... and the body arrived on `q.dead`, unchanged, with the three
    // dead-letter headers on top of the usual two.
    $dead = $this->awaitRaw($queue . '.dead');
    expect($dead->getBody())->toBe($envelope->toJson());
    $this->assertHeaders([
        Topology::HEADER_ATTEMPT => 4,
        Topology::HEADER_DEFERRALS => 0,
        Topology::HEADER_DEATH_REASON => 'fatal error: boom',
        Topology::HEADER_ORIGINAL_QUEUE => $queue,
        Topology::HEADER_ATTEMPTS => 4,
    ], $dead);

    expect($dead->get('message_id'))->toBe($envelope->jobId);
    expect($dead->get('type'))->toBe($envelope->jobType);
    expect($dead->get('delivery_mode'))->toBe(2);
    expect($dead->get('content_type'))->toBe('application/json');
});

it('rejects the original instead when dead-letter queues are off', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('dead-disabled');
    $backend = $this->backend((new RabbitMqOptions())->withDeclareDeadLetterQueues(false));
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $backend->publish($this->envelope($queue));

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);
    $delivery->deadLetter('fatal error: boom');

    // `q` has no dead-letter policy, so the broker drops it. Nothing was
    // published to a queue this backend does not own.
    $this->awaitCount($queue, 0);
    expect($this->queueExists($queue . '.dead'))->toBeFalse();
});

it('leaves the original unacked when dead-lettering into a missing dead queue', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('dead-missing');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    // The queue this backend declared a moment ago, deleted out from under
    // it: an operator, or a broker that came back without it.
    $dead = $backend->deadQueueName($queue);
    $this->deleteQueue($dead);
    expect($this->queueExists($dead))->toBeFalse();

    $backend->publish($this->envelope($queue));
    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);

    try {
        $delivery->deadLetter('handler gave up');
        Assert::fail('dead-lettering into a missing `q.dead` must fail rather than ack the original');
    } catch (PublishReturned $error) {
        // `toContain()` takes needles, not a message, so the "why" stays on the
        // PHPUnit assertion.
        Assert::assertStringContainsString(
            $dead,
            $error->getMessage(),
            'the error must name the queue it could not reach',
        );
        expect($error->getMessage())->toContain('returned by the broker');
    }

    // Closing requeues whatever was left unacked, so the broker still owning
    // the original is the proof that `deadLetter()` did not ack it.
    $backend->close();
    $this->awaitCount($queue, 1);
});

it('forwards a malformed body to the dead-letter queue and acks it', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('malformed');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    // Not JSON at all, and then a valid envelope behind it: the malformed one
    // must not stall the queue.
    $this->publishRaw($queue, 'this is not an envelope');
    $good = $this->envelope($queue);
    $backend->publish($good);

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);
    expect($delivery->envelope()->jobId)->toBe($good->jobId, 'the malformed body must be skipped, not handed over');
    $delivery->ack();

    $this->awaitCount($queue, 0);

    $dead = $this->awaitRaw($queue . '.dead');
    expect($dead->getBody())->toBe('this is not an envelope', 'the body is forwarded verbatim');
    $this->assertHeaders([
        Topology::HEADER_DEATH_REASON => Codec::REASON_MALFORMED,
        Topology::HEADER_ORIGINAL_QUEUE => $queue,
    ], $dead, 'a malformed body carries only where it came from and why');
    expect($dead->get('delivery_mode'))->toBe(2);
    expect($dead->has('message_id'))->toBeFalse('there is no job metadata to carry');
    expect($dead->has('type'))->toBeFalse();
});

it('rejects a malformed body when dead-letter queues are disabled', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('malformed-disabled');
    $backend = $this->backend((new RabbitMqOptions())->withDeclareDeadLetterQueues(false));
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $this->publishRaw($queue, '{"job_id":"not-a-uuid"}');
    $good = $this->envelope($queue);
    $backend->publish($good);

    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);
    expect($delivery->envelope()->jobId)->toBe($good->jobId);
    $delivery->ack();

    $this->awaitCount($queue, 0);
    expect($this->queueExists($queue . '.dead'))->toBeFalse();
});
