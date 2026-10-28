<?php

declare(strict_types=1);

use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\Assert;
use Queuey\Duration;
use Queuey\Exception\BackendException;
use Queuey\Exception\DelayTooLong;
use Queuey\Exception\UnknownQueue;
use Queuey\QueueConfig;
use Queuey\RabbitMq\RabbitMqOptions;
use Queuey\RabbitMq\Topology;
use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

it('waits out a retry in its hold queue and brings it back with the next attempt', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('retry');
    $hold = $queue . '.deferred.1000';
    $this->registerQueue($hold);

    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $backend->publish($this->envelope($queue, 1));

    $this->consumeInto($backend, $config);
    $first = $this->nextDelivery($backend);
    expect($first->envelope()->attempt)->toBe(1);

    $started = microtime(true);
    $first->retry($first->envelope()->nextAttempt(), Duration::seconds(1));

    // The hold queue is created on demand, named after the rounded delay.
    expect($this->queueExists($hold))->toBeTrue($hold . ' must exist while the job waits');
    expect($this->readyCount($hold))->toBe(1);

    // Nothing before the TTL is up ...
    $this->expectIdle($backend, 0.5);

    // ... and then the retry, one attempt further along and back at priority 0.
    $second = $this->nextDelivery($backend, 8.0);
    $elapsed = microtime(true) - $started;
    expect($elapsed)->toBeGreaterThanOrEqual(0.9, 'a retry must never come back early');
    expect($second->envelope()->attempt)->toBe(2);
    expect($second->envelope()->priority)->toBe(0);
    expect($second->envelope()->jobId)->toBe($first->envelope()->jobId);
    expect($second->envelope()->enqueuedAtMs)->toBe($first->envelope()->enqueuedAtMs);
    $second->ack();
});

it('does not stick a short retry behind a long one', function (): void {
    /** @var RabbitMqTestCase $this */
    // The whole reason hold queues exist: a shared wait queue with
    // per-message expirations would only ever expire its head, so the 6s
    // wait would hold the 1s wait back.
    $queue = $this->uniqueQueue('head-of-line');
    $this->registerQueue($queue . '.deferred.1000');
    $this->registerQueue($queue . '.deferred.6000');

    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $slow = $this->envelope($queue, 6);
    $quick = $this->envelope($queue, 1);
    $backend->publish($slow, Duration::seconds(6));
    $backend->publish($quick, Duration::seconds(1));

    $this->consumeInto($backend, $config);
    $first = $this->nextDelivery($backend, 5.0);
    expect($first->envelope()->jobId)->toBe($quick->jobId, 'the short wait must not queue behind the long one');
    $first->ack();

    $second = $this->nextDelivery($backend, 10.0);
    expect($second->envelope()->jobId)->toBe($slow->jobId);
    $second->ack();
});

it('brings a deferred job back ahead of the backlog', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('defer-priority');
    $this->registerQueue($queue . '.deferred.1000');

    $backend = $this->backend();
    // The default max_priority is 10 levels, which is what makes this work.
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    // A backlog, piled up with nobody consuming.
    $first = $this->envelope($queue, 1);
    $second = $this->envelope($queue, 2);
    $backend->publish($first);
    $backend->publish($second);
    $this->awaitCount($queue, 2);

    $held = $this->envelope($queue, 7)->deferred(10);
    $backend->defer($held, Duration::seconds(1));

    // Wait for the broker to move it back onto `q`, so all three are ready
    // and the ordering below is about priority, not about arrival time.
    $this->awaitCount($queue, 3);

    $this->consumeInto($backend, $config);
    $deferred = $this->nextDelivery($backend);
    expect($deferred->envelope()->jobId)->toBe($held->jobId, 'the deferred job must overtake the backlog');
    expect($deferred->envelope()->deferrals)->toBe(1);
    expect($deferred->envelope()->priority)->toBe(10);
    expect($deferred->envelope()->attempt)->toBe(7, 'a deferral is not a failed attempt');
    $deferred->ack();

    // ... and the backlog then follows in its original order.
    $one = $this->nextDelivery($backend);
    expect($one->envelope()->jobId)->toBe($first->jobId);
    $one->ack();
    $two = $this->nextDelivery($backend);
    expect($two->envelope()->jobId)->toBe($second->jobId);
    $two->ack();
});

it('deletes the hold queue once it goes unused', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('expiry');
    // A 1ms granularity puts the hold queue at x-message-ttl 500 and
    // x-expires 1000 (always twice the TTL, never tunable), so the broker's
    // clean-up can be watched inside a test.
    $backend = $this->backend((new RabbitMqOptions())->withRetryGranularity(Duration::ms(1)));
    $hold = $queue . '.deferred.500';
    $this->registerQueue($hold);

    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $started = microtime(true);
    $backend->publish($this->envelope($queue), Duration::ms(500));
    expect($this->queueExists($hold))->toBeTrue();

    $this->awaitCount($queue, 1, 3.0);

    // Checked exactly once, well past `x-expires`: a passive declare is
    // itself a use, so polling for the deletion would keep postponing it.
    $sleep = ($started + 2.5) - microtime(true);
    if ($sleep > 0) {
        usleep((int) ($sleep * 1_000_000));
    }
    expect($this->queueExists($hold))->toBeFalse($hold . ' must expire once it has gone unused');
});

it('fails the defer and leaves the original unacked when the hold queue has foreign arguments', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('foreign-hold');
    $hold = $queue . '.deferred.1000';
    $this->registerQueue($hold);

    $backend = $this->backend();
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    // Somebody else got there first, with arguments this backend would never
    // compute. RabbitMQ answers the redeclare with PRECONDITION_FAILED.
    $foreign = new AMQPTable();
    $foreign->set(Topology::ARG_MESSAGE_TTL, 1234, AMQPTable::T_INT_LONGLONG);
    $this->declareRaw($hold, true, $foreign);

    $backend->publish($this->envelope($queue));
    $this->consumeInto($backend, $config);
    $delivery = $this->nextDelivery($backend);

    try {
        $delivery->defer($delivery->envelope()->deferred(10), Duration::seconds(1));
        Assert::fail('a hold queue with foreign arguments must fail the defer');
    } catch (BackendException $error) {
        expect($error->getMessage())->toContain($hold);
    }

    // Only the declaration channel died, so the publishing channel is fine.
    $backend->cancelConsumers();
    $backend->publish($this->envelope($queue));

    // Publish before ack: the failed defer left the original unacked, so
    // closing the backend hands it straight back to the broker.
    $backend->close();
    $this->awaitCount($queue, 2);
});

it('refuses a deferral onto a queue this backend never declared', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('undeclared');
    $backend = $this->backend();
    $envelope = $this->envelope($queue);

    // The hold queue dead-letters into `q` by name, so publishing into one
    // for a queue we know nothing about would strand the message.
    expect(static function () use ($backend, $envelope): void {
        $backend->defer($envelope, Duration::seconds(1));
    })->toThrow(UnknownQueue::class);
});

it('refuses a delay past the cap rather than shortening it', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('too-long');
    $backend = $this->backend();
    $backend->declare(QueueConfig::new($queue));

    try {
        $backend->defer($this->envelope($queue), Duration::days(30));
        Assert::fail('a delay past the cap must be refused');
    } catch (DelayTooLong $error) {
        expect($error->getMessage())->toContain((string) Topology::MAX_DEFERRAL_MS);
    }

    // Nothing was published, and nothing was created.
    $this->awaitCount($queue, 0, 1.0);
});

it('keeps the retry and deferral granularities separate', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('granularity');
    $this->registerQueue($queue . '.deferred.10000');
    $this->registerQueue($queue . '.deferred.2000');

    $backend = $this->backend(
        (new RabbitMqOptions())
            ->withRetryGranularity(Duration::seconds(10))
            ->withDeferredGranularity(Duration::seconds(2)),
    );
    $config = QueueConfig::new($queue);
    $backend->declare($config);

    $backend->publish($this->envelope($queue), Duration::seconds(1));
    expect($this->queueExists($queue . '.deferred.10000'))
        ->toBeTrue('a retry rounds to the retry granularity');

    $backend->defer($this->envelope($queue), Duration::seconds(1));
    expect($this->queueExists($queue . '.deferred.2000'))
        ->toBeTrue('a deferral rounds to its own granularity');
});
