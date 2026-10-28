<?php

declare(strict_types=1);

use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\Assert;
use Queuey\Exception\BackendException;
use Queuey\QueueConfig;
use Queuey\RabbitMq\RabbitMqOptions;
use Queuey\RabbitMq\Topology;
use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

it('creates the main queue with priorities and a plain dead-letter queue', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('declare');
    $backend = $this->backend();

    $backend->declare(QueueConfig::new($queue));

    expect($this->queueExists($queue))->toBeTrue();
    expect($this->queueExists($queue . '.dead'))->toBeTrue();

    // A redeclare with the expected arguments is the strongest assertion
    // available: RabbitMQ answers a mismatch with PRECONDITION_FAILED.
    $priority = new AMQPTable();
    $priority->set(Topology::ARG_MAX_PRIORITY, 10, AMQPTable::T_INT_SHORTSHORT_U);
    expect($this->queueHasArgs($queue, true, $priority))
        ->toBeTrue('the main queue must carry x-max-priority 10 and nothing else');

    expect($this->queueHasArgs($queue . '.dead', true, new AMQPTable()))
        ->toBeTrue('the dead-letter queue must carry no arguments at all');
});

it('is idempotent', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('idempotent');
    $backend = $this->backend();
    $config = QueueConfig::new($queue);

    $backend->declare($config);
    $backend->declare($config);
    $backend->declare($config, $config);

    expect($this->queueExists($queue))->toBeTrue();
});

it('declares without the argument when maxPriority is zero', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('nopriority');
    $backend = $this->backend();

    $backend->declare(QueueConfig::new($queue)->withMaxPriority(0));

    expect($this->queueHasArgs($queue, true, new AMQPTable()))
        ->toBeTrue('a queue without priorities must be declared with no arguments');
});

it('refuses a redeclare with a different max priority and stays usable', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('conflict');
    $backend = $this->backend();

    $backend->declare(QueueConfig::new($queue)->withMaxPriority(5));

    try {
        $backend->declare(QueueConfig::new($queue)->withMaxPriority(9));
        Assert::fail('changing x-max-priority on an existing queue must be refused');
    } catch (BackendException $error) {
        // Surfaced, not hung: this is the whole point of the assertion.
        expect($error->getMessage())->toContain($queue);
        Assert::assertStringContainsStringIgnoringCase('precondition_failed', $error->getMessage());
    }

    // The refused declare closed its own short-lived channel and nothing
    // else, so the backend still works.
    $other = $this->uniqueQueue('conflict-after');
    $backend->declare(QueueConfig::new($other));
    expect($this->queueExists($other))->toBeTrue();

    $backend->publish($this->envelope($queue));
    $this->awaitCount($queue, 1);
});

it('refuses a queue with no room for its hold queues before anything is created', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = str_repeat('q', 250);
    $backend = $this->backend();

    try {
        $backend->declare(QueueConfig::new($queue));
        Assert::fail('a queue name with no room for its hold queues must be refused');
    } catch (BackendException $error) {
        expect($error->getMessage())->toContain('leaves no room for its hold queues');
    }

    expect($this->queueExists($queue))->toBeFalse('nothing may be created when the check fails');
});

it('leaves the dead-letter queues to the operator when asked to', function (): void {
    /** @var RabbitMqTestCase $this */
    $queue = $this->uniqueQueue('no-dlq');
    $backend = $this->backend((new RabbitMqOptions())->withDeclareDeadLetterQueues(false));

    $backend->declare(QueueConfig::new($queue));

    expect($this->queueExists($queue))->toBeTrue();
    expect($this->queueExists($queue . '.dead'))->toBeFalse();
});
