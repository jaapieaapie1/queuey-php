<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\EnqueueOptions;
use Queuey\Envelope;
use Queuey\Exception\JobBelongsToOtherQueueSet;
use Queuey\Exception\ShutDown;
use Queuey\Exception\UnknownQueue;
use Queuey\FrozenClock;
use Queuey\Memory\MemoryBackend;
use Queuey\Producer;
use Queuey\Tests\Fixtures\Foreign;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Ping;
use Queuey\Tests\Fixtures\TestQueues;

const PRODUCER_ALPHA = 'test.alpha';

const PRODUCER_BETA = 'test.beta-queue';

it('declares every queue of the set from its constructor', function (): void {
    [, $backend] = producerFixture();

    expect($backend->declared())->toBe(['test.alpha', 'test.beta-queue', 'test.send_emails', 'test.http_calls']);
});

it('publishes a first-attempt envelope when enqueueing', function (): void {
    [$clock, $backend, $producer] = producerFixture();

    $id = $producer->enqueue(new Greet('ada'));

    $envelope = producerOnlyPending($backend, PRODUCER_ALPHA);
    expect($envelope->jobId)->toBe($id);
    expect($envelope->jobType)->toBe('test::Greet');
    expect($envelope->queue)->toBe(PRODUCER_ALPHA);
    expect($envelope->attempt)->toBe(1);
    expect($envelope->priority)->toBe(0);
    expect($envelope->deferrals)->toBe(0);
    expect($envelope->correlationId)->toBeNull();
    expect($envelope->enqueuedAtMs)->toBe($clock->nowMs());
});

it('carries the correlation id from the options', function (): void {
    [, $backend, $producer] = producerFixture();

    $producer->enqueue(new Greet('traced'), EnqueueOptions::now()->withCorrelationId('trace-abc'));

    expect(producerOnlyPending($backend, PRODUCER_ALPHA)->correlationId)->toBe('trace-abc');
});

it('waits in hold at priority zero when enqueueing after a delay', function (): void {
    [$clock, $backend, $producer] = producerFixture();

    $producer->enqueueAfter(new Greet('later'), Duration::seconds(10));

    $held = $backend->held(PRODUCER_ALPHA);
    expect($held)->toHaveCount(1);
    expect($held[0]->priority)->toBe(0);
    expect($backend->pending(PRODUCER_ALPHA))->toBe([]);

    $clock->advance(Duration::seconds(10));
    $backend->wait(0.0);

    expect($backend->held(PRODUCER_ALPHA))->toBe([]);
    expect(producerOnlyPending($backend, PRODUCER_ALPHA)->priority)->toBe(0);
});

it('returns a deferred job at the queue top priority, ahead of the backlog', function (): void {
    [$clock, $backend, $producer] = producerFixture();

    $backlog = $producer->enqueue(new Greet('backlog'));
    $deferred = $producer->defer(new Greet('held'), Duration::seconds(30));

    expect($backend->held(PRODUCER_ALPHA))->toHaveCount(1);
    expect($backend->pending(PRODUCER_ALPHA))->toHaveCount(1, 'only the backlog so far');

    $clock->advance(Duration::seconds(29));
    $backend->wait(0.0);
    expect($backend->pending(PRODUCER_ALPHA))->toHaveCount(1);

    $clock->advance(Duration::seconds(1));
    $backend->wait(0.0);

    $pending = $backend->pending(PRODUCER_ALPHA);
    expect($pending)->toHaveCount(2);
    expect($pending[0]->jobId)->toBe($deferred, 'the deferred job is served first');
    expect($pending[0]->priority)->toBe(10);
    expect($pending[0]->deferrals)->toBe(0, 'the producer never deferred it once');
    expect($pending[0]->attempt)->toBe(1);
    expect($pending[1]->jobId)->toBe($backlog);
});

it('defers at priority zero on a queue without priorities', function (): void {
    [$clock, $backend, $producer] = producerFixture();

    $producer->defer(new Ping(1), Duration::seconds(5));

    $clock->advance(Duration::seconds(5));
    $backend->wait(0.0);

    $envelope = producerOnlyPending($backend, PRODUCER_BETA);
    expect($envelope->priority)->toBe(0);
    expect($envelope->deferrals)->toBe(0);
});

it('refuses a job that belongs to another queue set', function (): void {
    [, , $producer] = producerFixture();

    expect(static fn(): string => $producer->enqueue(new Foreign()))
        ->toThrow(JobBelongsToOtherQueueSet::class, 'BackedQueues::Emails');
});

it('declares nothing when the producer is undeclared', function (): void {
    [$clock] = producerFixture();

    $backend = new MemoryBackend($clock);
    $producer = Producer::undeclared(TestQueues::class, $backend);

    expect($backend->declared())->toBe([]);
    expect($producer->queueSet())->toBe(TestQueues::class);
    expect($producer->backend())->toBe($backend);

    expect(static fn(): string => $producer->enqueue(new Greet('nowhere')))->toThrow(UnknownQueue::class);
});

it('fails to defer on a closed backend', function (): void {
    [, $backend, $producer] = producerFixture();

    $backend->close();

    expect(static fn(): string => $producer->defer(new Greet('x'), Duration::seconds(1)))
        ->toThrow(ShutDown::class);
});

/**
 * A frozen clock, the memory backend on it, and a producer for the test queue
 * set — the fixture the old `setUp()` built.
 *
 * @return array{FrozenClock, MemoryBackend, Producer}
 */
function producerFixture(): array
{
    $clock = new FrozenClock(1_700_000_000_000);
    $backend = new MemoryBackend($clock);

    return [$clock, $backend, new Producer(TestQueues::class, $backend, clock: $clock)];
}

function producerOnlyPending(MemoryBackend $backend, string $queue): Envelope
{
    $pending = $backend->pending($queue);
    expect($pending)->toHaveCount(1);

    return $pending[0];
}
