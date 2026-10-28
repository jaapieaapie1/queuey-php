<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Delivery;
use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\AlreadySettled;
use Queuey\Exception\ShutDown;
use Queuey\Exception\UnknownQueue;
use Queuey\FrozenClock;
use Queuey\Memory\MemoryBackend;
use Queuey\QueueConfig;
use Queuey\Tests\Fixtures\TestQueues;

const MEMORY_BACKEND_ALPHA = 'test.alpha';

it('declares idempotently and remembers the order', function (): void {
    [$clock, $backend] = memoryBackendFixture();
    memoryBackendPublish($backend, $clock, 'a');

    $backend->declare(TestQueues::Alpha->config(), TestQueues::Beta->config());
    $backend->declare(TestQueues::Alpha->config());

    expect($backend->declared())->toBe(['test.alpha', 'test.beta-queue']);
    expect($backend->pending(MEMORY_BACKEND_ALPHA))->toHaveCount(1, 're-declaring must not drop messages');
});

it('refuses publishing to an undeclared queue', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    expect(static function () use ($backend, $clock): void {
        $backend->publish(Envelope::raw('test::Greet', 'test.nowhere', (object) [], $clock));
    })->toThrow(UnknownQueue::class);
});

it('refuses deferring onto an undeclared queue', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    expect(static function () use ($backend, $clock): void {
        $backend->defer(
            Envelope::raw('test::Greet', 'test.nowhere', (object) [], $clock),
            Duration::seconds(1),
        );
    })->toThrow(UnknownQueue::class);
});

it('refuses everything once closed', function (): void {
    [$clock, $backend] = memoryBackendFixture();
    $backend->close();

    expect($backend->isClosed())->toBeTrue();
    expect(static function () use ($backend, $clock): void {
        $backend->publish(Envelope::raw('test::Greet', MEMORY_BACKEND_ALPHA, (object) [], $clock));
    })->toThrow(ShutDown::class);
});

it('refuses a declare once closed', function (): void {
    [, $backend] = memoryBackendFixture();
    $backend->close();

    expect(static function () use ($backend): void {
        $backend->declare(TestQueues::Beta->config());
    })->toThrow(ShutDown::class);
});

it('lets a higher priority overtake the backlog and keeps equal priorities fifo', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'first');
    memoryBackendPublish($backend, $clock, 'second');
    memoryBackendPublish($backend, $clock, 'urgent', priority: 10);
    memoryBackendPublish($backend, $clock, 'third');
    memoryBackendPublish($backend, $clock, 'also urgent', priority: 10);

    expect(memoryBackendNames($backend->pending(MEMORY_BACKEND_ALPHA)))
        ->toBe(['urgent', 'also urgent', 'first', 'second', 'third']);
});

it('lands a middle priority between the levels', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'top', priority: 10);
    memoryBackendPublish($backend, $clock, 'plain');
    memoryBackendPublish($backend, $clock, 'middle', priority: 5);

    expect(memoryBackendNames($backend->pending(MEMORY_BACKEND_ALPHA)))->toBe(['top', 'middle', 'plain']);
});

it('keeps a delayed publish in hold until the clock passes it', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    $backend->publish(memoryBackendEnvelope($clock, 'later'), Duration::seconds(10));

    expect($backend->held(MEMORY_BACKEND_ALPHA))->toHaveCount(1);
    expect($backend->pending(MEMORY_BACKEND_ALPHA))->toBe([]);

    $clock->advance(Duration::seconds(9));
    $backend->wait(0.0);
    expect($backend->held(MEMORY_BACKEND_ALPHA))->toHaveCount(1, 'not due yet');

    $clock->advance(Duration::seconds(1));
    $backend->wait(0.0);
    expect($backend->held(MEMORY_BACKEND_ALPHA))->toBe([]);
    expect(memoryBackendNames($backend->pending(MEMORY_BACKEND_ALPHA)))->toBe(['later']);
});

it('publishes straight away on a zero delay', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    $backend->publish(memoryBackendEnvelope($clock, 'now'), Duration::zero());
    $backend->defer(memoryBackendEnvelope($clock, 'also now'), Duration::zero());

    expect($backend->held(MEMORY_BACKEND_ALPHA))->toBe([]);
    expect($backend->pending(MEMORY_BACKEND_ALPHA))->toHaveCount(2);
});

it('queues released holds by priority in hold order', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    $backend->publish(memoryBackendEnvelope($clock, 'plain'), Duration::seconds(1));
    $backend->defer(memoryBackendEnvelope($clock, 'deferred', priority: 10), Duration::seconds(1));
    memoryBackendPublish($backend, $clock, 'backlog');

    $clock->advance(Duration::seconds(1));
    $backend->wait(0.0);

    expect(memoryBackendNames($backend->pending(MEMORY_BACKEND_ALPHA)))
        ->toBe(['deferred', 'backlog', 'plain']);
});

it('hands at most one delivery per consumer per wait', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'one');
    memoryBackendPublish($backend, $clock, 'two');

    $seen = [];
    $backend->consume(TestQueues::Alpha->config(), static function (Delivery $delivery) use (&$seen): void {
        $seen[] = memoryBackendNameOf($delivery->envelope());
        $delivery->ack();
    });

    $backend->wait(0.0);
    expect($seen)->toBe(['one']);

    $backend->wait(0.0);
    expect($seen)->toBe(['one', 'two']);
    expect($backend->acked())->toBe(2);
});

it('hands nothing to cancelled consumers', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    $seen = 0;
    $backend->consume(TestQueues::Alpha->config(), static function (Delivery $delivery) use (&$seen): void {
        ++$seen;
        $delivery->ack();
    });
    $backend->cancelConsumers();
    memoryBackendPublish($backend, $clock, 'ignored');
    $backend->wait(0.0);

    expect($seen)->toBe(0);
    expect($backend->consumerCount())->toBe(0);
    expect($backend->pending(MEMORY_BACKEND_ALPHA))->toHaveCount(1);
});

it('refuses consuming an undeclared queue', function (): void {
    [, $backend] = memoryBackendFixture();

    expect(static function () use ($backend): void {
        $backend->consume(QueueConfig::new('test.nowhere'), static fn(Delivery $d) => $d->ack());
    })->toThrow(UnknownQueue::class);
});

it('holds the successor and acks the original on a retry', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'flaky');
    $delivery = memoryBackendTakeOne($backend);

    $delivery->retry($delivery->envelope()->nextAttempt(), Duration::seconds(30));

    expect($backend->acked())->toBe(1);
    $held = $backend->held(MEMORY_BACKEND_ALPHA);
    expect($held)->toHaveCount(1);
    expect($held[0]->attempt)->toBe(2);
});

it('records a dead letter with its reason', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'doomed');
    $delivery = memoryBackendTakeOne($backend);

    $delivery->deadLetter('fatal error: nope');

    $dead = $backend->dead(MEMORY_BACKEND_ALPHA);
    expect($dead)->toHaveCount(1);
    expect($dead[0]['reason'])->toBe('fatal error: nope');
    expect($dead[0]['envelope']->jobId)->toBe($delivery->envelope()->jobId);
});

it('refuses to settle twice', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'once');
    $delivery = memoryBackendTakeOne($backend);
    $delivery->ack();

    expect(static function () use ($delivery): void {
        $delivery->ack();
    })->toThrow(AlreadySettled::class);
});

it('leaves the delivery unsettled when the publish fails', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'flaky');
    $delivery = memoryBackendTakeOne($backend);
    $backend->close();

    // Publish before ack: the original is still ours to settle.
    expect(static function () use ($delivery): void {
        $delivery->retry($delivery->envelope()->nextAttempt(), Duration::seconds(1));
    })->toThrow(ShutDown::class, null, 'a closed backend must refuse the successor');

    expect($backend->acked())->toBe(0);
    $delivery->ack();
    expect($backend->acked())->toBe(1);
});

it('gives up after the timeout when waiting on a system clock', function (): void {
    $backend = new MemoryBackend();
    $backend->declare(TestQueues::Alpha->config());

    $started = microtime(true);
    $backend->wait(0.05);

    expect(microtime(true) - $started)->toBeGreaterThanOrEqual(0.04);
});

it('refuses a wait once closed', function (): void {
    [, $backend] = memoryBackendFixture();
    $backend->close();

    // A worker polling a backend that is gone must be told, not left to spin.
    expect(static function () use ($backend): void {
        $backend->wait(0.0);
    })->toThrow(ShutDown::class);
});

it('yields through the sleeper on an idle wait and gives up on a frozen clock', function (): void {
    [$clock] = memoryBackendFixture();

    $slept = [];
    $backend = new MemoryBackend($clock, static function (float $seconds) use (&$slept): void {
        $slept[] = $seconds;
    });
    $backend->declare(TestQueues::Alpha->config());

    $backend->wait(1.0);

    expect($slept)->toHaveCount(1, 'a clock that cannot move makes a second poll pointless');
    expect($slept[0])->toBeGreaterThan(0.0, 'the idle step is a real yield, not a spin');
});

it('returns from a wait without a timeout when the clock cannot move', function (): void {
    [$clock] = memoryBackendFixture();

    $slept = 0;
    $backend = new MemoryBackend($clock, static function () use (&$slept): void {
        ++$slept;
    });
    $backend->declare(TestQueues::Alpha->config());

    $backend->wait(null);

    expect($slept)->toBe(1, '`null` is no limit, but a frozen clock still ends the poll');
});

it('releases a hold through a sleeper that advances the clock', function (): void {
    [$clock] = memoryBackendFixture();

    $backend = new MemoryBackend($clock, static function () use ($clock): void {
        $clock->advance(Duration::ms(100));
    });
    $backend->declare(TestQueues::Alpha->config());
    $backend->publish(memoryBackendEnvelope($clock, 'later'), Duration::ms(500));

    $seen = [];
    $backend->consume(TestQueues::Alpha->config(), static function (Delivery $delivery) use (&$seen): void {
        $seen[] = memoryBackendNameOf($delivery->envelope());
        $delivery->ack();
    });

    $backend->wait(5.0);

    expect($seen)->toBe(['later']);
    expect($backend->succeeded())->toBe(1);
});

it('brings released holds out in due order, not in hold order', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    $backend->publish(memoryBackendEnvelope($clock, 'plain'), Duration::seconds(1));
    $backend->defer(memoryBackendEnvelope($clock, 'deferred', priority: 10), Duration::seconds(1));
    $backend->publish(memoryBackendEnvelope($clock, 'early'), Duration::ms(500));
    memoryBackendPublish($backend, $clock, 'backlog');

    $clock->advance(Duration::seconds(1));
    $backend->wait(0.0);

    // `early` came due half a second before the other two, which went into
    // hold before it; the two that fall due together keep their hold order.
    expect(memoryBackendNames($backend->pending(MEMORY_BACKEND_ALPHA)))
        ->toBe(['deferred', 'backlog', 'early', 'plain']);
});

it('counts a dead letter as a settle but not as an ack', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'doomed');

    memoryBackendTakeOne($backend)->deadLetter('fatal error: nope');

    expect($backend->acked())->toBe(0, 'a dead letter is not an ack');
    expect($backend->succeeded())->toBe(0);
    expect($backend->settled())->toBe(1);
    expect($backend->dead(MEMORY_BACKEND_ALPHA))->toHaveCount(1);
});

it('tells success, retry and deferral apart in the ack projections', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'done');
    memoryBackendPublish($backend, $clock, 'flaky');
    memoryBackendPublish($backend, $clock, 'early');

    memoryBackendTakeOne($backend)->ack();
    $retried = memoryBackendTakeOne($backend);
    $retried->retry($retried->envelope()->nextAttempt(), Duration::seconds(1));
    $deferred = memoryBackendTakeOne($backend);
    $deferred->defer($deferred->envelope()->deferred(10), Duration::seconds(1));

    expect($backend->acked())->toBe(3, 'all three outcomes ack the original');
    expect($backend->succeeded())->toBe(1, 'only one of them means the job is done');
    expect($backend->retried())->toBe(1);
    expect($backend->settled())->toBe(3);
});

it('puts the message back when a consumer callback throws', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'first');
    memoryBackendPublish($backend, $clock, 'second');
    $backend->consume(TestQueues::Alpha->config(), static function (Delivery $delivery): void {
        throw new RuntimeException('the callback blew up');
    });

    expect(static function () use ($backend): void {
        $backend->wait(0.0);
    })->toThrow(RuntimeException::class, 'the callback blew up', 'a throwing callback must not be swallowed');

    expect(memoryBackendNames($backend->pending(MEMORY_BACKEND_ALPHA)))
        ->toBe(['first', 'second'], 'an unsettled delivery goes back to the head of its queue');
    expect($backend->settled())->toBe(0);
});

it('keeps the settle when a callback settles before it throws', function (): void {
    [$clock, $backend] = memoryBackendFixture();

    memoryBackendPublish($backend, $clock, 'handled');
    $backend->consume(TestQueues::Alpha->config(), static function (Delivery $delivery): void {
        $delivery->ack();

        throw new RuntimeException('after the fact');
    });

    // The message was settled before the throw, so nothing is requeued.
    expect(static function () use ($backend): void {
        $backend->wait(0.0);
    })->toThrow(RuntimeException::class, 'after the fact', 'a throwing callback must not be swallowed');

    expect($backend->pending(MEMORY_BACKEND_ALPHA))->toBe([]);
    expect($backend->succeeded())->toBe(1);
});

/**
 * A frozen clock and a backend with the alpha queue already declared.
 *
 * @return array{FrozenClock, MemoryBackend}
 */
function memoryBackendFixture(): array
{
    $clock = new FrozenClock(1_700_000_000_000);
    $backend = new MemoryBackend($clock);
    $backend->declare(TestQueues::Alpha->config());

    return [$clock, $backend];
}

/**
 * @param list<Envelope> $envelopes
 *
 * @return list<string>
 */
function memoryBackendNames(array $envelopes): array
{
    return array_map(memoryBackendNameOf(...), $envelopes);
}

/** The `name` field of a `Greet`-shaped payload. */
function memoryBackendNameOf(Envelope $envelope): string
{
    $payload = (array) $envelope->payload;
    $name = $payload['name'] ?? null;
    if (!\is_string($name)) {
        Assert::fail('envelope payload has no `name`');
    }

    return $name;
}

function memoryBackendEnvelope(FrozenClock $clock, string $name, int $priority = 0): Envelope
{
    $envelope = Envelope::raw('test::Greet', MEMORY_BACKEND_ALPHA, (object) ['name' => $name], $clock);
    if ($priority === 0) {
        return $envelope;
    }

    return new Envelope(
        jobId: $envelope->jobId,
        jobType: $envelope->jobType,
        queue: $envelope->queue,
        attempt: $envelope->attempt,
        enqueuedAtMs: $envelope->enqueuedAtMs,
        priority: $priority,
        payload: $envelope->payload,
    );
}

function memoryBackendPublish(
    MemoryBackend $backend,
    FrozenClock $clock,
    string $name,
    int $priority = 0,
): Envelope {
    $envelope = memoryBackendEnvelope($clock, $name, $priority);
    $backend->publish($envelope);

    return $envelope;
}

/** The next delivery on alpha, handed out and left unsettled. */
function memoryBackendTakeOne(MemoryBackend $backend): Delivery
{
    $taken = null;
    $backend->consume(TestQueues::Alpha->config(), static function (Delivery $delivery) use (&$taken): void {
        $taken = $delivery;
    });
    $backend->wait(0.0);
    $backend->cancelConsumers();

    Assert::assertInstanceOf(Delivery::class, $taken);

    return $taken;
}
