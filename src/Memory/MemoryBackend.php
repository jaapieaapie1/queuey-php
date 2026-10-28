<?php

declare(strict_types=1);

namespace Queuey\Memory;

use Queuey\Backend;
use Queuey\Clock;
use Queuey\Delivery;
use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\ShutDown;
use Queuey\Exception\UnknownQueue;
use Queuey\QueueConfig;
use Queuey\SystemClock;

/**
 * An in-memory {@see Backend} for tests and local development.
 *
 * Deterministic on purpose: there are no threads and no timers. Delays are
 * deadlines on the injected {@see Clock}, so a test with a
 * {@see \Queuey\FrozenClock} decides when a held envelope is released by
 * advancing the clock, and {@see self::wait()} does the release and hands at
 * most one delivery to each registered consumer.
 *
 * ```php
 * $clock = new FrozenClock(0);
 * $backend = new MemoryBackend($clock);
 * $backend->declare(AppQueues::Emails->config());
 * $producer = new Producer(AppQueues::class, $backend, clock: $clock);
 * ```
 *
 * Differences from a broker worth knowing about: `prefetch` is ignored (a
 * delivery is handled inside `wait()` before the next one is handed out), a
 * delivery that is never settled is simply gone, and dead letters are kept in an
 * inspectable list instead of on a `q.dead` queue.
 */
final class MemoryBackend implements Backend
{
    /** How long an idle {@see self::wait()} yields for between polls. */
    private const IDLE_STEP_US = 2_000;

    /** @var array<string, QueueConfig> queue name to the config its first declare carried */
    private array $configs = [];

    /** @var array<string, list<Envelope>> queue name to the envelopes waiting, in priority order */
    private array $pending = [];

    /** @var array<string, list<array{envelope: Envelope, dueAtMs: int}>> queue name to what is in hold */
    private array $held = [];

    /** @var array<string, list<array{envelope: Envelope, reason: string}>> queue name to what was given up on */
    private array $dead = [];

    /** @var list<array{queue: string, onDelivery: \Closure(Delivery): void}> */
    private array $consumers = [];

    /** @var array<string, int> {@see AckKind} name to how many acks of that kind were recorded */
    private array $acks = [];

    private bool $closed = false;

    /** @var \Closure(float): void */
    private readonly \Closure $sleeper;

    /**
     * @param (callable(float $seconds): void)|null $sleeper how an idle {@see self::wait()} yields;
     *                                                       defaults to `usleep`. A test on a
     *                                                       {@see \Queuey\FrozenClock} can pass a
     *                                                       no-op, or one that advances the clock
     */
    public function __construct(
        private readonly Clock $clock = new SystemClock(),
        ?callable $sleeper = null,
    ) {
        if ($sleeper !== null) {
            $this->sleeper = $sleeper(...);

            return;
        }

        $this->sleeper = static function (float $seconds): void {
            usleep((int) round($seconds * 1_000_000));
        };
    }

    public function declare(QueueConfig ...$queues): void
    {
        $this->assertOpen();
        foreach ($queues as $config) {
            // Idempotent: re-declaring never drops pending messages or consumers.
            if (!isset($this->configs[$config->name])) {
                $this->configs[$config->name] = $config;
                $this->pending[$config->name] = [];
                $this->held[$config->name] = [];
                $this->dead[$config->name] = [];
            }
        }
    }

    public function publish(Envelope $envelope, ?Duration $delay = null): void
    {
        $this->assertOpen();
        $this->assertDeclared($envelope->queue);

        if ($delay === null || $delay->isZero()) {
            $this->enqueueByPriority($envelope);

            return;
        }

        $this->hold($envelope, $delay);
    }

    public function defer(Envelope $envelope, Duration $delay): void
    {
        $this->assertOpen();
        $this->assertDeclared($envelope->queue);

        if ($delay->isZero()) {
            $this->enqueueByPriority($envelope);

            return;
        }

        $this->hold($envelope, $delay);
    }

    /**
     * @param callable(Delivery): void $onDelivery
     */
    public function consume(QueueConfig $queue, callable $onDelivery): void
    {
        $this->assertOpen();
        $this->assertDeclared($queue->name);

        $this->consumers[] = ['queue' => $queue->name, 'onDelivery' => $onDelivery(...)];
    }

    /**
     * Release everything whose hold has elapsed, then hand at most one delivery
     * to each registered consumer.
     *
     * An idle call polls in small steps until something is dispatched or
     * `$timeoutSeconds` have passed on the injected clock, yielding the CPU
     * through the sleeper between polls — the constructor's, `usleep` by default.
     *
     * The deadline is read from the clock, so a {@see \Queuey\FrozenClock} would
     * never reach it. Such a clock also cannot make a held envelope due while we
     * sleep, and nothing else runs in a single-threaded process, so an idle call
     * yields exactly once and returns as soon as it sees that the clock did not
     * move: waiting longer could not change the answer. A sleeper that advances a
     * frozen clock keeps the loop going until the deadline instead.
     *
     * @throws ShutDown once {@see self::close()} has been called: a backend that
     *                  is gone delivers nothing ever again, and a worker polling
     *                  it must be told rather than left spinning
     */
    public function wait(?float $timeoutSeconds = null): void
    {
        $this->assertOpen();
        $this->releaseDue();
        if ($this->dispatchRound() > 0) {
            return;
        }

        $deadlineMs = $timeoutSeconds === null
            ? null
            : $this->clock->nowMs() + (int) ceil(max(0.0, $timeoutSeconds) * 1000);

        while (!$this->closed) {
            if ($deadlineMs !== null && $this->clock->nowMs() >= $deadlineMs) {
                return;
            }
            $beforeMs = $this->clock->nowMs();
            ($this->sleeper)(self::IDLE_STEP_US / 1_000_000);
            $this->releaseDue();
            if ($this->dispatchRound() > 0) {
                return;
            }
            if ($this->clock->nowMs() === $beforeMs) {
                return;
            }
        }
    }

    public function cancelConsumers(): void
    {
        $this->consumers = [];
    }

    public function close(): void
    {
        $this->closed = true;
        $this->cancelConsumers();
    }

    /** Whether {@see self::close()} has been called. */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * The envelopes waiting on `$queue`, in the order they will be handed out.
     *
     * @return list<Envelope>
     */
    public function pending(string $queue): array
    {
        return $this->pending[$queue] ?? [];
    }

    /**
     * The envelopes whose hold has not elapsed yet: invisible to consumers, and
     * released by the next {@see self::wait()} once the clock has passed them.
     *
     * @return list<Envelope>
     */
    public function held(string $queue): array
    {
        return array_map(
            static fn(array $entry): Envelope => $entry['envelope'],
            $this->held[$queue] ?? [],
        );
    }

    /**
     * What was dead-lettered on `$queue`, with the reason, in dead-letter order.
     *
     * @return list<array{envelope: Envelope, reason: string}>
     */
    public function dead(string $queue): array
    {
        return $this->dead[$queue] ?? [];
    }

    /**
     * How many delivery attempts were settled by an ack.
     *
     * Three outcomes end in one: the handler succeeded, a retry was scheduled
     * ({@see \Queuey\Delivery::retry()} acks the original) and a deferral was
     * held ({@see \Queuey\Delivery::defer()} does the same). So this counts
     * attempts rather than successes — and *not* dead letters, which are no ack
     * at all, here or on the Rust side.
     *
     * For "did this job succeed", use {@see self::succeeded()}; for every settle
     * whatever its kind, {@see self::settled()}.
     */
    public function acked(): int
    {
        return array_sum($this->acks);
    }

    /**
     * How many attempts were settled by {@see \Queuey\Delivery::ack()}: the ones
     * whose handler returned. This is what most assertions mean by "acked".
     */
    public function succeeded(): int
    {
        return $this->acks[AckKind::Succeeded->name] ?? 0;
    }

    /**
     * How many attempts were acked because {@see \Queuey\Delivery::retry()}
     * scheduled a further one. The rescheduled copy arrives separately with
     * `attempt + 1`.
     */
    public function retried(): int
    {
        return $this->acks[AckKind::Retried->name] ?? 0;
    }

    /**
     * How many deliveries were settled at all: every ack plus every dead letter.
     */
    public function settled(): int
    {
        return $this->acked() + array_sum(array_map(\count(...), $this->dead));
    }

    /**
     * Every declared queue, in declaration order.
     *
     * @return list<string>
     */
    public function declared(): array
    {
        return array_keys($this->configs);
    }

    /** How many consumers are registered right now. */
    public function consumerCount(): int
    {
        return \count($this->consumers);
    }

    /**
     * Record the ack half of a settle, and why it happened: the kind is what
     * tells a success apart from the ack a retry or a deferral performs.
     *
     * @internal to {@see MemoryDelivery}
     */
    public function recordAck(AckKind $kind = AckKind::Succeeded): void
    {
        $this->acks[$kind->name] = ($this->acks[$kind->name] ?? 0) + 1;
    }

    /**
     * Move `$envelope` onto its queue's dead list. Publish-before-ack, so this
     * runs before the original is acked and fails on a closed backend.
     *
     * @internal to {@see MemoryDelivery}
     */
    public function recordDead(Envelope $envelope, string $reason): void
    {
        $this->assertOpen();
        $this->assertDeclared($envelope->queue);

        $this->dead[$envelope->queue][] = ['envelope' => $envelope, 'reason' => $reason];
    }

    /**
     * Queue `$envelope` behind every pending envelope that is at least as
     * important and ahead of the rest, so equal priorities stay FIFO and a
     * deferred job overtakes the normal backlog.
     */
    private function enqueueByPriority(Envelope $envelope): void
    {
        $queue = $this->pending[$envelope->queue] ?? [];
        $position = \count($queue);
        while ($position > 0 && $queue[$position - 1]->priority < $envelope->priority) {
            --$position;
        }
        array_splice($queue, $position, 0, [$envelope]);

        /** @var list<Envelope> $queue */
        $this->pending[$envelope->queue] = $queue;
    }

    /** Put `$envelope` out of sight until the clock has moved past `$delay`. */
    private function hold(Envelope $envelope, Duration $delay): void
    {
        $this->held[$envelope->queue][] = [
            'envelope' => $envelope,
            'dueAtMs' => $this->clock->nowMs() + $delay->toMs(),
        ];
    }

    /**
     * Move every hold that is due onto its queue, the one that came due first
     * ahead of the rest.
     *
     * Several holds can fall due in the same call — the clock jumped, or a whole
     * batch was released at once — and a broker would have let them out in the
     * order their TTLs expired, not in the order they went into hold. Sorting by
     * `dueAtMs` reproduces that; PHP's sort is stable, so holds due at the same
     * millisecond keep their insertion order.
     */
    private function releaseDue(): void
    {
        $now = $this->clock->nowMs();
        foreach ($this->held as $queue => $entries) {
            $keep = [];
            $due = [];
            foreach ($entries as $entry) {
                if ($entry['dueAtMs'] <= $now) {
                    $due[] = $entry;
                } else {
                    $keep[] = $entry;
                }
            }
            if ($due === []) {
                continue;
            }
            $this->held[$queue] = $keep;
            usort($due, static fn(array $a, array $b): int => $a['dueAtMs'] <=> $b['dueAtMs']);
            foreach ($due as $entry) {
                $this->enqueueByPriority($entry['envelope']);
            }
        }
    }

    /**
     * Hand the head of its queue to each consumer that has one waiting.
     *
     * @return int how many deliveries were dispatched
     */
    private function dispatchRound(): int
    {
        $dispatched = 0;
        foreach ($this->consumers as $consumer) {
            $queue = $consumer['queue'];
            $waiting = $this->pending[$queue] ?? [];
            if ($waiting === []) {
                continue;
            }

            $envelope = array_shift($waiting);
            $this->pending[$queue] = $waiting;
            ++$dispatched;
            $delivery = new MemoryDelivery($this, $envelope);
            try {
                ($consumer['onDelivery'])($delivery);
            } catch (\Throwable $e) {
                // A callback that throws never settled the delivery, so the
                // message would be lost. Put it back at the head of its queue,
                // the way Rust's `DeliveryReceiver::drop` requeues what a dropped
                // consumer never handed on.
                if (!$delivery->isSettled()) {
                    array_unshift($this->pending[$queue], $envelope);
                }

                throw $e;
            }
        }

        return $dispatched;
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw ShutDown::create();
        }
    }

    private function assertDeclared(string $queue): void
    {
        if (!isset($this->configs[$queue])) {
            throw UnknownQueue::named($queue);
        }
    }
}
