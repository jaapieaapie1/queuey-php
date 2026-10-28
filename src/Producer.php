<?php

declare(strict_types=1);

namespace Queuey;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\JobBelongsToOtherQueueSet;
use Queuey\Internal\JobMetadata;

/**
 * Publishes jobs into one queue set.
 *
 * The queue set is a constructor argument rather than a type parameter, so the
 * check Rust makes when the crate is compiled — a job of another application
 * cannot be enqueued here — happens at the moment of the enqueue instead, and
 * throws {@see JobBelongsToOtherQueueSet}.
 *
 * ```php
 * $producer = new Producer(AppQueues::class, $backend);
 * $id = $producer->enqueue(new SendEmail('a@b.c'));
 * ```
 */
final class Producer
{
    /** @var class-string<QueueSet> */
    private readonly string $queueSet;

    private readonly LoggerInterface $logger;

    private readonly Clock $clock;

    /**
     * Create a producer and declare every queue of `$queues`.
     *
     * @param class-string<QueueSet> $queues
     * @param bool                   $declare internal; {@see self::undeclared()} is the documented way to pass `false`
     *
     * @throws InvalidConfiguration when `$queues` is not a queue set
     */
    public function __construct(
        string $queues,
        private readonly Backend $backend,
        ?LoggerInterface $logger = null,
        ?Clock $clock = null,
        bool $declare = true,
    ) {
        if (!is_a($queues, QueueSet::class, true)) {
            throw new InvalidConfiguration(\sprintf('%s is not a %s', $queues, QueueSet::class));
        }

        $this->queueSet = $queues;
        $this->logger = $logger ?? new NullLogger();
        $this->clock = $clock ?? new SystemClock();

        if ($declare) {
            $this->backend->declare(...array_map(
                static fn(QueueSet $queue): QueueConfig => $queue->config(),
                $queues::all(),
            ));
        }
    }

    /**
     * A producer that declares nothing: the queues must already exist.
     *
     * Cheaper at startup, and the right choice when the queues are owned by
     * somebody else — but a backend that has never seen a queue cannot build the
     * hold it needs, so {@see self::enqueueAfter()} and {@see self::defer()} may
     * fail with {@see \Queuey\Exception\UnknownQueue}.
     *
     * @param class-string<QueueSet> $queues
     */
    public static function undeclared(
        string $queues,
        Backend $backend,
        ?LoggerInterface $logger = null,
        ?Clock $clock = null,
    ): self {
        return new self($queues, $backend, $logger, $clock, declare: false);
    }

    /**
     * Publish `$job` to the queue its `#[OnQueue]` names. Returns the job id.
     *
     * @throws JobBelongsToOtherQueueSet when the job's queue is not part of this producer's set
     */
    public function enqueue(Job $job, ?EnqueueOptions $options = null): string
    {
        $options ??= EnqueueOptions::now();
        $queue = $this->queueFor($job);

        $envelope = Envelope::new($job, $this->clock);
        if ($options->correlationId !== null) {
            $envelope = $envelope->withCorrelationId($options->correlationId);
        }

        $delay = $options->delay();
        if ($options->isDeferred() && $delay !== null) {
            // Not `Envelope::deferred()`: a producer-side deferral has never run,
            // so its `deferrals` counter must stay at `0` (INTEROP-SPEC §1).
            $envelope = $envelope->withPriority($queue->config()->maxPriority ?? 0);
            $this->backend->defer($envelope, $delay);
        } else {
            $this->backend->publish($envelope, $delay);
        }

        $this->logger->debug('job enqueued', [
            'job_id' => $envelope->jobId,
            'job_type' => $envelope->jobType,
            'queue' => $envelope->queue,
            'delay_ms' => $delay?->toMs() ?? 0,
            'deferred' => $options->isDeferred(),
        ]);

        return $envelope->jobId;
    }

    /**
     * Publish `$job` to become visible after `$delay`, behind the backlog
     * (priority `0`), exactly like a retry. Returns the job id.
     */
    public function enqueueAfter(Job $job, Duration $delay): string
    {
        return $this->enqueue($job, EnqueueOptions::after($delay));
    }

    /**
     * Hold `$job` for `$delay` and release it at the *front* of its queue, with
     * the highest priority the queue supports (`0` when it has none).
     *
     * The producer-side twin of a handler throwing
     * {@see \Queuey\Exception\JobDeferred}: the envelope is still a first-attempt
     * one (`attempt = 1`, `deferrals = 0`), it only jumps the queue.
     */
    public function defer(Job $job, Duration $delay): string
    {
        return $this->enqueue($job, EnqueueOptions::deferred($delay));
    }

    /** The backend this producer publishes through. */
    public function backend(): Backend
    {
        return $this->backend;
    }

    /** The queue set this producer is pinned to. */
    public function queueSet(): string
    {
        return $this->queueSet;
    }

    /**
     * The queue `$job` belongs to, once it is certain it belongs to this set.
     *
     * @throws JobBelongsToOtherQueueSet
     */
    private function queueFor(Job $job): QueueSet
    {
        $queue = JobMetadata::for($job::class)->queue;
        if ($queue::class !== $this->queueSet) {
            throw new JobBelongsToOtherQueueSet(\sprintf(
                '%s is published to %s::%s, but this producer publishes to %s',
                $job::class,
                $queue::class,
                $queue->name,
                $this->queueSet,
            ));
        }

        return $queue;
    }
}
