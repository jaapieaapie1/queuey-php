<?php

declare(strict_types=1);

namespace Queuey;

/**
 * Everything the worker knows about the execution a handler is inside.
 *
 * The constructor is public on purpose: a handler's branching on `attempt`,
 * `deferrals` or `age` is worth a unit test, and that test should not have to
 * stand up a backend and a worker to produce one of these.
 */
final class JobContext
{
    /** Time since the job was first enqueued. */
    public readonly Duration $age;

    /**
     * @param string      $jobId         stable across every attempt and deferral
     * @param int         $attempt       1-based
     * @param int         $maxAttempts   total attempts the resolved retry policy allows
     * @param int         $deferrals     how often the job deferred itself; independent of `$attempt`
     * @param int         $priority      the broker priority this delivery arrived with
     * @param string|null $correlationId carried through retries and deferrals, never interpreted
     */
    public function __construct(
        public readonly string $jobId,
        public readonly string $jobType,
        public readonly string $queue,
        public readonly int $attempt,
        public readonly int $maxAttempts,
        public readonly int $deferrals = 0,
        public readonly int $priority = 0,
        ?Duration $age = null,
        public readonly ?string $correlationId = null,
    ) {
        $this->age = $age ?? Duration::zero();
    }

    /** Whether a failure now means the job is dead-lettered. */
    public function isLastAttempt(): bool
    {
        return $this->attempt >= $this->maxAttempts;
    }
}
