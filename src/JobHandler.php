<?php

declare(strict_types=1);

namespace Queuey;

/**
 * Processes jobs of exactly one type. Register with `WorkerBuilder::handler()`.
 *
 * Outcomes are signalled with exceptions: return normally for success, throw
 * {@see \Queuey\Exception\FatalFailure} to dead-letter at once,
 * {@see \Queuey\Exception\JobDeferred} to come back later without burning an
 * attempt, and {@see \Queuey\Exception\RetryableFailure} — or anything else — to
 * hand control to the retry policy.
 *
 * @template T of Job
 */
interface JobHandler
{
    /**
     * The one job class this handler processes.
     *
     * Not static, so {@see FnHandler} can report a class chosen at runtime.
     *
     * @return class-string<T>
     */
    public function jobClass(): string;

    /**
     * @param T $job
     */
    public function handle(Job $job, JobContext $ctx): void;
}
