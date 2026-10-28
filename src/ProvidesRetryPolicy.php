<?php

declare(strict_types=1);

namespace Queuey;

/**
 * Optional companion to {@see JobHandler}, checked with `instanceof`.
 *
 * This is the highest-precedence source of a retry policy (INTEROP-SPEC §5) and
 * the only one that gets to look at the decoded payload. Returning `null` falls
 * through to the worker's overrides, the job's own policy and the queue's.
 *
 * @template T of Job
 */
interface ProvidesRetryPolicy
{
    /**
     * @param T $job
     */
    public function retryPolicyFor(Job $job): ?RetryPolicy;
}
