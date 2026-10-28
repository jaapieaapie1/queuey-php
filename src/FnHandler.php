<?php

declare(strict_types=1);

namespace Queuey;

/**
 * A {@see JobHandler} made from a callable, for handlers too small to deserve a class.
 *
 * ```php
 * FnHandler::for(SendEmail::class, function (SendEmail $job, JobContext $ctx): void {
 *     $mailer->send($job->to);
 * });
 * ```
 *
 * @template T of Job
 *
 * @implements JobHandler<T>
 */
final class FnHandler implements JobHandler
{
    /** @var \Closure(T, JobContext): void */
    private readonly \Closure $fn;

    /**
     * @param class-string<T>             $jobClass
     * @param callable(T, JobContext):void $fn
     */
    private function __construct(private readonly string $jobClass, callable $fn)
    {
        $this->fn = $fn(...);
    }

    /**
     * @template J of Job
     *
     * @param class-string<J>              $jobClass
     * @param callable(J, JobContext):void $fn
     *
     * @return self<J>
     */
    public static function for(string $jobClass, callable $fn): self
    {
        return new self($jobClass, $fn);
    }

    /**
     * @return class-string<T>
     */
    public function jobClass(): string
    {
        return $this->jobClass;
    }

    public function handle(Job $job, JobContext $ctx): void
    {
        ($this->fn)($job, $ctx);
    }
}
