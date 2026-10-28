<?php

declare(strict_types=1);

namespace Queuey\Internal;

use Queuey\Attributes\OnQueue;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Job;
use Queuey\QueueSet;
use Queuey\RetryPolicy;

/**
 * What `#[OnQueue]` says about a job class, resolved once and cached.
 *
 * @internal
 */
final class JobMetadata
{
    /** @var array<class-string, self> */
    private static array $cache = [];

    /**
     * @param string $name the `job_type` on the wire; the FQCN unless `#[OnQueue(name: ...)]` says otherwise
     */
    private function __construct(
        public readonly QueueSet $queue,
        public readonly string $name,
        public readonly ?RetryPolicy $retry,
    ) {}

    /**
     * @param class-string $jobClass
     *
     * @throws InvalidConfiguration when the class is not a job, or carries no usable `#[OnQueue]`
     */
    public static function for(string $jobClass): self
    {
        return self::$cache[$jobClass] ??= self::resolve($jobClass);
    }

    /** Drop everything resolved so far. For tests that define job classes on the fly. */
    public static function reset(): void
    {
        self::$cache = [];
    }

    /**
     * @param class-string $jobClass
     */
    private static function resolve(string $jobClass): self
    {
        if (!is_a($jobClass, Job::class, true)) {
            throw new InvalidConfiguration(\sprintf('%s does not implement %s', $jobClass, Job::class));
        }

        $attributes = (new \ReflectionClass($jobClass))->getAttributes(OnQueue::class);
        if ($attributes === []) {
            throw new InvalidConfiguration(\sprintf(
                '%s has no #[OnQueue(...)]: a job must name the queue it is published to',
                $jobClass,
            ));
        }
        if (\count($attributes) > 1) {
            throw new InvalidConfiguration(\sprintf('%s carries more than one #[OnQueue]', $jobClass));
        }

        try {
            $onQueue = $attributes[0]->newInstance();
        } catch (\Throwable $e) {
            throw new InvalidConfiguration(\sprintf('#[OnQueue] on %s: %s', $jobClass, $e->getMessage()), 0, $e);
        }

        try {
            $retry = $onQueue->retry?->toPolicy();
        } catch (\Throwable $e) {
            throw new InvalidConfiguration(\sprintf('#[OnQueue] on %s: %s', $jobClass, $e->getMessage()), 0, $e);
        }

        return new self($onQueue->queue, $onQueue->name ?? $jobClass, $retry);
    }
}
