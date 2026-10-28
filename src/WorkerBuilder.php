<?php

declare(strict_types=1);

namespace Queuey;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Queuey\Exception\DuplicateHandler;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\JobBelongsToOtherQueueSet;
use Queuey\Internal\JobMetadata;

/**
 * Configures a {@see Worker}. Created by {@see Worker::builder()}.
 *
 * Every setter mutates and returns the same builder, so the calls chain:
 *
 * ```php
 * $worker = Worker::builder(AppQueues::class, $backend)
 *     ->handler(new SendEmailHandler($mailer))
 *     ->retryOverride(AppQueues::Emails, RetryPolicy::fixed(5, Duration::seconds(10)))
 *     ->closeBackendOnShutdown(true)
 *     ->build();
 * ```
 */
final class WorkerBuilder
{
    /**
     * Registered handlers, each already erased to the closure {@see Worker} runs.
     *
     * PHP generics live in PHPDoc only, so a map of `JobHandler<SomeJob>` values
     * cannot be typed without losing the tie between a handler and its job class.
     * Erasing each handler here — where its concrete job type is still known —
     * keeps decode, policy choice and `handle()` inside one scope that type-checks,
     * and leaves the worker with plain closures.
     *
     * The decode is its own stage, returning the two that follow bound to the job
     * it produced: only a failure to decode may be reported as a decode error, so
     * the worker has to be able to run the other two outside that catch.
     *
     * @var list<array{jobClass: class-string<Job>, handlerClass: class-string, decode: \Closure(Envelope): array{policyFor: \Closure(): ?RetryPolicy, run: \Closure(JobContext): void}}>
     */
    private array $handlers = [];

    /** @var array<string, RetryPolicy> broker queue name to override */
    private array $queueRetry = [];

    /** @var array<string, RetryPolicy> job type (`job_type` on the wire) to override */
    private array $jobRetry = [];

    /** @var list<QueueSet>|null the subset to consume, or `null` for every queue of the set */
    private ?array $only = null;

    private ?DeadLetterHook $hook = null;

    private ?Duration $jobTimeout = null;

    private bool $closeBackend = false;

    private ?int $maxJobs = null;

    private ?LoggerInterface $logger = null;

    private ?Clock $clock = null;

    /**
     * @param class-string<QueueSet> $queues
     *
     * @internal use {@see Worker::builder()}
     */
    public function __construct(
        private readonly string $queues,
        private readonly Backend $backend,
    ) {}

    /**
     * Register a handler for one job type.
     *
     * The job's `#[OnQueue]` queue has to belong to this worker's queue set;
     * {@see self::build()} refuses it otherwise, which is the runtime stand-in
     * for the bound Rust checks when the crate is compiled.
     *
     * @template T of Job
     *
     * @param JobHandler<T> $handler
     */
    public function handler(JobHandler $handler): self
    {
        $jobClass = $handler->jobClass();

        $this->handlers[] = [
            'jobClass' => $jobClass,
            'handlerClass' => $handler::class,
            'decode' => static function (Envelope $envelope) use ($handler, $jobClass): array {
                // Decode the payload, and hand back the two stages that need the
                // job it produced: the policy choice a `ProvidesRetryPolicy`
                // handler makes (INTEROP-SPEC §5) and the call itself. Both are
                // bound here because both need the job's concrete type, and both
                // are separate from the decode because a handler that throws while
                // choosing a policy has not failed to decode anything.
                $job = $envelope->decode($jobClass);

                return [
                    'policyFor' => static fn(): ?RetryPolicy => $handler instanceof ProvidesRetryPolicy
                        ? $handler->retryPolicyFor($job)
                        : null,
                    'run' => static fn(JobContext $ctx) => $handler->handle($job, $ctx),
                ];
            },
        ];

        return $this;
    }

    /**
     * Consume only these queues instead of every queue of the set. Duplicates
     * are ignored; calling this twice replaces the earlier selection.
     */
    public function queues(QueueSet ...$only): self
    {
        foreach ($only as $queue) {
            $this->assertOwnQueue($queue);
        }
        $this->only = array_values($only);

        return $this;
    }

    /**
     * Replace the retry policy of every job consumed from `$queue`, whatever its
     * attributes declared.
     *
     * How many attempts a flaky dependency deserves is an operational decision
     * that belongs in whatever this process reads its configuration from, not in
     * an attribute that needs a release to change. The override applies to this
     * worker only: a producer never consults a policy, and another worker on the
     * same queue keeps its own.
     *
     * An override for a queue no registered handler consumes is inert, and
     * {@see self::build()} says so at debug level.
     */
    public function retryOverride(QueueSet $queue, RetryPolicy $policy): self
    {
        $this->assertOwnQueue($queue);
        $this->queueRetry[$queue->queueName()] = $policy;

        return $this;
    }

    /**
     * Replace the retry policy of one job type. Beats {@see self::retryOverride()},
     * so a queue-wide override can be set first and one job pulled out of it.
     *
     * The job has to be published to this worker's queue set, like the one a
     * handler claims: an override for a job this worker could never consume is a
     * mistake worth naming rather than a line that quietly does nothing.
     *
     * @param class-string<Job> $jobClass
     *
     * @throws JobBelongsToOtherQueueSet when `$jobClass` is published to another queue set
     */
    public function jobRetryOverride(string $jobClass, RetryPolicy $policy): self
    {
        $meta = JobMetadata::for($jobClass);
        if ($meta->queue::class !== $this->queues) {
            throw new JobBelongsToOtherQueueSet(\sprintf(
                '%s is published to %s::%s, but this worker consumes %s',
                $jobClass,
                $meta->queue::class,
                $meta->queue->name,
                $this->queues,
            ));
        }

        $this->jobRetry[$meta->name] = $policy;

        return $this;
    }

    /**
     * Call `$hook` for every job this worker dead-letters, whatever the cause.
     *
     * The one outcome that usually needs acting on is also the one no handler can
     * observe on its own: a job with no handler or an undecodable payload never
     * reaches user code, and a handler that fails cannot know whether the policy
     * will retry or give up. Registering a second hook replaces the first.
     */
    public function onDeadLetter(DeadLetterHook $hook): self
    {
        $this->hook = $hook;

        return $this;
    }

    /**
     * Interrupt a handler that runs longer than `$timeout` and treat it as a
     * retryable failure.
     *
     * Synchronous PHP has no way to cancel a call, so this is `pcntl_alarm()`
     * plus a `SIGALRM` handler that throws
     * {@see \Queuey\Exception\RetryableFailure} into whatever the handler is
     * doing — which means it needs **ext-pcntl** ({@see self::build()} throws
     * without it), its resolution is whole seconds (rounded up), and a handler
     * blocked inside a C call that does not return to the VM is not interrupted.
     */
    public function jobTimeout(Duration $timeout): self
    {
        if ($timeout->isZero()) {
            throw new InvalidConfiguration('`jobTimeout` must be greater than zero');
        }
        $this->jobTimeout = $timeout;

        return $this;
    }

    /**
     * Stop the run loop once `$maxJobs` deliveries have been dispatched.
     *
     * For tests and for supervisors that recycle their workers; a round that has
     * already started is always finished, so a worker consuming several queues
     * may process a few more.
     */
    public function maxJobs(int $maxJobs): self
    {
        if ($maxJobs < 1) {
            throw new InvalidConfiguration(\sprintf('`maxJobs` must be at least 1, got %d', $maxJobs));
        }
        $this->maxJobs = $maxJobs;

        return $this;
    }

    /**
     * Close the backend when {@see Worker::run()} returns. Defaults to `false`:
     * the backend is usually shared with a {@see Producer} that outlives the
     * worker, so closing it is the caller's decision.
     */
    public function closeBackendOnShutdown(bool $close = true): self
    {
        $this->closeBackend = $close;

        return $this;
    }

    public function logger(LoggerInterface $logger): self
    {
        $this->logger = $logger;

        return $this;
    }

    /** The clock used for job age and for {@see Worker::runFor()}. */
    public function clock(Clock $clock): self
    {
        $this->clock = $clock;

        return $this;
    }

    /**
     * Resolve the retry policies, declare the selected queues and produce the worker.
     *
     * @throws DuplicateHandler          when two handlers claim the same job type
     * @throws JobBelongsToOtherQueueSet when a handler's job is published to another queue set
     * @throws InvalidConfiguration      when a job timeout was asked for without ext-pcntl
     */
    public function build(): Worker
    {
        $logger = $this->logger ?? new NullLogger();

        if ($this->jobTimeout !== null && !\extension_loaded('pcntl')) {
            throw new InvalidConfiguration(
                'jobTimeout() needs ext-pcntl: synchronous PHP cannot interrupt a handler without a signal',
            );
        }

        $registrations = [];
        foreach ($this->handlers as $handler) {
            $meta = JobMetadata::for($handler['jobClass']);
            if ($meta->queue::class !== $this->queues) {
                throw new JobBelongsToOtherQueueSet(\sprintf(
                    '%s handles %s, which is published to %s::%s, but this worker consumes %s',
                    $handler['handlerClass'],
                    $handler['jobClass'],
                    $meta->queue::class,
                    $meta->queue->name,
                    $this->queues,
                ));
            }
            if (isset($registrations[$meta->name])) {
                throw DuplicateHandler::forJobType($meta->name);
            }

            $config = $meta->queue->config();
            $registrations[$meta->name] = [
                'decode' => $handler['decode'],
                'queue' => $config->name,
                // INTEROP-SPEC §5: job override, then queue override, then what the
                // attributes declared for the job, then for the queue (whose own
                // default is `RetryPolicy::none()`).
                'policy' => $this->jobRetry[$meta->name]
                    ?? $this->queueRetry[$config->name]
                    ?? $meta->retry
                    ?? $config->retry,
                'deferPriority' => $config->maxPriority ?? 0,
            ];
        }

        $this->reportInertOverrides($registrations, $logger);

        $configs = [];
        foreach ($this->only ?? $this->queues::all() as $queue) {
            $config = $queue->config();
            if (!isset($configs[$config->name])) {
                $configs[$config->name] = $config;
            }
        }
        $this->backend->declare(...array_values($configs));

        return new Worker(
            backend: $this->backend,
            registrations: $registrations,
            queues: array_values($configs),
            hook: $this->hook,
            logger: $logger,
            clock: $this->clock ?? new SystemClock(),
            jobTimeout: $this->jobTimeout,
            maxJobs: $this->maxJobs,
            closeBackend: $this->closeBackend,
        );
    }

    /**
     * Say at debug level which overrides will never fire: an override that
     * matched nothing is a typo far more often than it is intentional.
     *
     * @param array<string, array{decode: \Closure(Envelope): array{policyFor: \Closure(): ?RetryPolicy, run: \Closure(JobContext): void}, queue: string, policy: RetryPolicy, deferPriority: int}> $registrations
     */
    private function reportInertOverrides(array $registrations, LoggerInterface $logger): void
    {
        foreach (array_keys($this->jobRetry) as $jobType) {
            if (!isset($registrations[$jobType])) {
                $logger->debug('retry override for an unregistered job type', ['job_type' => $jobType]);
            }
        }

        $consumed = array_column($registrations, 'queue');
        foreach (array_keys($this->queueRetry) as $queue) {
            if (!\in_array($queue, $consumed, true)) {
                $logger->debug('retry override for a queue with no registered handler', ['queue' => $queue]);
            }
        }
    }

    private function assertOwnQueue(QueueSet $queue): void
    {
        if ($queue::class !== $this->queues) {
            throw new InvalidConfiguration(\sprintf(
                '%s::%s belongs to another queue set; this worker consumes %s',
                $queue::class,
                $queue->name,
                $this->queues,
            ));
        }
    }
}
