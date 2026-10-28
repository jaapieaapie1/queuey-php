<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Backoff;
use Queuey\Duration;
use Queuey\Exception\DuplicateHandler;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\JobBelongsToOtherQueueSet;
use Queuey\FnHandler;
use Queuey\FrozenClock;
use Queuey\JobContext;
use Queuey\Memory\MemoryBackend;
use Queuey\RetryPolicy;
use Queuey\Tests\Fixtures\BackedQueues;
use Queuey\Tests\Fixtures\Digest;
use Queuey\Tests\Fixtures\Foreign;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Ping;
use Queuey\Tests\Fixtures\RecordingLogger;
use Queuey\Tests\Fixtures\TestQueues;
use Queuey\Worker;
use Queuey\WorkerBuilder;

it('declares every queue of the set on build', function (): void {
    $backend = workerBuilderBackend();

    $worker = workerBuilderBuilder($backend)->build();

    expect($backend->declared())->toBe(['test.alpha', 'test.beta-queue', 'test.send_emails', 'test.http_calls']);
    expect($worker->queues())->toHaveCount(4);
    expect($backend->consumerCount())->toBe(0, 'consumers are registered by run(), not by build()');
});

it('restricts the declared subset and deduplicates it', function (): void {
    $backend = workerBuilderBackend();

    $worker = workerBuilderBuilder($backend)
        ->queues(TestQueues::Alpha, TestQueues::Beta, TestQueues::Alpha)
        ->build();

    expect($backend->declared())->toBe(['test.alpha', 'test.beta-queue']);
    expect(array_map(static fn($config): string => $config->name, $worker->queues()))
        ->toBe(['test.alpha', 'test.beta-queue']);
});

it('refuses a queue of another set', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend());

    expect(static fn(): WorkerBuilder => $builder->queues(BackedQueues::Emails))
        ->toThrow(InvalidConfiguration::class, 'belongs to another queue set');
});

it('refuses a retry override for a queue of another set', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend());

    expect(static fn(): WorkerBuilder => $builder->retryOverride(BackedQueues::Emails, RetryPolicy::exponential(3)))
        ->toThrow(InvalidConfiguration::class);
});

it('refuses a retry override for a job of another queue set', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend());

    // Overriding the policy of a job this worker can never consume is a typo,
    // not a configuration choice.
    expect(static fn(): WorkerBuilder => $builder->jobRetryOverride(Foreign::class, RetryPolicy::exponential(3)))
        ->toThrow(JobBelongsToOtherQueueSet::class, 'this worker consumes ' . TestQueues::class);
});

it('fails the build on two handlers for one job type', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend())
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null));

    expect(static fn(): Worker => $builder->build())
        ->toThrow(DuplicateHandler::class, '`test::Greet` registered twice');
});

it('fails the build on a handler for another queue set', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend())
        ->handler(FnHandler::for(Foreign::class, static fn(Foreign $job, JobContext $ctx) => null));

    expect(static fn(): Worker => $builder->build())
        ->toThrow(JobBelongsToOtherQueueSet::class, 'this worker consumes ' . TestQueues::class);
});

it('applies the queue policy when nothing overrides it', function (): void {
    $worker = workerBuilderBuilder(workerBuilderBackend())->handler(workerBuilderDigest())->build();

    // `test.send_emails` declares `retry(max_attempts = 3, fixed 2s)`.
    workerBuilderExpectPolicy(RetryPolicy::fixed(3, Duration::seconds(2)), $worker, Digest::class);
});

it('falls back to none for a queue without a retry', function (): void {
    $worker = workerBuilderBuilder(workerBuilderBackend())->handler(workerBuilderGreet())->build();

    workerBuilderExpectPolicy(RetryPolicy::none(), $worker, 'test::Greet');
});

it('lets a policy declared on the job beat the queue', function (): void {
    $worker = workerBuilderBuilder(workerBuilderBackend())->handler(workerBuilderPing())->build();

    workerBuilderExpectPolicy(
        RetryPolicy::new(5, Backoff::exponential(base: Duration::ms(250))),
        $worker,
        Ping::class,
    );
});

it('lets a queue override beat a policy declared on the job', function (): void {
    $override = RetryPolicy::fixed(7, Duration::seconds(1));
    $worker = workerBuilderBuilder(workerBuilderBackend())
        ->handler(workerBuilderPing())
        ->retryOverride(TestQueues::Beta, $override)
        ->build();

    workerBuilderExpectPolicy($override, $worker, Ping::class);
});

it('lets a job override beat a queue override', function (): void {
    $queueWide = RetryPolicy::fixed(7, Duration::seconds(1));
    $justThisJob = RetryPolicy::fixed(2, Duration::ms(50));

    $worker = workerBuilderBuilder(workerBuilderBackend())
        ->handler(workerBuilderGreet())
        ->handler(workerBuilderDigest())
        ->retryOverride(TestQueues::Alpha, $queueWide)
        ->jobRetryOverride(Greet::class, $justThisJob)
        ->build();

    workerBuilderExpectPolicy($justThisJob, $worker, 'test::Greet');
    workerBuilderExpectPolicy(RetryPolicy::fixed(3, Duration::seconds(2)), $worker, Digest::class);
});

it('leaves an override that matches nothing inert and logs it at debug', function (): void {
    $logger = new RecordingLogger();
    $worker = workerBuilderBuilder(workerBuilderBackend())
        ->logger($logger)
        ->handler(workerBuilderGreet())
        ->retryOverride(TestQueues::HTTPCalls, RetryPolicy::exponential(9))
        ->jobRetryOverride(Ping::class, RetryPolicy::exponential(9))
        ->build();

    workerBuilderExpectPolicy(RetryPolicy::none(), $worker, 'test::Greet');
    expect($worker->retryPolicyFor(Ping::class))->toBeNull();
    expect($logger->messages('debug'))->toBe([
        'retry override for an unregistered job type',
        'retry override for a queue with no registered handler',
    ]);
});

it('requires a positive max jobs', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend());

    expect(static fn(): WorkerBuilder => $builder->maxJobs(0))->toThrow(InvalidConfiguration::class);
});

it('refuses a zero job timeout', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend());

    expect(static fn(): WorkerBuilder => $builder->jobTimeout(Duration::zero()))
        ->toThrow(InvalidConfiguration::class);
});

it('needs ext-pcntl for a job timeout', function (): void {
    $builder = workerBuilderBuilder(workerBuilderBackend())
        ->handler(workerBuilderGreet())
        ->jobTimeout(Duration::seconds(1));

    if (\extension_loaded('pcntl')) {
        expect($builder->build())->toBeInstanceOf(Worker::class);

        return;
    }

    expect(static fn(): Worker => $builder->build())
        ->toThrow(InvalidConfiguration::class, 'needs ext-pcntl');
});

function workerBuilderBackend(): MemoryBackend
{
    return new MemoryBackend(new FrozenClock(1_700_000_000_000));
}

function workerBuilderBuilder(MemoryBackend $backend): WorkerBuilder
{
    return Worker::builder(TestQueues::class, $backend);
}

/**
 * @return FnHandler<Greet>
 */
function workerBuilderGreet(): FnHandler
{
    return FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null);
}

/**
 * @return FnHandler<Ping>
 */
function workerBuilderPing(): FnHandler
{
    return FnHandler::for(Ping::class, static fn(Ping $job, JobContext $ctx) => null);
}

/**
 * @return FnHandler<Digest>
 */
function workerBuilderDigest(): FnHandler
{
    return FnHandler::for(Digest::class, static fn(Digest $job, JobContext $ctx) => null);
}

function workerBuilderExpectPolicy(RetryPolicy $expected, Worker $worker, string $jobType): void
{
    $actual = $worker->retryPolicyFor($jobType);

    Assert::assertInstanceOf(RetryPolicy::class, $actual, \sprintf('no policy resolved for %s', $jobType));
    expect($expected->equals($actual))
        ->toBeTrue(\sprintf('expected %d attempts, got %d', $expected->maxAttempts, $actual->maxAttempts));
}
