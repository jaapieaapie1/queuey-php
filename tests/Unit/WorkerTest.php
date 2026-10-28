<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Backend;
use Queuey\DeadLetter;
use Queuey\DeadLetterCause;
use Queuey\Delivery;
use Queuey\Duration;
use Queuey\EnqueueOptions;
use Queuey\Envelope;
use Queuey\Exception\BackendException;
use Queuey\Exception\ConnectionLost;
use Queuey\Exception\ConsumerStopped;
use Queuey\Exception\FatalFailure;
use Queuey\Exception\JobDeferred;
use Queuey\Exception\RetryableFailure;
use Queuey\Exception\ShutDown;
use Queuey\FnDeadLetterHook;
use Queuey\FnHandler;
use Queuey\FrozenClock;
use Queuey\Job;
use Queuey\JobContext;
use Queuey\JobHandler;
use Queuey\Memory\MemoryBackend;
use Queuey\Producer;
use Queuey\ProvidesRetryPolicy;
use Queuey\QueueConfig;
use Queuey\RetryPolicy;
use Queuey\Tests\Fixtures\Digest;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Ping;
use Queuey\Tests\Fixtures\RecordingLogger;
use Queuey\Tests\Fixtures\TestQueues;
use Queuey\Worker;
use Queuey\WorkerBuilder;
use Queuey\WorkerHandle;

const WORKER_ALPHA = 'test.alpha';

const WORKER_MAIL = 'test.send_emails';

it('acks a successful job', function (): void {
    [$backend, $producer, $builder, , $logger] = workerFixture();
    $producer->enqueue(new Greet('ada'));

    $seen = null;
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx) use (&$seen): void {
            $seen = [$job->name, $ctx->attempt, $ctx->maxAttempts, $ctx->jobType, $ctx->queue, $ctx->priority];
        }))
        ->build();

    workerRunRounds($worker);

    expect($seen)->toBe(['ada', 1, 1, 'test::Greet', WORKER_ALPHA, 0]);
    expect($backend->acked())->toBe(1);
    expect($worker->handle()->processed())->toBe(1);
    expect($backend->pending(WORKER_ALPHA))->toBe([]);
    expect($backend->dead(WORKER_ALPHA))->toBe([]);
    expect($logger->messages('info'))->toContain('job succeeded');
});

it('keeps the identity of a retried job and brings it back one attempt later', function (): void {
    [$backend, $producer, $builder, $clock, $logger] = workerFixture();
    $id = $producer->enqueue(
        new Digest('ops'),
        EnqueueOptions::now()->withCorrelationId('trace-1'),
    );
    $first = $backend->pending(WORKER_MAIL)[0];

    $attempts = [];
    $worker = $builder()
        ->handler(FnHandler::for(Digest::class, static function (Digest $job, JobContext $ctx) use (&$attempts): void {
            $attempts[] = $ctx->attempt;
            if ($ctx->attempt === 1) {
                throw RetryableFailure::withMessage('downstream hiccup');
            }
        }))
        ->build();

    workerRunRounds($worker);

    // The queue declares `fixed(3, 2s)`, so the successor waits two seconds.
    $held = $backend->held(WORKER_MAIL);
    expect($held)->toHaveCount(1);
    expect($held[0]->attempt)->toBe(2);
    expect($held[0]->jobId)->toBe($id);
    expect($held[0]->enqueuedAtMs)->toBe($first->enqueuedAtMs);
    expect($held[0]->correlationId)->toBe('trace-1');
    expect($held[0]->priority)->toBe(0);

    $clock->advance(Duration::seconds(2));
    workerRunRounds($worker);

    expect($attempts)->toBe([1, 2]);
    expect($backend->acked())->toBe(2);
    expect($backend->dead(WORKER_MAIL))->toBe([]);
    expect($logger->messages('warning'))->toContain('job failed; retrying');
});

it('dead-letters a job that runs out of attempts', function (): void {
    [$backend, $producer, $builder, $clock, $logger] = workerFixture();
    $producer->enqueue(new Greet('doomed'));

    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw RetryableFailure::withMessage('boom');
        }))
        ->jobRetryOverride(Greet::class, RetryPolicy::fixed(2, Duration::seconds(1)))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);
    $clock->advance(Duration::seconds(1));
    workerRunRounds($worker);

    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    expect($letters[0]['reason'])->toBe('max attempts (2) exhausted after attempt 2: boom');
    expect($letters[0]['envelope']->attempt)->toBe(2);

    expect($dead)->toHaveCount(1);
    expect($dead[0]->cause)->toBe(DeadLetterCause::Exhausted);
    expect($dead[0]->maxAttempts)->toBe(2);
    expect($dead[0]->attempts())->toBe(2);
    expect($logger->messages('error'))->toContain('job failed; giving up');
});

it('dead-letters a fatal failure on its first attempt', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('nope'));

    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw FatalFailure::withMessage('address does not exist');
        }))
        ->jobRetryOverride(Greet::class, RetryPolicy::fixed(5, Duration::seconds(1)))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);

    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    expect($letters[0]['reason'])->toBe('fatal error: address does not exist');
    expect($backend->held(WORKER_ALPHA))->toBe([], 'a fatal failure never consults the policy');

    expect($dead)->toHaveCount(1);
    expect($dead[0]->cause)->toBe(DeadLetterCause::Fatal);
    expect($dead[0]->maxAttempts)->toBe(5);
});

it('dead-letters a job no handler claims', function (): void {
    [$backend, , $builder, $clock] = workerFixture();
    $backend->publish(Envelope::raw('test::Unknown', WORKER_ALPHA, (object) ['x' => 1], $clock));

    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);

    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    expect($letters[0]['reason'])->toBe('no handler for job type `test::Unknown`');

    expect($dead)->toHaveCount(1);
    expect($dead[0]->cause)->toBe(DeadLetterCause::NoHandler);
    expect($dead[0]->maxAttempts)->toBeNull('no handler means no policy either');
    expect($dead[0]->cause->reachedHandler())->toBeFalse();
});

it('dead-letters a payload that does not decode', function (): void {
    [$backend, , $builder, $clock] = workerFixture();
    $backend->publish(Envelope::raw('test::Greet', WORKER_ALPHA, (object) ['wrong' => 'field'], $clock));

    $ran = false;
    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function () use (&$ran): void {
            $ran = true;
        }))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);

    expect($ran)->toBeFalse('nothing decoded, so nothing ran');
    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    expect($letters[0]['reason'])->toStartWith('decode error: ');

    expect($dead)->toHaveCount(1);
    expect($dead[0]->cause)->toBe(DeadLetterCause::Decode);
    expect($dead[0]->maxAttempts)->toBe(1);
});

it('treats an unexpected throwable as retryable', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('surprise'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw new \RuntimeException('kaboom');
        }))
        ->build();

    workerRunRounds($worker);

    // `test.alpha` has no retry policy, so the first failure is also the last.
    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    expect($letters[0]['reason'])->toBe('max attempts (1) exhausted after attempt 1: RuntimeException: kaboom');
});

it('keeps the attempt on a deferral and comes back at the top priority of the queue', function (): void {
    [$backend, $producer, $builder, $clock, $logger] = workerFixture();
    $producer->enqueue(new Greet('early'));

    $seen = [];
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx) use (&$seen): void {
            $seen[] = [$ctx->attempt, $ctx->deferrals, $ctx->priority];
            if ($ctx->deferrals === 0) {
                throw JobDeferred::for(Duration::seconds(5), 'not yet');
            }
        }))
        ->build();

    workerRunRounds($worker);

    $held = $backend->held(WORKER_ALPHA);
    expect($held)->toHaveCount(1);
    expect($held[0]->attempt)->toBe(1, 'a deferral is not a failed attempt');
    expect($held[0]->deferrals)->toBe(1);
    expect($held[0]->priority)->toBe(10);
    expect($logger->messages('info'))->toContain('job deferred');

    $clock->advance(Duration::seconds(5));
    workerRunRounds($worker);

    expect($seen)->toBe([[1, 0, 0], [1, 1, 10]]);
    expect($backend->acked())->toBe(2);
    expect($backend->dead(WORKER_ALPHA))->toBe([]);
});

it('leaves a deferral on a queue without priorities at zero', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Ping(1));

    $worker = $builder()
        ->handler(FnHandler::for(Ping::class, static function (): void {
            throw JobDeferred::for(Duration::seconds(1), 'later');
        }))
        ->build();

    workerRunRounds($worker);

    $held = $backend->held('test.beta-queue');
    expect($held)->toHaveCount(1);
    expect($held[0]->priority)->toBe(0);
    expect($held[0]->deferrals)->toBe(1);
});

it('drops back to priority zero for a retry that follows a deferral', function (): void {
    [$backend, $producer, $builder, $clock] = workerFixture();
    $producer->enqueue(new Digest('ops'));

    $worker = $builder()
        ->handler(FnHandler::for(Digest::class, static function (Digest $job, JobContext $ctx): void {
            if ($ctx->deferrals === 0) {
                throw JobDeferred::for(Duration::seconds(1), 'warming up');
            }

            throw RetryableFailure::withMessage('still broken');
        }))
        ->build();

    workerRunRounds($worker);
    $clock->advance(Duration::seconds(1));
    workerRunRounds($worker);

    $held = $backend->held(WORKER_MAIL);
    expect($held)->toHaveCount(1);
    expect($held[0]->attempt)->toBe(2);
    expect($held[0]->deferrals)->toBe(1, 'the deferral is remembered');
    expect($held[0]->priority)->toBe(0, 'a retry must not keep jumping the backlog');
});

it('lets a handler pick a policy from the payload', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('vip'));
    $producer->enqueue(new Greet('plain'));

    $seen = [];
    $record = static function (string $name, int $maxAttempts) use (&$seen): void {
        $seen[$name] = $maxAttempts;
    };
    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(workerPolicyFromPayload($record))
        ->jobRetryOverride(Greet::class, RetryPolicy::none())
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker, 2);

    expect($seen)->toBe(['vip' => 3, 'plain' => 1], 'only the vip payload gets three attempts');
    expect($backend->held(WORKER_ALPHA))->toHaveCount(1, 'the vip job is coming back');
    expect($dead)->toHaveCount(1);
    expect($dead[0]->reason)->toBe('max attempts (1) exhausted after attempt 1: no thanks');
    expect($dead[0]->maxAttempts)->toBe(1, 'the hook reports the policy the handler chose');
});

it('runs the hook before the delivery is settled', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('doomed'));

    $observed = null;
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw FatalFailure::withMessage('nope');
        }))
        ->onDeadLetter(FnDeadLetterHook::for(static function (DeadLetter $letter) use ($backend, &$observed): void {
            $observed = $backend->dead('test.alpha');
        }))
        ->build();

    workerRunRounds($worker);

    expect($observed)->toBe([], 'the hook runs before the dead letter is published');
    expect($backend->dead(WORKER_ALPHA))->toHaveCount(1);
});

it('dead-letters anyway when the hook throws', function (): void {
    [$backend, $producer, $builder, , $logger] = workerFixture();
    $producer->enqueue(new Greet('doomed'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw FatalFailure::withMessage('nope');
        }))
        ->onDeadLetter(FnDeadLetterHook::for(static function (): void {
            throw new \LogicException('hook is broken');
        }))
        ->build();

    workerRunRounds($worker);

    expect($backend->dead(WORKER_ALPHA))->toHaveCount(1);
    expect($logger->messages('error'))->toContain('dead-letter hook failed; dead-lettering anyway');
    expect($worker->handle()->settleFailures())->toBe(0);
});

it('keeps the hook silent for jobs that succeed, retry or defer', function (): void {
    [, $producer, $builder, $clock] = workerFixture();
    $producer->enqueue(new Digest('ops'));

    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(FnHandler::for(Digest::class, static function (Digest $job, JobContext $ctx): void {
            match ($ctx->attempt) {
                1 => throw RetryableFailure::withMessage('once'),
                default => null,
            };
        }))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);
    $clock->advance(Duration::seconds(2));
    workerRunRounds($worker);

    expect($dead)->toBe([]);
});

it('counts a failure to settle and never throws it', function (): void {
    [$backend, $producer, , $clock, $logger] = workerFixture();
    $producer->enqueue(new Greet('ada'));

    $worker = Worker::builder(TestQueues::class, workerUnsettleableBackend($backend))
        ->clock($clock)
        ->logger($logger)
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->build();

    workerRunRounds($worker);

    expect($worker->handle()->settleFailures())->toBe(1);
    expect($worker->handle()->processed())->toBe(1);
    expect($backend->acked())->toBe(0);
    expect($logger->messages('error'))->toContain('failed to settle delivery');
});

it('stops the loop once maxJobs is reached', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('one'));
    $producer->enqueue(new Greet('two'));
    $producer->enqueue(new Greet('three'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->maxJobs(2)
        ->build();

    $worker->run();

    expect($worker->handle()->processed())->toBe(2);
    expect($backend->pending(WORKER_ALPHA))->toHaveCount(1);
    expect($backend->isClosed())->toBeFalse('the backend is left open by default');
    expect($backend->consumerCount())->toBe(0, 'the consumers are cancelled on the way out');
});

it('closes the backend on shutdown only when asked to', function (): void {
    [$backend, , $builder] = workerFixture();

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->closeBackendOnShutdown(true)
        ->build();

    workerRunRounds($worker);

    expect($backend->isClosed())->toBeTrue();
});

it('stops immediately when shutdown comes before the run', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('untouched'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->build();
    $worker->handle()->shutdown();
    $worker->run();

    expect($worker->handle()->isShutdown())->toBeTrue();
    expect($worker->handle()->processed())->toBe(0);
    expect($backend->pending(WORKER_ALPHA))->toHaveCount(1);
});

it('consumes only the queues it was restricted to', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('alpha'));
    $producer->enqueue(new Digest('mail'));

    $worker = $builder()
        ->queues(TestQueues::Alpha)
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->handler(FnHandler::for(Digest::class, static fn(Digest $job, JobContext $ctx) => null))
        ->build();

    workerRunRounds($worker, 2);

    expect($backend->pending(WORKER_ALPHA))->toBe([]);
    expect($backend->pending(WORKER_MAIL))->toHaveCount(1, 'the other queue is nobody\'s business');
    expect($worker->handle()->processed())->toBe(1);
});

it('stops runFor once the clock has passed the deadline', function (): void {
    [$backend, $producer, $builder, $clock] = workerFixture();
    $producer->enqueue(new Greet('one'));
    $producer->enqueue(new Greet('two'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function () use ($clock): void {
            $clock->advance(Duration::seconds(30));
        }))
        ->build();

    $worker->runFor(Duration::seconds(10));

    expect($worker->handle()->processed())->toBe(1);
    expect($backend->pending(WORKER_ALPHA))->toHaveCount(1);
});

it('carries a correlation id into every attempt and into the dead-letter hook', function (): void {
    [, $producer, $builder, $clock] = workerFixture();
    $producer->enqueue(new Greet('traced'), EnqueueOptions::now()->withCorrelationId('trace-xyz'));

    $seen = [];
    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx) use (&$seen): void {
            $seen[] = $ctx->correlationId;

            throw RetryableFailure::withMessage('nope');
        }))
        ->jobRetryOverride(Greet::class, RetryPolicy::fixed(2, Duration::seconds(1)))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);
    $clock->advance(Duration::seconds(1));
    workerRunRounds($worker);

    expect($seen)->toBe(['trace-xyz', 'trace-xyz']);
    expect($dead)->toHaveCount(1);
    expect($dead[0]->envelope->correlationId)->toBe('trace-xyz');
});

it('turns a job timeout into a retryable failure', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('slow'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            $until = microtime(true) + 5.0;
            while (microtime(true) < $until) {
                usleep(10_000);
            }
        }))
        ->jobTimeout(Duration::seconds(1))
        ->build();

    workerRunRounds($worker);

    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    expect($letters[0]['reason'])->toBe('max attempts (1) exhausted after attempt 1: job timed out after 1s');
})->skip(fn(): bool => !\extension_loaded('pcntl'), 'ext-pcntl is required for job timeouts');

it('asks for shutdown when a signal arrives', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('one'));
    $producer->enqueue(new Greet('two'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            posix_kill(posix_getpid(), \SIGTERM);
        }))
        ->build();

    $worker->run();

    expect($worker->handle()->isShutdown())->toBeTrue();
    expect($worker->handle()->processed())->toBe(1);
    expect($backend->pending(WORKER_ALPHA))->toHaveCount(1);
    expect(pcntl_signal_get_handler(\SIGTERM))
        ->toBe(\SIG_DFL, 'the handler installed for the run is restored afterwards');
})->skip(
    fn(): bool => !\extension_loaded('pcntl') || !\function_exists('posix_kill'),
    'ext-pcntl and ext-posix are required for signal handling',
);

it('ends the run with an error when the backend goes away', function (): void {
    [$backend, $producer, $builder, , $logger] = workerFixture();
    $producer->enqueue(new Greet('the last one'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function () use ($backend): void {
            // The connection goes away underneath the worker.
            $backend->close();
        }))
        ->build();

    try {
        $worker->run();
        Assert::fail('a backend that stopped delivering must end the run');
    } catch (ConsumerStopped $e) {
        expect($e->getPrevious())->toBeInstanceOf(ShutDown::class, 'the backend error is kept as the cause');
    }

    expect($worker->handle()->processed())->toBe(1, 'the delivery in hand was finished first');
    expect($logger->messages('error'))->toContain('consumer stream failed');
});

it('never starts consuming on a closed backend', function (): void {
    [$backend, , $builder, , $logger] = workerFixture();

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->build();
    $backend->close();

    expect(static function () use ($worker): void {
        $worker->run();
    })->toThrow(ConsumerStopped::class, 'consumer for queue `test.alpha`');

    expect($logger->messages('error'))->toContain('failed to start consumer');
    expect($worker->handle()->processed())->toBe(0);
});

it('logs an error from the consumer stream and propagates it as it is', function (): void {
    [$backend, , , $clock, $logger] = workerFixture();

    $worker = Worker::builder(
        TestQueues::class,
        workerFailingBackend($backend, new ConnectionLost('the socket went away')),
    )
        ->clock($clock)
        ->logger($logger)
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->build();

    try {
        $worker->run();
        Assert::fail('a broken consumer must end the run');
    } catch (ConnectionLost $e) {
        expect($e->getMessage())->toBe('the socket went away', 'a backend error is not rewritten');
    }

    expect($logger->messages('error'))->toContain('consumer stream failed');
    expect($backend->consumerCount())->toBe(0, 'the consumers are cancelled on the way out');
});

it('never lets a failing close mask the failure that ended the run', function (): void {
    [$backend, , , $clock, $logger] = workerFixture();

    $failing = workerFailingBackend(
        $backend,
        new ConnectionLost('the socket went away'),
        new BackendException('close failed too'),
    );
    $worker = Worker::builder(TestQueues::class, $failing)
        ->clock($clock)
        ->logger($logger)
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->closeBackendOnShutdown(true)
        ->build();

    try {
        $worker->run();
        Assert::fail('a broken consumer must end the run');
    } catch (ConnectionLost $e) {
        expect($e->getMessage())->toBe('the socket went away', 'the first failure is the one reported');
    }

    expect($logger->messages('warning'))->toContain('backend close failed');
});

it('ends an otherwise clean run with a failing close', function (): void {
    [$backend, , , $clock, $logger] = workerFixture();

    $failing = workerFailingBackend($backend, null, new BackendException('close failed'));
    $worker = Worker::builder(TestQueues::class, $failing)
        ->clock($clock)
        ->logger($logger)
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->closeBackendOnShutdown(true)
        ->build();

    expect(static function () use ($worker): void {
        workerRunRounds($worker);
    })->toThrow(BackendException::class, 'close failed');
});

it('treats a handler that throws while choosing a policy as retryable, not as a decode error', function (): void {
    [$backend, $producer, $builder, , $logger] = workerFixture();
    $producer->enqueue(new Greet('vip'));

    $handler = new
        /**
         * @implements JobHandler<Greet>
         * @implements ProvidesRetryPolicy<Greet>
         */
        class implements JobHandler, ProvidesRetryPolicy {
            public bool $ran = false;

            public function jobClass(): string
            {
                return Greet::class;
            }

            public function retryPolicyFor(Job $job): ?RetryPolicy
            {
                throw new \RuntimeException('no policy today');
            }

            public function handle(Job $job, JobContext $ctx): void
            {
                $this->ran = true;
            }
        };
    $hook = workerCollectDeadLetters($dead);
    $worker = $builder()
        ->handler($handler)
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);

    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    // `test.alpha` has no policy, so the first failure is also the last — but
    // it is a failure, not a decode error.
    expect($letters[0]['reason'])
        ->toBe('max attempts (1) exhausted after attempt 1: RuntimeException: no policy today');
    expect($dead[0]->cause)->toBe(DeadLetterCause::Exhausted);
    expect($handler->ran)->toBeFalse('the handler never got to run the job');
    expect($logger->messages('warning'))->toContain('handler failed to choose a retry policy');
    expect($worker->handle()->processed())->toBe(1);
});

it('never consults the retry policy for a deferral', function (): void {
    [$backend, $producer, $builder, $clock] = workerFixture();

    // `test.alpha` allows exactly one attempt, and no number of deferrals may
    // ever turn into "attempts exhausted".
    $producer->enqueue(new Greet('patient'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw JobDeferred::for(Duration::seconds(1), 'still waiting');
        }))
        ->build();

    for ($round = 0; $round < 4; ++$round) {
        workerRunRounds($worker);
        $clock->advance(Duration::seconds(1));
    }

    expect($backend->dead(WORKER_ALPHA))->toBe([], 'a deferral is not a failed attempt');
    $held = $backend->held(WORKER_ALPHA);
    expect($held)->toHaveCount(1);
    expect($held[0]->attempt)->toBe(1);
    expect($held[0]->deferrals)->toBe(4);
    expect($worker->handle()->processed())->toBe(4);
});

it('keeps a job in hold across a worker shutdown', function (): void {
    [$backend, $producer, $builder, $clock] = workerFixture();
    $producer->enqueue(new Digest('ops'));

    $attempts = [];
    $handler = FnHandler::for(Digest::class, static function (Digest $job, JobContext $ctx) use (&$attempts): void {
        $attempts[] = $ctx->attempt;
        if ($ctx->attempt === 1) {
            throw RetryableFailure::withMessage('not now');
        }
    });

    $worker = $builder()->handler($handler)->build();
    workerRunRounds($worker);
    $worker->handle()->shutdown();
    $worker->run();

    expect($backend->held(WORKER_MAIL))->toHaveCount(1, 'the successor waits in hold, worker or no worker');
    expect($backend->consumerCount())->toBe(0);

    // A fresh worker, once the hold has elapsed, picks it up where the old
    // one left off.
    $clock->advance(Duration::seconds(2));
    workerRunRounds($builder()->handler($handler)->build());

    expect($attempts)->toBe([1, 2]);
    expect($backend->held(WORKER_MAIL))->toBe([]);
    expect($backend->succeeded())->toBe(1);
});

it('routes handlers of different job types independently', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('ada'));
    $producer->enqueue(new Digest('ops'));
    $producer->enqueue(new Ping(7));

    $seen = [];
    $record = static function (string $what) use (&$seen): void {
        $seen[] = $what;
    };
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx) use ($record): void {
            $record('greet:' . $job->name);
        }))
        ->handler(FnHandler::for(Digest::class, static function (Digest $job, JobContext $ctx) use ($record): void {
            $record('digest:' . $job->to);
        }))
        ->handler(FnHandler::for(Ping::class, static function (Ping $job, JobContext $ctx) use ($record): void {
            $record('ping:' . $job->seq);
        }))
        ->build();

    workerRunRounds($worker);

    sort($seen);
    expect($seen)->toBe(['digest:ops', 'greet:ada', 'ping:7'], 'every job reached its own handler');
    expect($backend->succeeded())->toBe(3);
});

it('counts settle failures for retries, deferrals and dead letters too', function (): void {
    [$backend, $producer, , $clock, $logger] = workerFixture();
    $producer->enqueue(new Greet('retry me'));
    $producer->enqueue(new Digest('defer me'));
    $producer->enqueue(new Ping(1));

    $hook = workerCollectDeadLetters($dead);
    $worker = Worker::builder(TestQueues::class, workerUnsettleableBackend($backend))
        ->clock($clock)
        ->logger($logger)
        ->handler(FnHandler::for(Greet::class, static function (): void {
            throw RetryableFailure::withMessage('flaky');
        }))
        ->handler(FnHandler::for(Digest::class, static function (): void {
            throw JobDeferred::for(Duration::seconds(1), 'later');
        }))
        ->handler(FnHandler::for(Ping::class, static function (): void {
            throw FatalFailure::withMessage('nope');
        }))
        ->jobRetryOverride(Greet::class, RetryPolicy::fixed(3, Duration::seconds(1)))
        ->onDeadLetter($hook)
        ->build();

    workerRunRounds($worker);

    expect($worker->handle()->processed())->toBe(3);
    expect($worker->handle()->settleFailures())
        ->toBe(3, 'every outcome settles, so every outcome can fail to');

    $operations = array_values(array_map(
        static fn(array $record): mixed => $record['context']['operation'] ?? null,
        array_filter(
            $logger->records,
            static fn(array $record): bool => $record['message'] === 'failed to settle delivery',
        ),
    ));
    sort($operations);
    expect($operations)->toBe(['deadLetter', 'defer', 'retry']);

    expect($dead)->toHaveCount(1, 'the hook had already run when the dead-letter settle failed');
    expect($backend->dead('test.beta-queue'))->toBe([], 'and the settle itself got nowhere');
    expect($backend->settled())->toBe(0);
});

it('lets a handler shut the worker down mid-job', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('one'));
    $producer->enqueue(new Greet('two'));

    $seen = [];
    $handle = null;
    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx) use (&$seen, &$handle): void {
            $seen[] = $job->name;
            Assert::assertInstanceOf(WorkerHandle::class, $handle);
            $handle->shutdown();
        }))
        ->build();
    $handle = $worker->handle();

    $worker->run();

    expect($seen)->toBe(['one']);
    expect($worker->handle()->processed())->toBe(1);
    expect($backend->succeeded())->toBe(1, 'the job that asked for the shutdown is still acked');
    expect($backend->pending(WORKER_ALPHA))->toHaveCount(1, 'the rest stays for the next worker');
});

it('still ends retryable when the handler swallows the timeout', function (): void {
    [$backend, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('stubborn'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static function (): void {
            try {
                $until = microtime(true) + 10.0;
                while (microtime(true) < $until) {
                    usleep(10_000);
                }
            } catch (\Throwable) {
                // A handler whose cleanup swallows everything, alarm included.
            }
        }))
        ->jobTimeout(Duration::ms(1500))
        ->build();

    workerRunRounds($worker);

    $letters = $backend->dead(WORKER_ALPHA);
    expect($letters)->toHaveCount(1);
    // The timeout as configured, the way Rust prints it — not the whole
    // second `pcntl_alarm` was armed with.
    expect($letters[0]['reason'])->toBe('max attempts (1) exhausted after attempt 1: job timed out after 1.5s');
    expect($backend->succeeded())->toBe(0, 'a job that ran out of time is never a success');
})->skip(fn(): bool => !\extension_loaded('pcntl'), 'ext-pcntl is required for job timeouts');

it('leaves a pending host alarm and its handler intact through a job timeout', function (): void {
    [, $producer, $builder] = workerFixture();
    $producer->enqueue(new Greet('quick'));

    $worker = $builder()
        ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
        ->jobTimeout(Duration::seconds(1))
        ->build();

    $previous = pcntl_signal_get_handler(\SIGALRM);
    pcntl_signal(\SIGALRM, \SIG_IGN);
    pcntl_alarm(30);

    try {
        workerRunRounds($worker);

        expect(pcntl_alarm(0))->toBeGreaterThan(0, 'the alarm the host had pending is re-armed, not destroyed');
        expect(pcntl_signal_get_handler(\SIGALRM))->toBe(\SIG_IGN, 'and its SIGALRM handler is given back');
    } finally {
        pcntl_alarm(0);
        pcntl_signal(\SIGALRM, $previous);
    }
})->skip(fn(): bool => !\extension_loaded('pcntl'), 'ext-pcntl is required for job timeouts');

it('restores asynchronous signal dispatch', function (): void {
    [, , $builder] = workerFixture();

    $before = pcntl_async_signals();
    pcntl_async_signals(false);

    try {
        $worker = $builder()
            ->handler(FnHandler::for(Greet::class, static fn(Greet $job, JobContext $ctx) => null))
            ->build();
        workerRunRounds($worker);

        expect(pcntl_async_signals())
            ->toBeFalse('a worker inside a larger process leaves signal dispatch as it found it');
    } finally {
        pcntl_async_signals($before);
    }
})->skip(fn(): bool => !\extension_loaded('pcntl'), 'ext-pcntl is required for signal handling');

/**
 * The fixture every test starts from: a frozen clock, a memory backend on that
 * clock, a producer for `TestQueues`, a recording logger, and a factory for a
 * worker builder already wired to all of them.
 *
 * @return array{MemoryBackend, Producer, \Closure(): WorkerBuilder, FrozenClock, RecordingLogger}
 */
function workerFixture(): array
{
    $clock = new FrozenClock(1_700_000_000_000);
    $backend = new MemoryBackend($clock);
    $producer = new Producer(TestQueues::class, $backend, clock: $clock);
    $logger = new RecordingLogger();

    $builder = static fn(): WorkerBuilder => Worker::builder(TestQueues::class, $backend)
        ->clock($clock)
        ->logger($logger);

    return [$backend, $producer, $builder, $clock, $logger];
}

/**
 * Run exactly `$rounds` wait/dispatch rounds, so a frozen clock can never
 * spin the loop forever.
 */
function workerRunRounds(Worker $worker, int $rounds = 1): void
{
    $remaining = $rounds;
    $worker->runUntil(static function () use (&$remaining): bool {
        return $remaining-- <= 0;
    });
}

/**
 * A hook that appends every dead letter it is handed to `$letters`.
 *
 * @param list<DeadLetter>|null $letters
 *
 * @param-out list<DeadLetter> $letters
 */
function workerCollectDeadLetters(?array &$letters): FnDeadLetterHook
{
    $letters = [];

    return FnDeadLetterHook::for(static function (DeadLetter $letter) use (&$letters): void {
        $letters[] = $letter;
    });
}

/**
 * A handler that reads the payload before it settles on a policy — the top
 * of the precedence list in INTEROP-SPEC §5.
 *
 * @param \Closure(string, int): void $record what the handler saw, by job name
 *
 * @return JobHandler<Greet>
 */
function workerPolicyFromPayload(\Closure $record): JobHandler
{
    $handler = new
        /**
         * @implements JobHandler<Greet>
         * @implements ProvidesRetryPolicy<Greet>
         */
        class ($record) implements JobHandler, ProvidesRetryPolicy {
            /**
             * @param \Closure(string, int): void $record
             */
            public function __construct(private readonly \Closure $record) {}

            public function jobClass(): string
            {
                return Greet::class;
            }

            public function retryPolicyFor(Job $job): ?RetryPolicy
            {
                return $job instanceof Greet && $job->name === 'vip'
                    ? RetryPolicy::fixed(3, Duration::seconds(1))
                    : null;
            }

            public function handle(Job $job, JobContext $ctx): void
            {
                Assert::assertInstanceOf(Greet::class, $job);
                ($this->record)($job->name, $ctx->maxAttempts);

                throw RetryableFailure::withMessage('no thanks');
            }
        };

    return $handler;
}

/**
 * A backend that delegates everything to the memory one, but breaks where
 * the test needs it to: on `wait()`, on `close()`, or on both.
 */
function workerFailingBackend(MemoryBackend $inner, ?\Throwable $onWait, ?\Throwable $onClose = null): Backend
{
    return new class ($inner, $onWait, $onClose) implements Backend {
        public function __construct(
            private readonly MemoryBackend $inner,
            private readonly ?\Throwable $onWait,
            private readonly ?\Throwable $onClose,
        ) {}

        public function declare(QueueConfig ...$queues): void
        {
            $this->inner->declare(...$queues);
        }

        public function publish(Envelope $envelope, ?Duration $delay = null): void
        {
            $this->inner->publish($envelope, $delay);
        }

        public function defer(Envelope $envelope, Duration $delay): void
        {
            $this->inner->defer($envelope, $delay);
        }

        public function consume(QueueConfig $queue, callable $onDelivery): void
        {
            $this->inner->consume($queue, $onDelivery);
        }

        public function wait(?float $timeoutSeconds = null): void
        {
            if ($this->onWait !== null) {
                throw $this->onWait;
            }
            $this->inner->wait($timeoutSeconds);
        }

        public function cancelConsumers(): void
        {
            $this->inner->cancelConsumers();
        }

        public function close(): void
        {
            if ($this->onClose !== null) {
                throw $this->onClose;
            }
            $this->inner->close();
        }
    };
}

/** A backend whose deliveries can never be settled. */
function workerUnsettleableBackend(MemoryBackend $inner): Backend
{
    return new class ($inner) implements Backend {
        public function __construct(private readonly MemoryBackend $inner) {}

        public function declare(QueueConfig ...$queues): void
        {
            $this->inner->declare(...$queues);
        }

        public function publish(Envelope $envelope, ?Duration $delay = null): void
        {
            $this->inner->publish($envelope, $delay);
        }

        public function defer(Envelope $envelope, Duration $delay): void
        {
            $this->inner->defer($envelope, $delay);
        }

        public function consume(QueueConfig $queue, callable $onDelivery): void
        {
            $this->inner->consume($queue, static function (Delivery $delivery) use ($onDelivery): void {
                $onDelivery(new class ($delivery) implements Delivery {
                    public function __construct(private readonly Delivery $delivery) {}

                    public function envelope(): Envelope
                    {
                        return $this->delivery->envelope();
                    }

                    public function ack(): void
                    {
                        throw new BackendException('channel is gone');
                    }

                    public function deadLetter(string $reason): void
                    {
                        throw new BackendException('channel is gone');
                    }

                    public function retry(Envelope $next, Duration $delay): void
                    {
                        throw new BackendException('channel is gone');
                    }

                    public function defer(Envelope $next, Duration $delay): void
                    {
                        throw new BackendException('channel is gone');
                    }
                });
            });
        }

        public function wait(?float $timeoutSeconds = null): void
        {
            $this->inner->wait($timeoutSeconds);
        }

        public function cancelConsumers(): void
        {
            $this->inner->cancelConsumers();
        }

        public function close(): void
        {
            $this->inner->close();
        }
    };
}
