<?php

declare(strict_types=1);

namespace Queuey;

use Psr\Log\LoggerInterface;
use Queuey\Exception\ConsumerStopped;
use Queuey\Exception\FatalFailure;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\JobDeferred;
use Queuey\Exception\JobFailed;
use Queuey\Exception\RetryableFailure;
use Queuey\Exception\ShutDown;

/**
 * Consumes queues of one queue set and dispatches every delivery to a handler.
 *
 * Build one with {@see self::builder()}, run it with {@see self::run()}.
 * Concurrency is one: synchronous PHP runs a single job at a time, and a queue's
 * prefetch only shapes how much the broker buffers for it.
 *
 * The dispatch algorithm, the dead-letter reasons and the order of hook and
 * settle are INTEROP-SPEC §6, which a Rust worker on the same queues implements
 * identically:
 *
 * 1. no handler for `job_type` → dead-letter `` no handler for job type `X` ``;
 * 2. payload does not decode → dead-letter `decode error: …`;
 * 3. otherwise build a {@see JobContext} and call the handler;
 * 4. success → ack; {@see \Queuey\Exception\FatalFailure} → dead-letter
 *    `fatal error: …`; {@see JobDeferred} → hold at the queue's top priority
 *    without touching `attempt`; anything else → the retry policy decides between
 *    a further attempt and `max attempts (N) exhausted after attempt A: …`.
 *
 * A dead-letter hook runs *before* the delivery is settled, for all four causes,
 * and a failure to settle is logged and counted
 * ({@see WorkerHandle::settleFailures()}) but never thrown.
 */
final class Worker
{
    /** How long one `wait()` blocks, so shutdown flags and signals are noticed promptly. */
    private const WAIT_SECONDS = 1.0;

    private readonly WorkerHandle $handle;

    /**
     * @param array<string, array{decode: \Closure(Envelope): array{policyFor: \Closure(): ?RetryPolicy, run: \Closure(JobContext): void}, queue: string, policy: RetryPolicy, deferPriority: int}> $registrations keyed by job type
     * @param list<QueueConfig>                                                                                                                                                                  $queues
     *
     * @internal use {@see self::builder()}
     */
    public function __construct(
        private readonly Backend $backend,
        private readonly array $registrations,
        private readonly array $queues,
        private readonly ?DeadLetterHook $hook,
        private readonly LoggerInterface $logger,
        private readonly Clock $clock,
        private readonly ?Duration $jobTimeout,
        private readonly ?int $maxJobs,
        private readonly bool $closeBackend,
    ) {
        $this->handle = new WorkerHandle();
    }

    /**
     * Start configuring a worker for `$queues` on `$backend`.
     *
     * @param class-string<QueueSet> $queues
     */
    public static function builder(string $queues, Backend $backend): WorkerBuilder
    {
        if (!is_a($queues, QueueSet::class, true)) {
            throw new InvalidConfiguration(\sprintf('%s is not a %s', $queues, QueueSet::class));
        }

        return new WorkerBuilder($queues, $backend);
    }

    /** The handle that shuts this worker down and carries its counters. */
    public function handle(): WorkerHandle
    {
        return $this->handle;
    }

    /**
     * The queues this worker consumes.
     *
     * @return list<QueueConfig>
     */
    public function queues(): array
    {
        return $this->queues;
    }

    /**
     * The policy this worker will actually apply to `$jobType`, after every
     * override was resolved — so a process that takes its numbers from
     * configuration can assert what it ended up with.
     */
    public function retryPolicyFor(string $jobType): ?RetryPolicy
    {
        return $this->registrations[$jobType]['policy'] ?? null;
    }

    /**
     * Consume and dispatch until {@see WorkerHandle::shutdown()} is called (or a
     * `SIGTERM`/`SIGINT` asks for it, with ext-pcntl loaded).
     *
     * The backend is left open unless `closeBackendOnShutdown(true)` was set: it
     * is usually shared with a {@see Producer} that outlives the worker.
     *
     * Note for tests driving a {@see \Queuey\Memory\MemoryBackend} on a
     * {@see FrozenClock}: an idle `wait()` cannot block on a clock that does not
     * move, so the loop spins unless the backend was given a sleeper that yields
     * (its default) or advances the clock.
     *
     * @throws ConsumerStopped when the backend stops delivering — it was closed,
     *                         or its connection is gone for good
     * @throws \Queuey\Exception\BackendException for any other error the backend
     *                                            raises while consuming
     */
    public function run(): void
    {
        $this->loop(null);
    }

    /** Run until `$stop` returns `true`, or shutdown is requested. For supervisors and tests. */
    public function runUntil(callable $stop): void
    {
        $this->loop($stop);
    }

    /** Run for at most `$limit`, measured on the worker's clock. */
    public function runFor(Duration $limit): void
    {
        $deadlineMs = $this->clock->nowMs() + $limit->toMs();

        $this->loop(fn(): bool => $this->clock->nowMs() >= $deadlineMs);
    }

    /**
     * @param (callable(): bool)|null $stop
     */
    private function loop(?callable $stop): void
    {
        $this->logger->info('worker starting', [
            'queues' => \count($this->queues),
            'handlers' => \count($this->registrations),
        ]);

        $signals = $this->installSignalHandlers();
        $failure = null;

        try {
            foreach ($this->queues as $config) {
                try {
                    $this->backend->consume($config, $this->dispatch(...));
                } catch (\Throwable $e) {
                    $this->logger->error('failed to start consumer', [
                        'queue' => $config->name,
                        'error' => JobFailed::describe($e),
                    ]);

                    throw $e instanceof ShutDown ? ConsumerStopped::forQueue($config->name, $e) : $e;
                }
            }

            while (!$this->shouldStop($stop)) {
                $this->backend->wait(self::WAIT_SECONDS);
                if (\function_exists('pcntl_signal_dispatch')) {
                    pcntl_signal_dispatch();
                }
            }
        } catch (\Throwable $e) {
            // A backend that cannot deliver any more ends the run, the way Rust
            // returns `Error::ConsumerStopped`: `wait()` on a closed backend
            // returns at once, so carrying on would spin at full speed forever.
            $failure = $e instanceof ShutDown ? ConsumerStopped::backendGone($e) : $e;
            if (!$e instanceof ConsumerStopped) {
                $this->logger->error('consumer stream failed', [
                    'error' => JobFailed::describe($e),
                ]);
            }
        } finally {
            // Stop receiving first: a delivery already handed over was settled
            // inside `wait()`, so there is nothing left in hand to drain. None of
            // this may mask the failure that got us here, so every step is
            // guarded and reported on its own.
            $failure = $this->stopConsuming($failure);
            $this->restoreSignalHandlers($signals);
            $this->logger->info('worker stopped', [
                'processed' => $this->handle->processed(),
                'settle_failures' => $this->handle->settleFailures(),
            ]);
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * Cancel the consumers and, if asked for, close the backend — without ever
     * losing `$failure`.
     *
     * A close that fails while the run was otherwise fine becomes the failure
     * (as it does in Rust); one that fails on the way out of an earlier failure
     * is logged only, because the first error is the one worth reporting.
     */
    private function stopConsuming(?\Throwable $failure): ?\Throwable
    {
        try {
            $this->backend->cancelConsumers();
        } catch (\Throwable $e) {
            $this->logger->warning('failed to cancel the consumers', ['error' => JobFailed::describe($e)]);
        }

        if (!$this->closeBackend) {
            return $failure;
        }

        try {
            $this->backend->close();
        } catch (\Throwable $e) {
            $this->logger->warning('backend close failed', ['error' => JobFailed::describe($e)]);

            return $failure ?? $e;
        }

        return $failure;
    }

    /**
     * @param (callable(): bool)|null $stop
     */
    private function shouldStop(?callable $stop): bool
    {
        if ($this->handle->isShutdown()) {
            return true;
        }
        if ($this->maxJobs !== null && $this->handle->processed() >= $this->maxJobs) {
            return true;
        }

        return $stop !== null && $stop();
    }

    /** One delivery, start to finish. Never throws: every outcome ends in a settle. */
    private function dispatch(Delivery $delivery): void
    {
        $envelope = $delivery->envelope();
        $context = self::logContext($envelope);
        $registration = $this->registrations[$envelope->jobType] ?? null;

        if ($registration === null) {
            $reason = \sprintf('no handler for job type `%s`', $envelope->jobType);
            $this->logger->warning('no handler registered; dead-lettering', $context);
            // No handler means no policy either, hence `null` for `maxAttempts`.
            $this->deadLetter($delivery, $envelope, DeadLetterCause::NoHandler, $reason, null, $context);
            $this->handle->recordProcessed();

            return;
        }

        $policy = $registration['policy'];

        try {
            // Only the decode is judged as a decode failure. The closure hands
            // back the two later stages bound to the job it decoded, because both
            // need its concrete type and the builder erased that.
            $stages = ($registration['decode'])($envelope);
        } catch (\Throwable $e) {
            $reason = \sprintf('decode error: %s', $e->getMessage());
            $this->logger->error('payload did not match the handler\'s job class', $context + ['error' => $e->getMessage()]);
            $this->deadLetter($delivery, $envelope, DeadLetterCause::Decode, $reason, $policy->maxAttempts, $context);
            $this->handle->recordProcessed();

            return;
        }

        try {
            // A handler that implements `ProvidesRetryPolicy` picks a policy from
            // the payload — the top of the precedence list in INTEROP-SPEC §5,
            // resolved before the context is built so `maxAttempts` already speaks
            // about the policy this attempt is judged by.
            $policy = ($stages['policyFor'])() ?? $policy;
        } catch (\Throwable $e) {
            // The payload decoded, so this is not a decode failure: the handler
            // broke while choosing a policy, which is as retryable as breaking
            // inside `handle()` — and judged by the policy it failed to replace.
            $this->logger->warning('handler failed to choose a retry policy', $context + [
                'error' => JobFailed::describe($e),
            ]);
            $this->handle->recordProcessed();
            $this->onRetryable($delivery, $envelope, $policy, JobFailed::describe($e), $context);

            return;
        }

        $ctx = new JobContext(
            jobId: $envelope->jobId,
            jobType: $envelope->jobType,
            queue: $envelope->queue,
            attempt: $envelope->attempt,
            maxAttempts: $policy->maxAttempts,
            deferrals: $envelope->deferrals,
            priority: $envelope->priority,
            age: $envelope->age($this->clock),
            correlationId: $envelope->correlationId,
        );

        $this->logger->debug('running job', $context);
        $this->handle->recordProcessed();

        try {
            $this->runHandler($stages['run'], $ctx);
        } catch (JobDeferred $deferred) {
            $this->onDeferred($delivery, $envelope, $registration['deferPriority'], $deferred, $context);

            return;
        } catch (FatalFailure $fatal) {
            $reason = \sprintf('fatal error: %s', $fatal->getMessage());
            $this->logger->error('job failed fatally', $context + ['error' => $fatal->getMessage()]);
            $this->deadLetter($delivery, $envelope, DeadLetterCause::Fatal, $reason, $policy->maxAttempts, $context);

            return;
        } catch (\Throwable $e) {
            // A `RetryableFailure` carries its own message; anything else escaping
            // the handler is retryable too, described as `Class: message`.
            $message = $e instanceof JobFailed ? $e->getMessage() : JobFailed::describe($e);
            $this->onRetryable($delivery, $envelope, $policy, $message, $context);

            return;
        }

        $this->logger->info('job succeeded', $context);
        $this->settle(static fn() => $delivery->ack(), 'ack', $context);
    }

    /**
     * Call the handler, optionally under a `pcntl_alarm` that turns an overrun
     * into a {@see RetryableFailure}.
     *
     * The alarm throws into whatever the handler is doing, but a handler is free
     * to catch that — `catch (\Throwable)` around its own cleanup is enough — and
     * a job that swallowed its deadline must not be acked as a success. So the
     * signal handler also raises a flag, and an overrun that ran out of time is
     * reported as retryable whichever way the handler returned.
     *
     * The alarm and the handler for `SIGALRM` are borrowed, not taken: a pending
     * alarm the host process had set is re-armed with what is left of it.
     *
     * @param \Closure(JobContext): void $run
     */
    private function runHandler(\Closure $run, JobContext $ctx): void
    {
        if ($this->jobTimeout === null) {
            $run($ctx);

            return;
        }

        // Whole seconds is all `pcntl_alarm` can express; round up so a job is
        // never cut short of the timeout it was promised. The message quotes the
        // timeout as configured, which is what Rust's `{limit:?}` prints.
        $seconds = max(1, (int) ceil($this->jobTimeout->toMs() / 1000));
        $limit = self::describeDuration($this->jobTimeout);
        $timedOut = false;

        $previous = pcntl_signal_get_handler(\SIGALRM);
        pcntl_signal(\SIGALRM, static function () use ($limit, &$timedOut): void {
            $timedOut = true;

            throw RetryableFailure::withMessage(\sprintf('job timed out after %s', $limit));
        });
        // Arming ours cancels whatever the host had pending, so remember it.
        $pendingAlarm = pcntl_alarm(0);
        $startedAt = microtime(true);

        try {
            pcntl_alarm($seconds);
            $run($ctx);
        } catch (\Throwable $e) {
            if ($timedOut) {
                throw new RetryableFailure(\sprintf('job timed out after %s', $limit), $e);
            }

            throw $e;
        } finally {
            pcntl_alarm(0);
            pcntl_signal(\SIGALRM, $previous);
            if ($pendingAlarm > 0) {
                $elapsed = (int) floor(microtime(true) - $startedAt);
                pcntl_alarm(max(1, $pendingAlarm - $elapsed));
            }
        }

        if ($timedOut) {
            // The handler swallowed the alarm and returned normally. It still ran
            // out of time, so the outcome is the timeout, not a success.
            throw RetryableFailure::withMessage(\sprintf('job timed out after %s', $limit));
        }
    }

    /**
     * A duration the way Rust's `{limit:?}` prints one: `500ms`, `1s`, `1.5s`.
     *
     * The dead-letter reason for a timeout carries it, so both ports spell the
     * same configured timeout the same way.
     */
    private static function describeDuration(Duration $duration): string
    {
        $ms = $duration->toMs();
        if ($ms < 1000) {
            return $ms . 'ms';
        }

        $seconds = rtrim(rtrim(\sprintf('%.3F', $ms / 1000), '0'), '.');

        return $seconds . 's';
    }

    /**
     * A deferral is not a failure: the attempt counter stays put, the retry
     * policy is never consulted, and the envelope comes back at the front of its
     * queue.
     *
     * @param array<string, scalar|null> $context
     */
    private function onDeferred(
        Delivery $delivery,
        Envelope $envelope,
        int $priority,
        JobDeferred $deferred,
        array $context,
    ): void {
        $this->logger->info('job deferred', $context + [
            'delay_ms' => $deferred->delay->toMs(),
            'deferrals' => $envelope->deferrals + 1,
            'reason' => $deferred->reason,
        ]);
        $next = $envelope->deferred($priority);
        $this->settle(static fn() => $delivery->defer($next, $deferred->delay), 'defer', $context);
    }

    /**
     * @param array<string, scalar|null> $context
     */
    private function onRetryable(
        Delivery $delivery,
        Envelope $envelope,
        RetryPolicy $policy,
        string $message,
        array $context,
    ): void {
        $decision = $policy->decide($envelope->attempt);
        $delay = $decision->delay();

        if ($delay === null) {
            $reason = \sprintf(
                'max attempts (%d) exhausted after attempt %d: %s',
                $policy->maxAttempts,
                $envelope->attempt,
                $message,
            );
            $this->logger->error('job failed; giving up', $context + [
                'error' => $message,
                'max_attempts' => $policy->maxAttempts,
            ]);
            $this->deadLetter($delivery, $envelope, DeadLetterCause::Exhausted, $reason, $policy->maxAttempts, $context);

            return;
        }

        $this->logger->warning('job failed; retrying', $context + [
            'error' => $message,
            'delay_ms' => $delay->toMs(),
            'next_attempt' => $envelope->attempt + 1,
        ]);
        $next = $envelope->nextAttempt();
        $this->settle(static fn() => $delivery->retry($next, $delay), 'retry', $context);
    }

    /**
     * Notify the hook, then dead-letter. In that order, always: a hook that ran
     * while the process died is repeated after the redelivery, which is the
     * at-least-once behaviour the rest of the system has, whereas settling first
     * would lose the notification outright.
     *
     * @param array<string, scalar|null> $context
     */
    private function deadLetter(
        Delivery $delivery,
        Envelope $envelope,
        DeadLetterCause $cause,
        string $reason,
        ?int $maxAttempts,
        array $context,
    ): void {
        if ($this->hook !== null) {
            try {
                $this->hook->onDeadLetter(new DeadLetter($envelope, $cause, $reason, $maxAttempts));
            } catch (\Throwable $e) {
                // The hook is a notification, not a veto.
                $this->logger->error('dead-letter hook failed; dead-lettering anyway', $context + [
                    'cause' => $cause->name,
                    'error' => JobFailed::describe($e),
                ]);
            }
        }

        $this->settle(static fn() => $delivery->deadLetter($reason), 'deadLetter', $context);
    }

    /**
     * Log and count, but never propagate, a failure to settle.
     *
     * A settle failure means the broker still owns the message: it will be
     * redelivered once this consumer goes away, so the count is a signal of
     * duplicate work ahead rather than of lost work.
     *
     * @param callable(): void           $settle
     * @param array<string, scalar|null> $context
     */
    private function settle(callable $settle, string $operation, array $context): void
    {
        try {
            $settle();
        } catch (\Throwable $e) {
            $this->handle->recordSettleFailure();
            $this->logger->error('failed to settle delivery', $context + [
                'operation' => $operation,
                'error' => JobFailed::describe($e),
            ]);
        }
    }

    /**
     * Ask for a graceful stop on `SIGTERM` and `SIGINT`, and remember everything
     * that was in force before — the handlers *and* whether signals were
     * dispatched asynchronously, because a worker embedded in a larger process
     * must leave both as it found them.
     *
     * @return array{handlers: array<int, callable|int>, async: bool}|null the previous state, or `null` without ext-pcntl
     */
    private function installSignalHandlers(): ?array
    {
        if (!\extension_loaded('pcntl') || !\function_exists('pcntl_async_signals')) {
            return null;
        }

        $async = pcntl_async_signals();
        pcntl_async_signals(true);
        $handlers = [];
        foreach ([\SIGTERM, \SIGINT] as $signal) {
            $handlers[$signal] = pcntl_signal_get_handler($signal);
            pcntl_signal($signal, function (int $received): void {
                $this->logger->info('shutdown requested by signal', ['signal' => $received]);
                $this->handle->shutdown();
            });
        }

        return ['handlers' => $handlers, 'async' => $async];
    }

    /**
     * @param array{handlers: array<int, callable|int>, async: bool}|null $previous
     */
    private function restoreSignalHandlers(?array $previous): void
    {
        if ($previous === null) {
            return;
        }
        foreach ($previous['handlers'] as $signal => $handler) {
            pcntl_signal($signal, $handler);
        }
        pcntl_async_signals($previous['async']);
    }

    /**
     * @return array<string, scalar|null>
     */
    private static function logContext(Envelope $envelope): array
    {
        return [
            'job_id' => $envelope->jobId,
            'job_type' => $envelope->jobType,
            'queue' => $envelope->queue,
            'attempt' => $envelope->attempt,
        ];
    }
}
