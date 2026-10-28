<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\Exception\BackendException;
use Queuey\Exception\ConnectionLost;
use Queuey\Exception\FatalFailure;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\JobBelongsToOtherQueueSet;
use Queuey\Exception\JobDeferred;
use Queuey\Exception\JobFailed;
use Queuey\Exception\PublishNacked;
use Queuey\Exception\PublishReturned;
use Queuey\Exception\QueueyException;
use Queuey\Exception\RetryableFailure;

it('makes every library exception a QueueyException', function (): void {
    expect(RetryableFailure::withMessage('x'))->toBeInstanceOf(QueueyException::class);
    expect(FatalFailure::withMessage('x'))->toBeInstanceOf(QueueyException::class);
    expect(JobDeferred::for(Duration::seconds(1)))->toBeInstanceOf(QueueyException::class);
    expect(new InvalidConfiguration('x'))->toBeInstanceOf(QueueyException::class);
    expect(new PublishReturned('x'))->toBeInstanceOf(QueueyException::class);
});

it('groups the backend failures under one hierarchy', function (): void {
    expect(new PublishReturned('x'))->toBeInstanceOf(BackendException::class);
    expect(new PublishNacked('x'))->toBeInstanceOf(BackendException::class);
    expect(new ConnectionLost('x'))->toBeInstanceOf(BackendException::class);

    $wrapped = ConnectionLost::because(new \RuntimeException('socket closed'));
    expect($wrapped)->toBeInstanceOf(ConnectionLost::class);
    expect($wrapped->getMessage())->toBe('backend error: socket closed');
});

it('treats a foreign queue set as a configuration problem', function (): void {
    expect(new JobBelongsToOtherQueueSet('x'))->toBeInstanceOf(InvalidConfiguration::class);
});

it('keeps the concrete type when built with a message', function (): void {
    $retryable = RetryableFailure::withMessage('smtp timeout');
    $fatal = FatalFailure::withMessage('bad address');

    expect($retryable)->toBeInstanceOf(RetryableFailure::class);
    expect($fatal)->toBeInstanceOf(FatalFailure::class);
    expect($retryable)->toBeInstanceOf(JobFailed::class);
    expect($retryable->getMessage())->toBe('smtp timeout');
    expect($fatal->getMessage())->toBe('bad address');
});

it('renders the cause the way the worker does', function (): void {
    $cause = new \RuntimeException('connection refused');
    $failure = RetryableFailure::because($cause);

    expect($failure)->toBeInstanceOf(RetryableFailure::class);
    expect($failure->getMessage())->toBe('RuntimeException: connection refused');
    expect($failure->getPrevious())->toBe($cause);
    expect(JobFailed::describe($cause))->toBe('RuntimeException: connection refused');
});

it('carries its delay and reason on a deferred job', function (): void {
    $generic = JobDeferred::for(Duration::seconds(30));
    expect($generic->delay->toMs())->toBe(30_000);
    expect($generic->reason)->toBe('deferred');
    expect($generic->getMessage())->toBe('job deferred for 30000ms: deferred');

    $specific = JobDeferred::for(Duration::ms(1_500), 'rate limited');
    expect($specific->delay->toMs())->toBe(1_500);
    expect($specific->reason)->toBe('rate limited');
});
