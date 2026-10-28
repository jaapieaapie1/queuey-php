<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\JobContext;

it('knows when it is on the last attempt', function (): void {
    expect(jobContextWith(attempt: 1, maxAttempts: 3)->isLastAttempt())->toBeFalse();
    expect(jobContextWith(attempt: 2, maxAttempts: 3)->isLastAttempt())->toBeFalse();
    expect(jobContextWith(attempt: 3, maxAttempts: 3)->isLastAttempt())->toBeTrue();
    expect(jobContextWith(attempt: 4, maxAttempts: 3)->isLastAttempt())
        ->toBeTrue('a stale envelope is still the last chance');
    expect(jobContextWith(attempt: 1, maxAttempts: 1)->isLastAttempt())
        ->toBeTrue('no retries means the first attempt is the last');
});

it('starts from documented defaults', function (): void {
    $ctx = jobContextWith(attempt: 1, maxAttempts: 1);

    expect($ctx->deferrals)->toBe(0);
    expect($ctx->priority)->toBe(0);
    expect($ctx->age->isZero())->toBeTrue();
    expect($ctx->correlationId)->toBeNull();
});

it('exposes every field', function (): void {
    $ctx = new JobContext(
        jobId: '67e55044-10b1-426f-9247-bb680e5fe0c8',
        jobType: 'test::Greet',
        queue: 'test.alpha',
        attempt: 2,
        maxAttempts: 5,
        deferrals: 3,
        priority: 10,
        age: Duration::seconds(7),
        correlationId: 'trace-abc',
    );

    expect($ctx->jobId)->toBe('67e55044-10b1-426f-9247-bb680e5fe0c8');
    expect($ctx->jobType)->toBe('test::Greet');
    expect($ctx->queue)->toBe('test.alpha');
    expect($ctx->attempt)->toBe(2);
    expect($ctx->maxAttempts)->toBe(5);
    expect($ctx->deferrals)->toBe(3);
    expect($ctx->priority)->toBe(10);
    expect($ctx->age->toMs())->toBe(7_000);
    expect($ctx->correlationId)->toBe('trace-abc');
    expect($ctx->isLastAttempt())->toBeFalse();
});

function jobContextWith(int $attempt, int $maxAttempts): JobContext
{
    return new JobContext(
        jobId: '67e55044-10b1-426f-9247-bb680e5fe0c8',
        jobType: 'test::Greet',
        queue: 'test.alpha',
        attempt: $attempt,
        maxAttempts: $maxAttempts,
    );
}
