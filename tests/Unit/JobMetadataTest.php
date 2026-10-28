<?php

declare(strict_types=1);

use Queuey\BackoffKind;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Internal\JobMetadata;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Nothing;
use Queuey\Tests\Fixtures\Ping;
use Queuey\Tests\Fixtures\TestQueues;
use Queuey\Tests\Fixtures\Unattributed;

it('takes an explicit name as the job type', function (): void {
    $meta = JobMetadata::for(Greet::class);

    expect($meta->queue)->toBe(TestQueues::Alpha);
    expect($meta->name)->toBe('test::Greet');
    expect($meta->retry)->toBeNull();
});

it('defaults the name to the fully qualified class name', function (): void {
    expect(JobMetadata::for(Nothing::class)->name)->toBe(Nothing::class);
    expect(JobMetadata::for(Nothing::class)->name)->toBe('Queuey\Tests\Fixtures\Nothing');
});

it('turns a job retry into a policy', function (): void {
    $meta = JobMetadata::for(Ping::class);
    $retry = $meta->retry;

    expect($meta->queue)->toBe(TestQueues::Beta);
    expect($retry)->not->toBeNull();
    expect($retry?->maxAttempts)->toBe(5);
    expect($retry?->backoff->kind())->toBe(BackoffKind::Exponential);
    expect($retry?->backoff->base()->toMs())->toBe(250);
});

it('caches resolution', function (): void {
    expect(JobMetadata::for(Greet::class))->toBe(JobMetadata::for(Greet::class));
});

it('rejects a job class with no #[OnQueue]', function (): void {
    expect(static fn(): JobMetadata => JobMetadata::for(Unattributed::class))
        ->toThrow(InvalidConfiguration::class, 'has no #[OnQueue(...)]');
});

it('rejects a class that is not a job', function (): void {
    expect(static fn(): JobMetadata => JobMetadata::for(\stdClass::class))
        ->toThrow(InvalidConfiguration::class, 'does not implement');
});

it('forgets the cache on reset and resolves again', function (): void {
    $first = JobMetadata::for(Greet::class);

    JobMetadata::reset();

    $second = JobMetadata::for(Greet::class);
    expect($second)->not->toBe($first, 'the cache was really dropped');
    expect($second->queue)->toBe($first->queue);
    expect($second->name)->toBe($first->name);
    expect(JobMetadata::for(Greet::class))->toBe($second, 'and it caches again');
});
