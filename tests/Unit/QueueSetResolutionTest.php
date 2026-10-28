<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Internal\QueueSetRegistry;
use Queuey\QueueConfig;
use Queuey\Tests\Fixtures\BackedQueues;
use Queuey\Tests\Fixtures\BlankPrefixQueues;
use Queuey\Tests\Fixtures\CollidingQueues;
use Queuey\Tests\Fixtures\DelayWithExponentialQueues;
use Queuey\Tests\Fixtures\EmptyQueues;
use Queuey\Tests\Fixtures\TrailingDotPrefixQueues;
use Queuey\Tests\Fixtures\ZeroPrefetchQueues;

/**
 * Every attribute failure has to name the enum and the case it came from: this
 * is the only diagnostic PHP gets where Rust gets a compile error.
 */
it('resolves a backed enum just like a pure one', function (): void {
    expect(BackedQueues::Emails->queueName())->toBe('backed.emails');
    expect(BackedQueues::Images->queueName())->toBe('backed.img');
    expect(BackedQueues::all())->toBe([BackedQueues::Emails, BackedQueues::Images]);
    expect(BackedQueues::fromQueueName('backed.img'))->toBe(BackedQueues::Images);
});

it('rejects a trailing dot in the prefix and names the enum', function (): void {
    try {
        TrailingDotPrefixQueues::Alpha->queueName();
        Assert::fail('expected a rejection');
    } catch (InvalidConfiguration $e) {
        expect($e->getMessage())->toContain(TrailingDotPrefixQueues::class);
        expect($e->getMessage())->toContain('`prefix` must not end with `.`');
    }
});

it('rejects a blank prefix and names the enum', function (): void {
    try {
        BlankPrefixQueues::all()[0]->config();
        Assert::fail('expected a rejection');
    } catch (InvalidConfiguration $e) {
        expect($e->getMessage())->toContain(BlankPrefixQueues::class);
        expect($e->getMessage())->toContain('`prefix` must not be only whitespace');
    }
});

it('rejects a zero prefetch and names the case', function (): void {
    try {
        ZeroPrefetchQueues::Alpha->config();
        Assert::fail('expected a rejection');
    } catch (InvalidConfiguration $e) {
        expect($e->getMessage())->toContain('ZeroPrefetchQueues::Alpha');
        expect($e->getMessage())->toContain('`prefetch` must be between 1 and 65535');
    }
});

it('rejects a delay on an exponential backoff and names the case', function (): void {
    try {
        DelayWithExponentialQueues::Alpha->config();
        Assert::fail('expected a rejection');
    } catch (InvalidConfiguration $e) {
        expect($e->getMessage())->toContain('DelayWithExponentialQueues::Alpha');
        expect($e->getMessage())->toContain("`delay` is only valid with `backoff: 'fixed'`");
    }
});

it('refuses to let two cases resolve to the same queue', function (): void {
    expect(static fn(): QueueConfig => CollidingQueues::Alpha->config())
        ->toThrow(InvalidConfiguration::class, 'maps both Alpha and Beta to the queue `alpha`');
});

it('rejects an enum with no cases', function (): void {
    try {
        QueueSetRegistry::configs(EmptyQueues::class);
        Assert::fail('expected a rejection');
    } catch (InvalidConfiguration $e) {
        expect($e->getMessage())->toContain(EmptyQueues::class);
        expect($e->getMessage())->toContain('requires at least one variant');
    }
});

it('has nothing to offer for an enum with no cases', function (): void {
    // `all()` reads the enum directly, so it is simply empty; every path that
    // resolves configuration is the one that refuses.
    expect(EmptyQueues::all())->toBe([]);
    expect(EmptyQueues::fromQueueName('empty.anything'))->toBeNull();
});

it('caches resolution and forgets it on reset', function (): void {
    $first = QueueSetRegistry::configs(BackedQueues::class);
    expect(QueueSetRegistry::configs(BackedQueues::class))->toBe($first, 'the same array comes back');

    QueueSetRegistry::reset();

    $second = QueueSetRegistry::configs(BackedQueues::class);
    expect($second)->toEqual($first, 're-resolution produces the same configs');
    expect(BackedQueues::Emails->queueName())->toBe('backed.emails', 'and the enum still works after a reset');
});
