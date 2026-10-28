<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;
use Queuey\QueueConfig;
use Queuey\RetryPolicy;

it('starts from documented defaults', function (): void {
    $config = QueueConfig::new('q');

    expect($config->name)->toBe('q');
    expect($config->prefetch)->toBe(16);
    expect($config->durable)->toBeTrue();
    expect($config->messageTtl)->toBeNull();
    expect($config->maxPriority)->toBe(10);
    expect($config->retry->equals(RetryPolicy::none()))->toBeTrue();
    expect(QueueConfig::DEFAULT_PREFETCH)->toBe(16);
    expect(QueueConfig::DEFAULT_MAX_PRIORITY)->toBe(10);
});

it('returns new instances from its withers', function (): void {
    $base = QueueConfig::new('q');
    $tuned = $base
        ->withPrefetch(4)
        ->withDurable(false)
        ->withMessageTtl(Duration::seconds(30))
        ->withRetry(RetryPolicy::exponential(3));

    expect($tuned)->not->toBe($base);
    expect($base->prefetch)->toBe(16, 'the original is untouched');
    expect($tuned->prefetch)->toBe(4);
    expect($tuned->durable)->toBeFalse();
    expect($tuned->messageTtl?->toMs())->toBe(30_000);
    expect($tuned->retry->maxAttempts)->toBe(3);
    expect($tuned->maxPriority)->toBe(10, 'the other withers leave maxPriority alone');
    expect($tuned->name)->toBe('q');
});

it('turns priorities off when maxPriority is zero', function (): void {
    expect(QueueConfig::new('q')->withMaxPriority(0)->maxPriority)->toBeNull();
    expect((new QueueConfig('q', maxPriority: 0))->maxPriority)->toBeNull();
    expect((new QueueConfig('q', maxPriority: null))->maxPriority)->toBeNull();
});

it('stores the number of priority levels', function (): void {
    expect(QueueConfig::new('q')->withMaxPriority(5)->maxPriority)->toBe(5);
    expect(QueueConfig::new('q')->withMaxPriority(255)->maxPriority)->toBe(255);
    expect(QueueConfig::new('q')->withMaxPriority(5)->withMaxPriority(0)->maxPriority)
        ->toBeNull('last call wins');
});

it('bounds the prefetch', function (int $prefetch): void {
    expect(static fn(): QueueConfig => QueueConfig::new('q')->withPrefetch($prefetch))
        ->toThrow(InvalidConfiguration::class, '`prefetch` must be between 1 and 65535');
})->with([
    'zero' => [0],
    'negative' => [-1],
    'one past the maximum' => [65536],
]);

it('treats the prefetch bounds as inclusive', function (): void {
    expect(QueueConfig::new('q')->withPrefetch(1)->prefetch)->toBe(1);
    expect(QueueConfig::new('q')->withPrefetch(65535)->prefetch)->toBe(65535);
});

it('bounds the max priority', function (int $levels): void {
    expect(static fn(): QueueConfig => QueueConfig::new('q')->withMaxPriority($levels))
        ->toThrow(InvalidConfiguration::class, '`maxPriority` must be in 0..=255');
})->with([
    'negative' => [-1],
    'one past the maximum' => [256],
]);

it('rejects a blank name', function (): void {
    expect(static fn(): QueueConfig => QueueConfig::new('   '))->toThrow(InvalidConfiguration::class);
});

it('rejects an empty name', function (): void {
    expect(static fn(): QueueConfig => QueueConfig::new(''))->toThrow(InvalidConfiguration::class);
});

it('rejects a zero message ttl', function (): void {
    expect(static fn(): QueueConfig => QueueConfig::new('q')->withMessageTtl(Duration::zero()))
        ->toThrow(InvalidConfiguration::class, '`messageTtl` must be greater than zero');
});

it('rejects a message ttl a broker cannot express', function (): void {
    expect(static fn(): QueueConfig => QueueConfig::new('q')->withMessageTtl(Duration::ms(QueueConfig::MAX_MESSAGE_TTL_MS + 1)))
        ->toThrow(InvalidConfiguration::class, '`messageTtl` must be between 1ms and 4294967295ms (~49.7 days)');
});

it('says why the message ttl ceiling exists', function (): void {
    expect(static fn(): QueueConfig => QueueConfig::new('q')->withMessageTtl(Duration::days(60)))
        ->toThrow(
            InvalidConfiguration::class,
            'RabbitMQ stores x-message-ttl as an unsigned 32-bit millisecond count',
        );
});

it('accepts the message ttl ceiling itself', function (): void {
    expect(QueueConfig::MAX_MESSAGE_TTL_MS)->toBe(4294967295);
    expect(QueueConfig::new('q')->withMessageTtl(Duration::ms(4294967295))->messageTtl?->toMs())
        ->toBe(4294967295);
    expect((new QueueConfig('q', messageTtl: Duration::ms(4294967295)))->messageTtl?->toMs())
        ->toBe(4294967295);
});

it('enforces the message ttl ceiling in the constructor too', function (): void {
    expect(static fn(): QueueConfig => new QueueConfig('q', messageTtl: Duration::ms(4294967296)))
        ->toThrow(InvalidConfiguration::class, '`messageTtl` must be between 1ms and 4294967295ms');
});

it('leaves a realistic ttl nowhere near the ceiling', function (): void {
    expect(QueueConfig::new('q')->withMessageTtl(Duration::days(7))->messageTtl?->toMs())->toBe(604_800_000);
});
