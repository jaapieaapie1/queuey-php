<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;
use Queuey\RabbitMq\BackoffPolicy;
use Queuey\RabbitMq\Hold;
use Queuey\RabbitMq\RabbitMqOptions;
use Queuey\RabbitMq\Topology;

it('pins the defaults the spec pins', function (): void {
    $options = RabbitMqOptions::new();

    expect($options->connectionName)->toBeNull();
    expect($options->deadSuffix)->toBe(Topology::DEFAULT_DEAD_SUFFIX);
    expect($options->deadSuffix)->toBe('.dead');
    expect($options->declareDeadLetterQueues)->toBeTrue();
    expect($options->deferredSuffix)->toBe('.deferred');
    expect($options->retryGranularity->toMs())->toBe(1000);
    expect($options->deferredGranularity->toMs())->toBe(1000);
    expect($options->reconnect)->toBeInstanceOf(BackoffPolicy::class);
    expect($options->heartbeat)->toBe(30);
    expect($options->connectionTimeout)->toBe(10.0);
    expect($options->readWriteTimeout)->toBe(60.0);
    expect($options->publishConfirmTimeout)->toBe(30.0);
});

it('does not mutate the original in its withers', function (): void {
    $options = RabbitMqOptions::new();
    $changed = $options
        ->withConnectionName('orders-worker')
        ->withDeadSuffix('-dlq')
        ->withDeclareDeadLetterQueues(false)
        ->withDeferredSuffix('-hold')
        ->withRetryGranularity(Duration::seconds(10))
        ->withDeferredGranularity(Duration::ms(250))
        ->withPublishConfirmTimeout(5.0);

    expect($options->connectionName)->toBeNull();
    expect($options->deadSuffix)->toBe('.dead');
    expect($options->declareDeadLetterQueues)->toBeTrue();
    expect($options->retryGranularity->toMs())->toBe(1000);

    expect($changed->connectionName)->toBe('orders-worker');
    expect($changed->deadSuffix)->toBe('-dlq');
    expect($changed->declareDeadLetterQueues)->toBeFalse();
    expect($changed->deferredSuffix)->toBe('-hold');
    expect($changed->retryGranularity->toMs())->toBe(10_000);
    expect($changed->deferredGranularity->toMs())->toBe(250);
    expect($changed->publishConfirmTimeout)->toBe(5.0);
});

it('reads its own granularity for each hold kind', function (): void {
    $options = RabbitMqOptions::new()
        ->withRetryGranularity(Duration::seconds(10))
        ->withDeferredGranularity(Duration::ms(250));

    expect(Hold::Retry->granularity($options)->toMs())->toBe(10_000);
    expect(Hold::Deferral->granularity($options)->toMs())->toBe(250);
});

it('can turn reconnection off', function (): void {
    $options = RabbitMqOptions::new()->withReconnect(false);
    expect($options->reconnect)->toBeFalse();
});

it('can clear the connection name again', function (): void {
    $options = RabbitMqOptions::new()->withConnectionName('worker')->withConnectionName(null);
    expect($options->connectionName)->toBeNull();
});

it('refuses a read timeout below twice the heartbeat', function (): void {
    // php-amqplib refuses the socket outright, and an unexplained
    // InvalidArgumentException from a vendor constructor is no way to learn
    // about a configuration mistake.
    expect(static fn(): RabbitMqOptions => new RabbitMqOptions(heartbeat: 30, readWriteTimeout: 30.0))
        ->toThrow(InvalidConfiguration::class, 'at least twice the heartbeat');
});

it('can turn heartbeats off entirely', function (): void {
    $options = RabbitMqOptions::new()->withHeartbeat(0)->withReadWriteTimeout(3.0);
    expect($options->heartbeat)->toBe(0);
    expect($options->readWriteTimeout)->toBe(3.0);
});

it('refuses a zero read-write timeout even without heartbeats', function (): void {
    // With heartbeats off, the "twice the heartbeat" rule has nothing to
    // say, and php-amqplib reads a non-positive timeout as "wait forever":
    // a peer that stopped answering would hang the worker rather than fail.
    expect(static fn(): RabbitMqOptions => new RabbitMqOptions(heartbeat: 0, readWriteTimeout: 0.0))
        ->toThrow(InvalidConfiguration::class, '`readWriteTimeout` must be greater than zero');
});

it('refuses a negative read-write timeout', function (): void {
    expect(static fn(): RabbitMqOptions => new RabbitMqOptions(heartbeat: 0, readWriteTimeout: -1.0))
        ->toThrow(InvalidConfiguration::class);
});

it('refuses a negative heartbeat', function (): void {
    expect(static fn(): RabbitMqOptions => new RabbitMqOptions(heartbeat: -1))
        ->toThrow(InvalidConfiguration::class);
});

it('refuses a zero confirm timeout', function (): void {
    expect(static fn(): RabbitMqOptions => new RabbitMqOptions(publishConfirmTimeout: 0.0))
        ->toThrow(InvalidConfiguration::class);
});
