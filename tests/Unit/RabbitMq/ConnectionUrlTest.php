<?php

declare(strict_types=1);

use Queuey\Exception\InvalidConfiguration;
use Queuey\RabbitMq\AmqpUrl;
use Queuey\RabbitMq\RabbitMqOptions;

it('parses the canonical local url', function (): void {
    $url = AmqpUrl::parse('amqp://guest:guest@localhost:5672/%2f');

    expect($url->host)->toBe('localhost');
    expect($url->port)->toBe(5672);
    expect($url->user)->toBe('guest');
    expect($url->password)->toBe('guest');
    expect($url->vhost)->toBe('/', '`%2f` is the default vhost, not a path separator');
    expect($url->secure)->toBeFalse();
});

it('reads a missing vhost as the default one', function (): void {
    $url = AmqpUrl::parse('amqp://guest:guest@rabbit:5672');
    expect($url->vhost)->toBe('/');
});

it('reads a trailing slash as the empty named vhost', function (): void {
    // AMQP URI spec: the vhost is whatever follows the first `/`, so this is
    // the vhost called "", which is a different vhost from "/".
    expect(AmqpUrl::parse('amqp://rabbit/')->vhost)->toBe('');
});

it('takes a named vhost verbatim', function (): void {
    expect(AmqpUrl::parse('amqp://rabbit/production')->vhost)->toBe('production');
});

it('decodes a percent-encoded vhost', function (): void {
    expect(AmqpUrl::parse('amqp://rabbit/my%20vhost%2Finner')->vhost)->toBe('my vhost/inner');
});

it('takes the default port from the scheme', function (): void {
    expect(AmqpUrl::parse('amqp://rabbit/%2f')->port)->toBe(5672);
    expect(AmqpUrl::parse('amqps://rabbit/%2f')->port)->toBe(5671);
});

it('lets an explicit port win', function (): void {
    expect(AmqpUrl::parse('amqps://rabbit:15672/%2f')->port)->toBe(15672);
});

it('asks for tls when the scheme is amqps', function (): void {
    $url = AmqpUrl::parse('amqps://user:pw@rabbit/%2f');
    expect($url->secure)->toBeTrue();
    expect($url->user)->toBe('user');
    expect($url->password)->toBe('pw');
});

it('percent-decodes the credentials', function (): void {
    $url = AmqpUrl::parse('amqp://us%40er:p%3Fw@rabbit/%2f');
    expect($url->user)->toBe('us@er');
    expect($url->password)->toBe('p?w');
});

it('defaults the credentials to guest only when the url carries none', function (): void {
    $url = AmqpUrl::parse('amqp://rabbit/%2f');
    expect($url->user)->toBe('guest');
    expect($url->password)->toBe('guest');
});

it('does not let a named user without a password borrow guest', function (): void {
    // `amqp://svc@broker/` says who to be, not what to authenticate with.
    // Falling back to `guest` here would log in as a user the URL never
    // mentioned, and succeed on a default broker while doing it.
    $url = AmqpUrl::parse('amqp://svc@rabbit/%2f');
    expect($url->user)->toBe('svc');
    expect($url->password)->toBe('');
});

it('keeps an empty password', function (): void {
    expect(AmqpUrl::parse('amqp://user:@rabbit/%2f')->password)->toBe('');
});

it('keeps the brackets around an ipv6 host', function (): void {
    // The brackets are part of the host as every socket API wants it back:
    // `[::1]:5672` is unambiguous, `::1:5672` is not.
    $url = AmqpUrl::parse('amqp://[::1]:5672/%2f');
    expect($url->host)->toBe('[::1]');
    expect($url->port)->toBe(5672);
    expect($url->vhost)->toBe('/');
});

it('parses an ipv6 host with credentials and the default port', function (): void {
    $url = AmqpUrl::parse('amqps://user:pw@[2001:db8::1]/production');
    expect($url->host)->toBe('[2001:db8::1]');
    expect($url->port)->toBe(5671);
    expect($url->user)->toBe('user');
    expect($url->password)->toBe('pw');
    expect($url->vhost)->toBe('production');
    expect($url->secure)->toBeTrue();
});

it('leaves the options alone when the url carries no parameters', function (): void {
    $url = AmqpUrl::parse('amqp://rabbit/%2f');
    expect($url->heartbeat)->toBeNull();
    expect($url->connectionTimeout)->toBeNull();
    expect($url->channelMax)->toBeNull();

    $options = RabbitMqOptions::new();
    expect($url->applyTo($options))->toBe($options);
});

it('reads the heartbeat parameter as seconds', function (): void {
    $url = AmqpUrl::parse('amqp://rabbit/%2f?heartbeat=10');
    expect($url->heartbeat)->toBe(10);
    expect($url->applyTo(RabbitMqOptions::new())->heartbeat)->toBe(10);
});

it('reads the connection timeout parameter as milliseconds', function (): void {
    // lapin reads `connection_timeout` in milliseconds, so the same URL must
    // mean the same wait on both sides.
    $url = AmqpUrl::parse('amqp://rabbit/%2f?connection_timeout=2500');
    expect($url->connectionTimeout)->toBe(2.5);
    expect($url->applyTo(RabbitMqOptions::new())->connectionTimeout)->toBe(2.5);
});

it('lets the url win over the options it is applied to', function (): void {
    $url = AmqpUrl::parse('amqp://rabbit/%2f?heartbeat=5&connection_timeout=1000');
    $options = $url->applyTo(RabbitMqOptions::new()->withHeartbeat(30)->withConnectionTimeout(10.0));

    expect($options->heartbeat)->toBe(5);
    expect($options->connectionTimeout)->toBe(1.0);
});

it('parses channel_max and otherwise ignores it', function (): void {
    $url = AmqpUrl::parse('amqp://rabbit/%2f?channel_max=64');
    expect($url->channelMax)->toBe(64);

    // Nothing in the options describes a channel ceiling, and this backend
    // opens a handful of channels either way.
    $options = RabbitMqOptions::new();
    expect($url->applyTo($options))->toBe($options);
});

it('refuses a heartbeat the read timeout cannot cover and names both', function (): void {
    $url = AmqpUrl::parse('amqp://rabbit/%2f?heartbeat=60');

    expect(static fn(): RabbitMqOptions => $url->applyTo(RabbitMqOptions::new()))
        ->toThrow(InvalidConfiguration::class, 'the AMQP URL conflicts with the options');
});

it('refuses an unknown parameter', function (): void {
    expect(static fn(): AmqpUrl => AmqpUrl::parse('amqp://rabbit/%2f?heartbeat_ms=10000'))
        ->toThrow(InvalidConfiguration::class, 'unknown parameter `heartbeat_ms`');
});

it('refuses a non-numeric parameter', function (): void {
    expect(static fn(): AmqpUrl => AmqpUrl::parse('amqp://rabbit/%2f?heartbeat=fast'))
        ->toThrow(InvalidConfiguration::class, 'must be a non-negative integer');
});

it('reads the scheme case-insensitively', function (): void {
    expect(AmqpUrl::parse('AMQPS://rabbit/%2f')->secure)->toBeTrue();
});

it('refuses a foreign scheme', function (): void {
    expect(static fn(): AmqpUrl => AmqpUrl::parse('http://rabbit/%2f'))
        ->toThrow(InvalidConfiguration::class, 'must be `amqp` or `amqps`');
});

it('refuses a url without a host', function (): void {
    expect(static fn(): AmqpUrl => AmqpUrl::parse('amqp://'))->toThrow(InvalidConfiguration::class);
});

it('refuses nonsense', function (): void {
    expect(static fn(): AmqpUrl => AmqpUrl::parse('not a url at all'))->toThrow(InvalidConfiguration::class);
});
