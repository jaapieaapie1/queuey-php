<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Exception\ConnectionLost;
use Queuey\Exception\ConsumerStopped;
use Queuey\QueueConfig;
use Queuey\RabbitMq\RabbitMqOptions;
use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

/*
 * INTEROP-SPEC §8: a dropped connection is a pause, not a failure.
 *
 * The drop is staged by asking RabbitMQ itself to close the connection, which
 * needs the management plugin; the tests skip when it is not reachable.
 */

it('reconnects a publish after the connection drops', function (): void {
    /** @var RabbitMqTestCase $this */
    $name = reconnectRequireManagement($this->url, 'publish');
    $queue = $this->uniqueQueue('reconnect-publish');
    $backend = $this->backend((new RabbitMqOptions())->withConnectionName($name));
    $backend->declare(QueueConfig::new($queue));

    reconnectKillBrokerConnection($this->url, $name);

    // The publish notices the dead socket, dials again and goes through, so
    // job code never sees the outage.
    $backend->publish($this->envelope($queue));
    $this->awaitCount($queue, 1);
    expect($backend->isConnected())->toBeTrue();
});

it('resubscribes a consumer after the connection drops', function (): void {
    /** @var RabbitMqTestCase $this */
    $name = reconnectRequireManagement($this->url, 'consume');
    $queue = $this->uniqueQueue('reconnect-consume');
    $backend = $this->backend((new RabbitMqOptions())->withConnectionName($name));
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $this->consumeInto($backend, $config);

    reconnectKillBrokerConnection($this->url, $name);

    // Published from the outside, so the delivery can only come from a
    // consumer that put itself back on a new connection.
    $this->publishRaw($queue, $this->envelope($queue)->toJson());

    $delivery = $this->nextDelivery($backend, 15.0);
    expect($delivery->envelope()->queue)->toBe($queue);
    $delivery->ack();
});

it('re-declares the queues it had declared on a reconnect', function (): void {
    /** @var RabbitMqTestCase $this */
    $name = reconnectRequireManagement($this->url, 'redeclare');
    $queue = $this->uniqueQueue('reconnect-declare');
    $backend = $this->backend((new RabbitMqOptions())->withConnectionName($name));
    $backend->declare(QueueConfig::new($queue));

    // Stand in for a broker that came back without its queues.
    $this->deleteQueue($queue);
    $this->deleteQueue($queue . '.dead');
    expect($this->queueExists($queue))->toBeFalse();

    reconnectKillBrokerConnection($this->url, $name);
    $backend->publish($this->envelope($queue));

    expect($this->queueExists($queue))->toBeTrue('the replay must put the queue back');
    expect($this->queueExists($queue . '.dead'))->toBeTrue();
    $this->awaitCount($queue, 1);
});

it('can have its reconnection turned off', function (): void {
    /** @var RabbitMqTestCase $this */
    $name = reconnectRequireManagement($this->url, 'no-reconnect');
    $queue = $this->uniqueQueue('reconnect-off');
    $backend = $this->backend(
        (new RabbitMqOptions())->withConnectionName($name)->withReconnect(false),
    );
    $backend->declare(QueueConfig::new($queue));

    reconnectKillBrokerConnection($this->url, $name);

    $envelope = $this->envelope($queue);
    expect(static function () use ($backend, $envelope): void {
        $backend->publish($envelope);
    })->toThrow(ConnectionLost::class);
});

it('stops a consumer when reconnection is turned off', function (): void {
    /** @var RabbitMqTestCase $this */
    $name = reconnectRequireManagement($this->url, 'no-reconnect-consume');
    $queue = $this->uniqueQueue('reconnect-off-consume');
    $backend = $this->backend(
        (new RabbitMqOptions())->withConnectionName($name)->withReconnect(false),
    );
    $config = QueueConfig::new($queue);
    $backend->declare($config);
    $this->consumeInto($backend, $config);

    reconnectKillBrokerConnection($this->url, $name);

    // The drop is noticed on the next read, so it surfaces out of `wait()`
    // rather than out of the kill. With reconnection off, that is the end of
    // the consumer and the worker loop is meant to stop rather than spin.
    expect(static function () use ($backend): void {
        $deadline = microtime(true) + 10.0;
        while (microtime(true) < $deadline) {
            $backend->wait(0.25);
        }
    })->toThrow(ConsumerStopped::class);
});

/** A connection name for this test, or a skip when the management API is not there. */
function reconnectRequireManagement(string $url, string $label): string
{
    if (reconnectManagementRequest($url, 'GET', '/api/overview') === null) {
        Assert::markTestSkipped('the RabbitMQ management API is not reachable; skipping the reconnect tests.');
    }

    return \sprintf('qphp-test-%s-%s', bin2hex(random_bytes(4)), $label);
}

/**
 * Ask the broker to close the connection named `$connectionName`.
 *
 * The management API's connection list is fed by the stats collector and
 * lags by a few seconds, so the connection is waited for generously and the
 * close is confirmed by its HTTP status rather than by the list emptying.
 */
function reconnectKillBrokerConnection(string $url, string $connectionName): void
{
    $deadline = microtime(true) + 20.0;
    $target = null;
    while (microtime(true) < $deadline) {
        $target = reconnectFindConnection($url, $connectionName);
        if ($target !== null) {
            break;
        }
        usleep(200_000);
    }
    if ($target === null) {
        Assert::fail(\sprintf('the broker never listed a connection named `%s`', $connectionName));
    }

    [$status] = reconnectManagementCall($url, 'DELETE', '/api/connections/' . rawurlencode($target));
    expect($status)->toBeGreaterThanOrEqual(200, 'closing the connection through the management API failed');
    expect($status)->toBeLessThan(300, 'closing the connection through the management API failed');

    // The client only finds out on its next read, which is exactly what the
    // test is about; give the broker a moment to actually hang up first.
    usleep(500_000);
}

function reconnectFindConnection(string $url, string $connectionName): ?string
{
    $body = reconnectManagementRequest($url, 'GET', '/api/connections');
    if ($body === null) {
        return null;
    }

    $rows = json_decode($body, true);
    if (!\is_array($rows)) {
        return null;
    }

    foreach ($rows as $row) {
        if (!\is_array($row) || !\is_string($row['name'] ?? null)) {
            continue;
        }
        $properties = $row['client_properties'] ?? null;
        if (\is_array($properties) && ($properties['connection_name'] ?? null) === $connectionName) {
            return $row['name'];
        }
    }

    return null;
}

/** The response body, or `null` when the request failed. */
function reconnectManagementRequest(string $url, string $method, string $path): ?string
{
    [$status, $body] = reconnectManagementCall($url, $method, $path);

    return $status >= 200 && $status < 300 ? $body : null;
}

/**
 * @return array{0: int, 1: string} the HTTP status and the body
 */
function reconnectManagementCall(string $url, string $method, string $path): array
{
    $host = parse_url($url, \PHP_URL_HOST);
    $context = stream_context_create(['http' => [
        'method' => $method,
        'header' => 'Authorization: Basic ' . base64_encode('guest:guest') . "\r\n",
        'timeout' => 5,
        'ignore_errors' => true,
    ]]);

    $body = @file_get_contents(
        \sprintf('http://%s:15672%s', \is_string($host) ? $host : 'localhost', $path),
        false,
        $context,
    );
    if ($body === false) {
        return [0, ''];
    }

    // Set by the stream wrapper for the request just made; it exists
    // because the fetch above did not fail.
    $status = 0;
    foreach ($http_response_header as $header) {
        if (preg_match('#^HTTP/\S+\s+(\d{3})#', $header, $matches) === 1) {
            $status = (int) $matches[1];
        }
    }

    return [$status, $body];
}
