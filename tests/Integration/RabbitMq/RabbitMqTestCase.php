<?php

declare(strict_types=1);

namespace Queuey\Tests\Integration\RabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use Queuey\Delivery;
use Queuey\Envelope;
use Queuey\QueueConfig;
use Queuey\RabbitMq\AmqpUrl;
use Queuey\RabbitMq\Connection;
use Queuey\RabbitMq\RabbitMqBackend;
use Queuey\RabbitMq\RabbitMqOptions;

/**
 * Shared harness for the broker-backed tests.
 *
 * Every test names its queues after a fresh random prefix and deletes them
 * again, so concurrent runs against one broker do not collide. Assertions about
 * the topology go through a second, raw php-amqplib connection, so nothing the
 * backend does to its own connection can hide a failure.
 */
class RabbitMqTestCase extends TestCase
{
    public string $url;

    /**
     * Deliveries handed over by {@see self::consumeInto()}, oldest first.
     *
     * @var list<Delivery>
     */
    public array $received = [];

    private AMQPStreamConnection $control;

    /** @var list<string> queues to delete in tearDown */
    private array $toDelete = [];

    /** @var list<RabbitMqBackend> */
    private array $backends = [];

    protected function setUp(): void
    {
        $url = getenv('AMQP_URL');
        if ($url === false || $url === '') {
            self::markTestSkipped('AMQP_URL is not set; skipping the broker-backed suite.');
        }

        $this->url = $url;
        $this->control = $this->rawConnection();
    }

    protected function tearDown(): void
    {
        $this->received = [];

        foreach ($this->backends as $backend) {
            try {
                $backend->close();
            } catch (\Throwable) {
                // The test may well have closed it already, or killed it.
            }
        }
        $this->backends = [];

        foreach ($this->toDelete as $name) {
            $this->deleteQueue($name);
        }
        $this->toDelete = [];

        try {
            $this->control->close();
        } catch (\Throwable) {
            // Nothing left to clean up.
        }
    }

    /** A backend that is closed for us when the test ends. */
    public function backend(?RabbitMqOptions $options = null): RabbitMqBackend
    {
        $backend = RabbitMqBackend::connect($this->url, $options);
        $this->backends[] = $backend;

        return $backend;
    }

    /**
     * A queue name unique to this test run, with its `q` and `q.dead` already
     * registered for deletion.
     */
    public function uniqueQueue(string $label): string
    {
        $name = \sprintf('qphp-test-%s.%s', bin2hex(random_bytes(6)), $label);
        $this->registerQueue($name);
        $this->registerQueue($name . '.dead');

        return $name;
    }

    /** Also delete `$name` when the test ends. */
    public function registerQueue(string $name): void
    {
        $this->toDelete[] = $name;
    }

    public function envelope(string $queue, int $attempt = 1): Envelope
    {
        return new Envelope(
            jobId: self::uuid(),
            jobType: 'Queuey\\Tests\\Integration\\Probe',
            queue: $queue,
            attempt: $attempt,
            enqueuedAtMs: 1_700_000_000_000,
            payload: (object) ['probe' => true, 'n' => $attempt],
        );
    }

    /** Register a consumer that parks its deliveries in {@see self::$received}. */
    public function consumeInto(RabbitMqBackend $backend, QueueConfig $config): void
    {
        $backend->consume($config, function (Delivery $delivery): void {
            $this->received[] = $delivery;
        });
    }

    /** Drive `$backend` until a delivery shows up, or fail. */
    public function nextDelivery(RabbitMqBackend $backend, float $seconds = 5.0): Delivery
    {
        $deadline = microtime(true) + $seconds;
        while ($this->received === []) {
            $left = $deadline - microtime(true);
            if ($left <= 0) {
                self::fail(\sprintf('no delivery arrived within %s s', (string) $seconds));
            }
            $backend->wait(min($left, 0.25));
        }

        $delivery = array_shift($this->received);
        self::assertInstanceOf(Delivery::class, $delivery);

        return $delivery;
    }

    /** Drive `$backend` for `$seconds` and fail if anything arrives. */
    public function expectIdle(RabbitMqBackend $backend, float $seconds): void
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $backend->wait(min($deadline - microtime(true), 0.1));
            if ($this->received !== []) {
                self::fail('a delivery arrived while the queue was supposed to be idle');
            }
        }
    }

    /**
     * How many channels `$backend` is holding open on its connection.
     *
     * php-amqplib keeps the connection itself in `channels[0]`, so that entry
     * is not one. This is the only way to see a channel the client abandoned
     * without closing: the broker keeps it open and says nothing.
     */
    public function openChannelCount(RabbitMqBackend $backend): int
    {
        $property = new \ReflectionProperty(RabbitMqBackend::class, 'connection');
        $connection = $property->getValue($backend);
        self::assertInstanceOf(Connection::class, $connection);

        $channels = (array) $connection->raw()->channels;
        unset($channels[0]);

        return \count($channels);
    }

    // -- raw broker access -------------------------------------------------

    public function rawConnection(): AMQPStreamConnection
    {
        $url = AmqpUrl::parse($this->url);

        return new AMQPStreamConnection(
            $url->host,
            $url->port,
            $url->user,
            $url->password,
            $url->vhost,
        );
    }

    /** A channel of its own, because a refused method closes the channel it ran on. */
    public function controlChannel(): AMQPChannel
    {
        return $this->control->channel();
    }

    public function queueExists(string $name): bool
    {
        $channel = $this->controlChannel();

        try {
            $channel->queue_declare($name, true);

            return true;
        } catch (\Throwable) {
            return false;
        } finally {
            $this->closeQuietly($channel);
        }
    }

    /** How many messages are ready on `$name`; `-1` when the queue is gone. */
    public function readyCount(string $name): int
    {
        $channel = $this->controlChannel();

        try {
            $result = $channel->queue_declare($name, true);
            if (!\is_array($result) || !\is_int($result[1])) {
                return -1;
            }

            return $result[1];
        } catch (\Throwable) {
            return -1;
        } finally {
            $this->closeQuietly($channel);
        }
    }

    public function awaitCount(string $name, int $expected, float $seconds = 5.0): void
    {
        $deadline = microtime(true) + $seconds;
        $seen = -1;
        while (microtime(true) < $deadline) {
            $seen = $this->readyCount($name);
            if ($seen === $expected) {
                self::assertSame($expected, $seen);

                return;
            }
            usleep(50_000);
        }

        self::fail(\sprintf('`%s` held %d ready messages, expected %d', $name, $seen, $expected));
    }

    /**
     * Declare `$name` outside the backend, to seed a conflicting topology.
     */
    public function declareRaw(string $name, bool $durable, AMQPTable $args): void
    {
        $channel = $this->controlChannel();

        try {
            $channel->queue_declare($name, false, $durable, false, false, false, $args);
        } finally {
            $this->closeQuietly($channel);
        }
    }

    /** Whether `$name` exists with exactly `$args`; a mismatch is a `PRECONDITION_FAILED`. */
    public function queueHasArgs(string $name, bool $durable, AMQPTable $args): bool
    {
        $channel = $this->controlChannel();

        try {
            $channel->queue_declare($name, false, $durable, false, false, false, $args);

            return true;
        } catch (\Throwable) {
            return false;
        } finally {
            $this->closeQuietly($channel);
        }
    }

    /**
     * @param array<string, mixed> $props
     */
    public function publishRaw(string $queue, string $body, array $props = []): void
    {
        $channel = $this->controlChannel();

        try {
            $channel->basic_publish(new AMQPMessage($body, $props), '', $queue);
        } finally {
            $this->closeQuietly($channel);
        }
    }

    /** One message off `$queue`, acked, or `null` when it is empty. */
    public function getRaw(string $queue): ?AMQPMessage
    {
        $channel = $this->controlChannel();

        try {
            $message = $channel->basic_get($queue);
            if ($message instanceof AMQPMessage) {
                $channel->basic_ack($message->getDeliveryTag());

                return $message;
            }

            return null;
        } catch (\Throwable) {
            return null;
        } finally {
            $this->closeQuietly($channel);
        }
    }

    /** One message off `$queue`, waiting for it to show up. */
    public function awaitRaw(string $queue, float $seconds = 5.0): AMQPMessage
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $message = $this->getRaw($queue);
            if ($message !== null) {
                return $message;
            }
            usleep(50_000);
        }

        self::fail(\sprintf('nothing arrived on `%s` within %s s', $queue, (string) $seconds));
    }

    /**
     * The `application_headers` of `$message` as `name => value`, sorted by name.
     *
     * The broker hands a header table back in its own order, so comparing by
     * name is the only stable way to assert on one.
     *
     * @return array<string, mixed>
     */
    public static function headersOf(AMQPMessage $message): array
    {
        if (!$message->has('application_headers')) {
            return [];
        }

        $headers = $message->get('application_headers');
        self::assertInstanceOf(AMQPTable::class, $headers);

        /** @var array<string, mixed> $native */
        $native = $headers->getNativeData();
        ksort($native);

        return $native;
    }

    /**
     * @param array<string, mixed> $expected
     */
    public static function assertHeaders(array $expected, AMQPMessage $message, string $why = ''): void
    {
        ksort($expected);
        self::assertSame($expected, self::headersOf($message), $why);
    }

    public function deleteQueue(string $name): void
    {
        $channel = $this->controlChannel();

        try {
            $channel->queue_delete($name);
        } catch (\Throwable) {
            // Already gone, which is the state we wanted.
        } finally {
            $this->closeQuietly($channel);
        }
    }

    private function closeQuietly(AMQPChannel $channel): void
    {
        try {
            if ($channel->is_open()) {
                $channel->close();
            }
        } catch (\Throwable) {
            // The channel died telling us what was wrong; that was its job.
        }
    }

    private static function uuid(): string
    {
        $bytes = random_bytes(16);
        $bytes[6] = \chr((\ord($bytes[6]) & 0x0F) | 0x40);
        $bytes[8] = \chr((\ord($bytes[8]) & 0x3F) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }
}
