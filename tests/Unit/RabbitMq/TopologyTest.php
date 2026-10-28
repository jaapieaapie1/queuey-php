<?php

declare(strict_types=1);

use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\Assert;
use Queuey\Duration;
use Queuey\Exception\BackendException;
use Queuey\Exception\DelayTooLong;
use Queuey\Exception\InvalidConfiguration;
use Queuey\QueueConfig;
use Queuey\RabbitMq\Topology;

// -- names ---------------------------------------------------------------

it('names the dead-letter queue after the queue itself', function (): void {
    expect(Topology::deadQueueName('emails', Topology::DEFAULT_DEAD_SUFFIX))->toBe('emails.dead');
});

it('keeps a prefixed queue name in the dead-letter name', function (): void {
    expect(Topology::deadQueueName('myapp.emails', Topology::DEFAULT_DEAD_SUFFIX))->toBe('myapp.emails.dead');
});

it('honours a custom dead-letter suffix', function (): void {
    expect(Topology::deadQueueName('q', '-dlq'))->toBe('q-dlq');
});

it('puts the ttl in the hold queue name', function (): void {
    expect(Topology::deferredQueueName('emails', Topology::DEFAULT_DEFERRED_SUFFIX, 30_000))
        ->toBe('emails.deferred.30000');
    expect(Topology::deferredQueueName('myapp.emails', Topology::DEFAULT_DEFERRED_SUFFIX, 1_000))
        ->toBe('myapp.emails.deferred.1000');
});

it('honours a custom hold queue suffix', function (): void {
    expect(Topology::deferredQueueName('q', '-hold', 250))->toBe('q-hold.250');
});

it('gives every ttl its own hold queue, which is the whole point', function (): void {
    $short = Topology::deferredQueueName('q', Topology::DEFAULT_DEFERRED_SUFFIX, 1_000);
    $long = Topology::deferredQueueName('q', Topology::DEFAULT_DEFERRED_SUFFIX, 60_000);
    expect($long)->not->toBe($short);
});

// -- ttl rounding --------------------------------------------------------

it('keeps an exact multiple of the granularity verbatim', function (): void {
    expect(Topology::deferredTtlMs(topologySecond(), topologySecond()))->toBe(1_000);
    expect(Topology::deferredTtlMs(Duration::seconds(30), topologySecond()))->toBe(30_000);
    expect(Topology::deferredTtlMs(Duration::ms(750), Duration::ms(250)))->toBe(750);
});

it('rounds a partial step up so a job never returns early', function (): void {
    expect(Topology::deferredTtlMs(Duration::ms(29_200), topologySecond()))->toBe(30_000);
    expect(Topology::deferredTtlMs(Duration::ms(1_001), topologySecond()))->toBe(2_000);
    expect(Topology::deferredTtlMs(Duration::ms(1_999), topologySecond()))->toBe(2_000);
    // Sub-millisecond remainders count as a partial millisecond, then a partial step.
    expect(Topology::deferredTtlMs(Duration::seconds(1.0000005), topologySecond()))->toBe(2_000);
});

it('turns a delay below the granularity into one step', function (): void {
    expect(Topology::deferredTtlMs(Duration::ms(5), topologySecond()))->toBe(1_000);
    expect(Topology::deferredTtlMs(Duration::seconds(0.0000001), topologySecond()))->toBe(1_000);
    // Zero is not "publish it straight back": a hold always waits.
    expect(Topology::deferredTtlMs(Duration::zero(), topologySecond()))->toBe(1_000);
});

it('refuses a delay past the cap rather than releasing it early', function (): void {
    expect(static fn(): int => Topology::deferredTtlMs(Duration::days(30), topologySecond()))
        ->toThrow(DelayTooLong::class);
});

it('refuses one millisecond past the cap', function (): void {
    expect(static fn(): int => Topology::deferredTtlMs(Duration::ms(Topology::MAX_DEFERRAL_MS + 1), topologySecond()))
        ->toThrow(DelayTooLong::class);
});

it('accepts the cap itself at a matching granularity', function (): void {
    expect(Topology::deferredTtlMs(Duration::ms(Topology::MAX_DEFERRAL_MS), Duration::ms(1)))
        ->toBe(Topology::MAX_DEFERRAL_MS);
});

it('refuses a delay whose rounding up crosses the cap', function (): void {
    // Just under the cap, but the next whole second is past it.
    expect(static fn(): int => Topology::deferredTtlMs(Duration::ms(Topology::MAX_DEFERRAL_MS - 1), topologySecond()))
        ->toThrow(DelayTooLong::class);
});

it('clamps a zero granularity to one millisecond rather than dividing by zero', function (): void {
    expect(Topology::deferredTtlMs(Duration::ms(7), Duration::zero()))->toBe(7);
    expect(Topology::deferredTtlMs(Duration::zero(), Duration::zero()))->toBe(1);
    // Sub-millisecond granularities land in the same clamp.
    expect(Topology::deferredTtlMs(Duration::ms(7), Duration::seconds(0.0000001)))->toBe(7);
});

it('clamps an absurd granularity to the cap, not past it', function (): void {
    expect(Topology::deferredTtlMs(topologySecond(), Duration::days(1_000)))->toBe(Topology::MAX_DEFERRAL_MS);
});

it('names both numbers in the delay-too-long message', function (): void {
    try {
        Topology::deferredTtlMs(Duration::days(30), topologySecond());
        Assert::fail('a delay past the cap must be refused');
    } catch (DelayTooLong $error) {
        expect($error->getMessage())->toContain((string) Duration::days(30)->toMs());
        expect($error->getMessage())->toContain((string) Topology::MAX_DEFERRAL_MS);
    }
});

// -- main queue arguments ------------------------------------------------

it('leaves the queue arguments empty without a ttl or priorities', function (): void {
    expect(topologyTyped(Topology::queueArgs(topologyPlain('emails'))))->toBe([]);
});

it('carries the message ttl as a signed long long', function (): void {
    $args = topologyTyped(Topology::queueArgs(topologyPlain('emails')->withMessageTtl(Duration::seconds(30))));
    expect($args)->toBe([Topology::ARG_MESSAGE_TTL => [AMQPTable::T_INT_LONGLONG, 30_000]]);
});

it('carries the max priority as an unsigned byte', function (): void {
    $args = topologyTyped(Topology::queueArgs(QueueConfig::new('emails')));
    expect($args)->toBe([Topology::ARG_MAX_PRIORITY => [AMQPTable::T_INT_SHORTSHORT_U, 10]]);

    $args = topologyTyped(Topology::queueArgs(QueueConfig::new('emails')->withMaxPriority(255)));
    expect($args)->toBe([Topology::ARG_MAX_PRIORITY => [AMQPTable::T_INT_SHORTSHORT_U, 255]]);
});

it('omits the max priority when priorities are off', function (): void {
    $config = QueueConfig::new('emails')->withMaxPriority(0);
    expect($config->maxPriority)->toBeNull();
    expect(topologyTyped(Topology::queueArgs($config)))->not->toHaveKey(Topology::ARG_MAX_PRIORITY);
});

it('carries the ttl and the priority together', function (): void {
    $config = QueueConfig::new('emails')->withMessageTtl(Duration::seconds(5))->withMaxPriority(3);
    expect(topologyTyped(Topology::queueArgs($config)))->toBe([
        Topology::ARG_MESSAGE_TTL => [AMQPTable::T_INT_LONGLONG, 5_000],
        Topology::ARG_MAX_PRIORITY => [AMQPTable::T_INT_SHORTSHORT_U, 3],
    ]);
});

it('rounds a sub-millisecond ttl up to one', function (): void {
    $config = topologyPlain('emails')->withMessageTtl(Duration::seconds(0.0000001));
    expect(topologyTyped(Topology::queueArgs($config))[Topology::ARG_MESSAGE_TTL][1])->toBe(1);
});

it('keeps a ttl exactly at the limit verbatim', function (): void {
    $config = topologyPlain('emails')->withMessageTtl(Duration::ms(Topology::MAX_TTL_MS));
    expect(topologyTyped(Topology::queueArgs($config))[Topology::ARG_MESSAGE_TTL][1])->toBe(Topology::MAX_TTL_MS);
});

it('cannot even be configured with a ttl past what RabbitMQ accepts', function (): void {
    // RabbitMQ answers anything past MAX_TTL_MS with PRECONDITION_FAILED and
    // closes the declaring channel. `Topology::queueArgs` clamps defensively,
    // but `QueueConfig` refuses the value first, so the clamp is unreachable
    // through the public API — which is the better of the two answers.
    expect(static fn(): QueueConfig => topologyPlain('emails')->withMessageTtl(Duration::ms(Topology::MAX_TTL_MS + 1)))
        ->toThrow(InvalidConfiguration::class);
});

// -- dead-letter and hold queue arguments --------------------------------

it('leaves the dead-letter queue arguments empty', function (): void {
    expect(topologyTyped(Topology::deadQueueArgs()))->toBe([]);
});

it('gives only the main queue priorities', function (): void {
    $config = QueueConfig::new('emails');
    expect(topologyTyped(Topology::deadQueueArgs()))->not->toHaveKey(Topology::ARG_MAX_PRIORITY);
    expect(topologyTyped(Topology::deferredQueueArgs($config, 1_000)))->not->toHaveKey(Topology::ARG_MAX_PRIORITY);
});

it('holds for the ttl then dead-letters onto the main queue', function (): void {
    expect(topologyTyped(Topology::deferredQueueArgs(QueueConfig::new('myapp.emails'), 30_000)))->toBe([
        Topology::ARG_MESSAGE_TTL => [AMQPTable::T_INT_LONGLONG, 30_000],
        Topology::ARG_DEAD_LETTER_EXCHANGE => [AMQPTable::T_STRING_LONG, ''],
        Topology::ARG_DEAD_LETTER_ROUTING_KEY => [AMQPTable::T_STRING_LONG, 'myapp.emails'],
        Topology::ARG_EXPIRES => [AMQPTable::T_INT_LONGLONG, 60_000],
    ]);
});

it('depends only on the hold queue name for its arguments', function (): void {
    // Two callers that disagree about *everything* except the main queue's
    // name and the TTL must still compute identical arguments, or the broker
    // locks one of them out of `q.deferred.1000` for ever.
    $one = QueueConfig::new('q')->withMessageTtl(Duration::seconds(99))->withPrefetch(1);
    $two = QueueConfig::new('q')->withMaxPriority(0)->withPrefetch(200);
    expect(topologyTyped(Topology::deferredQueueArgs($two, 1_000)))
        ->toBe(topologyTyped(Topology::deferredQueueArgs($one, 1_000)));
});

it('always makes x-expires strictly greater than the ttl', function (): void {
    foreach ([1, 1_000, 30_000, Topology::MAX_DEFERRAL_MS] as $ttl) {
        $expires = topologyTyped(Topology::deferredQueueArgs(QueueConfig::new('q'), $ttl))[Topology::ARG_EXPIRES][1];
        expect($expires)->toBeGreaterThan($ttl, \sprintf('x-expires must outlive the %d ms TTL', $ttl));
        expect($expires)->toBeLessThanOrEqual(Topology::MAX_TTL_MS);
    }
});

it('clamps x-expires when doubling the ttl would overflow', function (): void {
    // Unreachable through `deferredTtlMs`, which caps at MAX_DEFERRAL_MS;
    // guarded anyway so a hand-rolled call cannot produce an argument
    // RabbitMQ answers with PRECONDITION_FAILED.
    $args = topologyTyped(Topology::deferredQueueArgs(QueueConfig::new('q'), Topology::MAX_TTL_MS));
    expect($args[Topology::ARG_EXPIRES][1])->toBe(Topology::MAX_TTL_MS);
});

it('never carries a queue ttl of its own from the config', function (): void {
    // The hold queue's TTL is the delay, not the main queue's message TTL.
    $config = QueueConfig::new('q')->withMessageTtl(Duration::seconds(99));
    $args = topologyTyped(Topology::deferredQueueArgs($config, 2_000));
    expect($args[Topology::ARG_MESSAGE_TTL][1])->toBe(2_000);
});

// -- name length ---------------------------------------------------------

it('accepts a queue name with room for its hold queues', function (): void {
    Topology::checkDeferrableName('myapp.emails', Topology::DEFAULT_DEFERRED_SUFFIX);
    // The longest name that still fits: 255 - len(".deferred.") - 10 digits.
    $longest = str_repeat('q', Topology::MAX_SHORT_STRING - \strlen('.deferred.') - 10);
    expect(\strlen($longest))->toBe(235);
    Topology::checkDeferrableName($longest, Topology::DEFAULT_DEFERRED_SUFFIX);
    expect(\strlen(Topology::deferredQueueName($longest, Topology::DEFAULT_DEFERRED_SUFFIX, Topology::MAX_DEFERRAL_MS)))
        ->toBe(255);
});

it('refuses a queue name that leaves no room for hold queues at declare time', function (): void {
    // 250 bytes is a perfectly legal queue name, but
    // `{q}.deferred.2147483647` is 270 bytes, so every deferral on it would
    // fail. Better to say so once, at declare time.
    try {
        Topology::checkDeferrableName(str_repeat('q', 250), Topology::DEFAULT_DEFERRED_SUFFIX);
        Assert::fail('a name with no room for hold queues must be refused');
    } catch (BackendException $error) {
        expect($error->getMessage())->toContain('leaves no room for its hold queues');
        expect($error->getMessage())->toContain('270 bytes');
        expect($error->getMessage())->toContain('255-byte');
    }
});

it('uses the configured suffix when checking the hold queue name', function (): void {
    $name = str_repeat('q', 240);
    // 240 + 2 ("-h") + 1 (".") + 10 digits = 253: accepted.
    Topology::checkDeferrableName($name, '-h');

    // 240 + 10 (".deferred.") + 10 digits = 260: refused.
    expect(static function () use ($name): void {
        Topology::checkDeferrableName($name, Topology::DEFAULT_DEFERRED_SUFFIX);
    })->toThrow(BackendException::class);
});

// -- constants -----------------------------------------------------------

it('pins the constants the spec pins', function (): void {
    expect(Topology::MAX_TTL_MS)->toBe(4294967295);
    expect(Topology::MAX_DEFERRAL_MS)->toBe(2147483647);
    expect(Topology::MAX_DEFERRAL_MS)->toBe(intdiv(Topology::MAX_TTL_MS, 2));
    expect(Topology::DEFAULT_DEAD_SUFFIX)->toBe('.dead');
    expect(Topology::DEFAULT_DEFERRED_SUFFIX)->toBe('.deferred');
    expect(Topology::MAX_SHORT_STRING)->toBe(255);
    expect(Topology::CONSUMER_TAG_QUEUE_LIMIT)->toBe(160);
});

function topologySecond(): Duration
{
    return Duration::seconds(1);
}

/** `QueueConfig::new` is a priority queue by default; these assertions only care about the TTL. */
function topologyPlain(string $name): QueueConfig
{
    return QueueConfig::new($name)->withMaxPriority(0);
}

/**
 * The table as `key => [amqpType, value]`, which is what the wire cares about.
 *
 * @return array<string, array{0: int, 1: mixed}>
 */
function topologyTyped(AMQPTable $table): array
{
    $out = [];
    foreach ($table as $key => $entry) {
        Assert::assertIsArray($entry);
        Assert::assertIsString($key);
        Assert::assertIsInt($entry[0]);
        $out[$key] = [$entry[0], $entry[1]];
    }

    return $out;
}
