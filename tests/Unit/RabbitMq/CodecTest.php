<?php

declare(strict_types=1);

use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\Assert;
use Queuey\Envelope;
use Queuey\RabbitMq\Codec;
use Queuey\RabbitMq\Topology;

it('carries the content type and persistence in the props', function (): void {
    $props = Codec::propsFor(codecEnvelope());
    expect($props['content_type'])->toBe('application/json');
    expect($props['delivery_mode'])->toBe(2);
});

it('carries the message id and type in the props', function (): void {
    $props = Codec::propsFor(codecEnvelope());
    expect($props['message_id'])->toBe('67e55044-10b1-426f-9247-bb680e5fe0c8');
    expect($props['type'])->toBe('myapp::jobs::SendEmail');
});

it('carries the attempt and deferrals headers as unsigned longs', function (): void {
    $headers = codecTypedHeaders(Codec::propsFor(codecEnvelope()));
    expect($headers)->toBe([
        Topology::HEADER_ATTEMPT => [AMQPTable::T_INT_LONG_U, 3],
        Topology::HEADER_DEFERRALS => [AMQPTable::T_INT_LONG_U, 0],
    ]);
});

it('tracks the envelope in the deferrals header', function (): void {
    $headers = codecTypedHeaders(Codec::propsFor(codecDeferredEnvelope()));
    expect($headers[Topology::HEADER_DEFERRALS][1])->toBe(2);
    // A deferral is not an attempt.
    expect($headers[Topology::HEADER_ATTEMPT][1])->toBe(3);
});

it('carries the envelope priority in the props', function (): void {
    // Normal work is priority 0, and the property is always set so a queue
    // with `x-max-priority` orders every message the same way.
    expect(Codec::propsFor(codecEnvelope())['priority'])->toBe(0);
    expect(Codec::propsFor(codecDeferredEnvelope())['priority'])->toBe(10);
    expect(Codec::deadLetterProps(codecDeferredEnvelope(), 'boom')['priority'])->toBe(10);
});

it('never carries an expiration, because the hold queue times the wait', function (): void {
    // A per-message expiration would fight the hold queue's x-message-ttl
    // and bring back head-of-line blocking between different delays.
    expect(Codec::propsFor(codecEnvelope()))->not->toHaveKey('expiration');
    expect(Codec::propsFor(codecDeferredEnvelope()))->not->toHaveKey('expiration');
    expect(Codec::deadLetterProps(codecEnvelope(), 'boom'))->not->toHaveKey('expiration');
    expect(Codec::malformedProps('myapp.emails'))->not->toHaveKey('expiration');
});

it('records the reason, the queue and the attempts in the dead-letter headers', function (): void {
    $headers = codecTypedHeaders(Codec::deadLetterProps(codecEnvelope(), 'handler returned Fatal'));
    expect($headers)->toBe([
        Topology::HEADER_ATTEMPT => [AMQPTable::T_INT_LONG_U, 3],
        Topology::HEADER_DEFERRALS => [AMQPTable::T_INT_LONG_U, 0],
        Topology::HEADER_DEATH_REASON => [AMQPTable::T_STRING_LONG, 'handler returned Fatal'],
        Topology::HEADER_ORIGINAL_QUEUE => [AMQPTable::T_STRING_LONG, 'myapp.emails'],
        Topology::HEADER_ATTEMPTS => [AMQPTable::T_INT_LONG_U, 3],
    ]);
});

it('keeps the identity in the dead-letter props', function (): void {
    $props = Codec::deadLetterProps(codecEnvelope(), 'boom');
    expect($props['message_id'])->toBe('67e55044-10b1-426f-9247-bb680e5fe0c8');
    expect($props['type'])->toBe('myapp::jobs::SendEmail');
    expect($props['content_type'])->toBe('application/json');
});

it('records the origin and the reason only in the malformed props', function (): void {
    $props = Codec::malformedProps('myapp.emails');
    expect([
        $props['delivery_mode'],
        array_keys($props),
    ])->toBe([2, ['delivery_mode', 'application_headers']]);
    expect(codecTypedHeaders($props))->toBe([
        Topology::HEADER_DEATH_REASON => [AMQPTable::T_STRING_LONG, 'malformed envelope'],
        Topology::HEADER_ORIGINAL_QUEUE => [AMQPTable::T_STRING_LONG, 'myapp.emails'],
    ]);
});

it('carries no job metadata in the malformed props', function (): void {
    $props = Codec::malformedProps('myapp.emails');
    expect($props)->not->toHaveKey('message_id');
    expect($props)->not->toHaveKey('type');
    expect($props)->not->toHaveKey('priority');
    expect(codecTypedHeaders($props))->not->toHaveKey(Topology::HEADER_ATTEMPT);
});

it('truncates an over-long job type rather than rejecting it', function (): void {
    $envelope = codecEnvelope('é');
    $type = Codec::propsFor($envelope)['type'];
    expect($type)->toBeString();

    /** @var string $type */
    expect(\strlen($type))->toBeLessThanOrEqual(Topology::MAX_SHORT_STRING);
    // 'é' is two bytes, so truncation must land on an even byte offset.
    expect(\strlen($type))->toBe(254);
    expect(mb_strlen($type, 'UTF-8'))->toBe(127);
});

it('never splits a character when truncating', function (): void {
    expect(Codec::truncateAtBoundary('héllo', 2))->toBe('h');
    expect(Codec::truncateAtBoundary('héllo', 3))->toBe('hé');
    expect(Codec::truncateAtBoundary('héllo', 99))->toBe('héllo');
    expect(Codec::truncateAtBoundary('é', 1))->toBe('');
});

it('keeps a job type of exactly the maximum length verbatim', function (): void {
    $jobType = str_repeat('a', Topology::MAX_SHORT_STRING);
    $envelope = new Envelope(
        jobId: '67e55044-10b1-426f-9247-bb680e5fe0c8',
        jobType: $jobType,
        queue: 'myapp.emails',
        attempt: 1,
        enqueuedAtMs: 1_700_000_000_000,
    );
    expect(Codec::propsFor($envelope)['type'])->toBe($jobType);
    expect(\strlen(Codec::truncateAtBoundary($jobType, 255)))->toBe(255);
});

function codecEnvelope(string $repeatedJobTypeChar = ''): Envelope
{
    $jobType = $repeatedJobTypeChar === ''
        ? 'myapp::jobs::SendEmail'
        : str_repeat($repeatedJobTypeChar, 400);

    return new Envelope(
        jobId: '67e55044-10b1-426f-9247-bb680e5fe0c8',
        jobType: $jobType,
        queue: 'myapp.emails',
        attempt: 3,
        enqueuedAtMs: 1_700_000_000_000,
        payload: (object) ['to' => 'a@b.c'],
    );
}

/** The same envelope after two deferrals onto a 10-level priority queue. */
function codecDeferredEnvelope(): Envelope
{
    return codecEnvelope()->deferred(10)->deferred(10);
}

/**
 * The `application_headers` table as `key => [amqpType, value]`.
 *
 * @param array<string, mixed> $props
 *
 * @return array<string, array{0: int, 1: mixed}>
 */
function codecTypedHeaders(array $props): array
{
    $headers = $props['application_headers'] ?? null;
    Assert::assertInstanceOf(AMQPTable::class, $headers);

    $out = [];
    foreach ($headers as $key => $entry) {
        Assert::assertIsArray($entry);
        Assert::assertIsString($key);
        Assert::assertIsInt($entry[0]);
        $out[$key] = [$entry[0], $entry[1]];
    }

    return $out;
}
