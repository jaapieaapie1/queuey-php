<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\Envelope;
use Queuey\Exception\DecodeError;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Exception\MalformedEnvelope;
use Queuey\FrozenClock;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Nothing;
use Queuey\Tests\Fixtures\Ping;

/** INTEROP-SPEC §1, first pinned fixture. */
const ENVELOPE_FIXTURE_FULL = '{"job_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","job_type":"myapp::jobs::SendEmail","queue":"myapp.emails","attempt":2,"enqueued_at_ms":1700000000000,"deferrals":1,"priority":10,"payload":{"to":"a@b.c"}}';

/** A payload a Rust `u64` fits in and a PHP `int` does not, plus every byte a scanner can trip over. */
const ENVELOPE_FIXTURE_BIG_INT = '{"job_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1700000000000,"deferrals":0,"priority":0,"payload":{"n":12345678901234567890,"s":"x\\"}{","a":[{}],"e":{}}}';

/** INTEROP-SPEC §1, second pinned fixture: written before deferrals existed. */
const ENVELOPE_FIXTURE_OLD = '{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"test::Greet","queue":"test.alpha","attempt":2,"enqueued_at_ms":1700000000000,"payload":{"name":"ada"}}';

it('fills in the job metadata', function (): void {
    $clock = new FrozenClock(1_700_000_000_000);
    $envelope = Envelope::new(new Greet('ada'), $clock);

    expect($envelope->jobType)->toBe('test::Greet');
    expect($envelope->queue)->toBe('test.alpha');
    expect($envelope->attempt)->toBe(1);
    expect($envelope->enqueuedAtMs)->toBe(1_700_000_000_000);
    expect($envelope->deferrals)->toBe(0);
    expect($envelope->priority)->toBe(0);
    expect($envelope->correlationId)->toBeNull();
    expect($envelope->payload)->toEqual((object) ['name' => 'ada']);
});

it('gives distinct envelopes distinct uuids', function (): void {
    $a = Envelope::new(new Greet('a'));
    $b = Envelope::new(new Greet('a'));

    expect($a->jobId)->not->toBe($b->jobId);
    expect($a->jobId)->toMatch(
        '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
        'job ids are canonical hyphenated lowercase UUID v4',
    );
});

it('builds a raw envelope without a job class', function (): void {
    $envelope = Envelope::raw('other::Job', 'other.queue', ['n' => 1]);

    expect($envelope->jobType)->toBe('other::Job');
    expect($envelope->queue)->toBe('other.queue');
    expect($envelope->attempt)->toBe(1);
    expect($envelope->correlationId)->toBeNull();
});

it('decodes both pinned fixtures', function (): void {
    $full = Envelope::fromJson(ENVELOPE_FIXTURE_FULL);
    expect($full->jobId)->toBe('67e55044-10b1-426f-9247-bb680e5fe0c8');
    expect($full->jobType)->toBe('myapp::jobs::SendEmail');
    expect($full->queue)->toBe('myapp.emails');
    expect($full->attempt)->toBe(2);
    expect($full->enqueuedAtMs)->toBe(1_700_000_000_000);
    expect($full->deferrals)->toBe(1);
    expect($full->priority)->toBe(10);
    expect($full->correlationId)->toBeNull();
    expect($full->payload)->toEqual((object) ['to' => 'a@b.c']);

    $old = Envelope::fromJson(ENVELOPE_FIXTURE_OLD);
    expect($old->attempt)->toBe(2);
    expect($old->deferrals)->toBe(0, 'a missing `deferrals` defaults to 0');
    expect($old->priority)->toBe(0, 'a missing `priority` defaults to 0');
    expect($old->correlationId)->toBeNull();
});

it('re-encodes the old fixture with the defaulted fields', function (): void {
    $json = Envelope::fromJson(ENVELOPE_FIXTURE_OLD)->toJson();

    expect($json)->toContain('"deferrals":0');
    expect($json)->toContain('"priority":0');
    expect($json)->not->toContain('correlation_id');
    expect($json)->toBe(
        '{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"test::Greet","queue":"test.alpha","attempt":2,"enqueued_at_ms":1700000000000,"deferrals":0,"priority":0,"payload":{"name":"ada"}}',
    );
});

it('pins the key order', function (): void {
    $json = Envelope::fromJson(ENVELOPE_FIXTURE_FULL)
        ->withCorrelationId('trace-abc')
        ->toJson();

    $decoded = json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
    expect($decoded)->toBeArray();

    /** @var array<string, mixed> $decoded */
    expect(array_keys($decoded))->toBe(
        ['job_id', 'job_type', 'queue', 'attempt', 'enqueued_at_ms', 'deferrals', 'priority', 'correlation_id', 'payload'],
    );
});

it('keeps an unset correlation id out of the json', function (): void {
    expect(Envelope::new(new Greet('ada'))->toJson())->not->toContain('correlation_id');
});

it('carries a correlation id through retries and deferrals', function (): void {
    $envelope = Envelope::new(new Greet('ada'))->withCorrelationId('trace-abc');

    expect($envelope->correlationId)->toBe('trace-abc');
    expect($envelope->nextAttempt()->correlationId)->toBe('trace-abc');
    expect($envelope->deferred(10)->correlationId)->toBe('trace-abc');
});

it('round trips through json', function (): void {
    $envelope = Envelope::new(new Greet('grace'))->withCorrelationId('trace-1');
    $back = Envelope::fromJson($envelope->toJson());

    expect($back)->toEqual($envelope);
    expect($back->toJson())->toBe($envelope->toJson());
});

it('increments the attempt and preserves the identity', function (): void {
    $envelope = Envelope::new(new Greet('ada'));
    $second = $envelope->nextAttempt();

    expect($second->attempt)->toBe(2);
    expect($second->jobId)->toBe($envelope->jobId);
    expect($second->jobType)->toBe($envelope->jobType);
    expect($second->queue)->toBe($envelope->queue);
    expect($second->enqueuedAtMs)->toBe($envelope->enqueuedAtMs);
    expect($second->payload)->toEqual($envelope->payload);
    expect($second->priority)->toBe(0);
    expect($envelope->attempt)->toBe(1, 'the original is untouched');
    expect($second->nextAttempt()->attempt)->toBe(3);
});

it('keeps the deferral count but drops the priority on a retry', function (): void {
    $deferred = Envelope::new(new Greet('ada'))->deferred(10);
    $retried = $deferred->nextAttempt();

    expect($retried->attempt)->toBe(2);
    expect($retried->deferrals)->toBe(1, 'a retry is not a second deferral');
    expect($retried->priority)->toBe(0, 'a retry goes behind the backlog');
    expect($deferred->priority)->toBe(10, 'the deferred envelope is untouched');
    expect($deferred->attempt)->toBe(1);
    expect($retried->nextAttempt()->priority)->toBe(0);
});

it('increments the deferrals and sets the priority on a deferral', function (): void {
    $envelope = Envelope::new(new Greet('ada'));
    $held = $envelope->deferred(10);

    expect($held->deferrals)->toBe(1);
    expect($held->priority)->toBe(10);
    expect($held->attempt)->toBe($envelope->attempt, 'a deferral is not an attempt');
    expect($held->jobId)->toBe($envelope->jobId);
    expect($held->enqueuedAtMs)->toBe($envelope->enqueuedAtMs);
    expect($envelope->deferrals)->toBe(0, 'the original is untouched');
    expect($envelope->priority)->toBe(0);
});

it('accumulates deferrals and stays at zero on a zero-priority queue', function (): void {
    $twice = Envelope::new(new Greet('ada'))->deferred(10)->deferred(0);

    expect($twice->deferrals)->toBe(2);
    expect($twice->priority)->toBe(0);
    expect($twice->attempt)->toBe(1);
});

it('round trips an empty object payload as an object', function (): void {
    $envelope = Envelope::new(new Nothing());

    expect($envelope->toJson())->toContain('"payload":{}');
    expect(Envelope::fromJson($envelope->toJson())->toJson())->toContain('"payload":{}');
});

it('keeps an empty object an object across a retry', function (): void {
    $body = Envelope::fromJson(
        '{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"payload":{}}',
    );

    expect($body->nextAttempt()->toJson())->toContain('"payload":{}');
});

it('keeps a list payload a list', function (): void {
    $envelope = Envelope::raw('t', 'q', [1, 2, 3]);

    expect($envelope->toJson())->toContain('"payload":[1,2,3]');
});

it('decodes back to the original job', function (): void {
    $envelope = Envelope::new(new Greet('linus'));

    expect($envelope->decode(Greet::class))->toEqual(new Greet('linus'));
});

it('still decodes after a round trip', function (): void {
    $envelope = Envelope::fromJson(Envelope::new(new Ping(7))->toJson());

    expect($envelope->decode(Ping::class))->toEqual(new Ping(7));
});

it('throws when decoding into the wrong shape', function (): void {
    expect(static fn(): Ping => Envelope::new(new Greet('linus'))->decode(Ping::class))
        ->toThrow(DecodeError::class);
});

it('rejects a garbage message body', function (string $body): void {
    expect(static fn(): Envelope => Envelope::fromJson($body))->toThrow(MalformedEnvelope::class);
})->with([
    'not json' => ['not json'],
    'empty' => [''],
    'a json array' => ['[1,2,3]'],
    'a json scalar' => ['42'],
    'only a job id' => ['{"job_id":"nope"}'],
    'a non-uuid job id' => ['{"job_id":"nope","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"payload":{}}'],
    'a missing payload' => ['{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1}'],
    'a missing attempt' => ['{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","enqueued_at_ms":1,"payload":{}}'],
    'attempt zero' => ['{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":0,"enqueued_at_ms":1,"payload":{}}'],
    'a string attempt' => ['{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":"2","enqueued_at_ms":1,"payload":{}}'],
    'a priority above 255' => ['{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"priority":256,"payload":{}}'],
    'a numeric job type' => ['{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":7,"queue":"q","attempt":1,"enqueued_at_ms":1,"payload":{}}'],
]);

it('ignores unknown fields', function (): void {
    $envelope = Envelope::fromJson(
        '{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"payload":{},"from_the_future":true}',
    );

    expect($envelope->attempt)->toBe(1);
    expect($envelope->toJson())->not->toContain('from_the_future');
});

it('treats a null payload as a payload', function (): void {
    $envelope = Envelope::fromJson(
        '{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"payload":null}',
    );

    expect($envelope->payload)->toBeNull();
    expect($envelope->toJson())->toContain('"payload":null');
});

it('normalises an uppercase job id to lowercase', function (): void {
    $envelope = Envelope::fromJson(
        '{"job_id":"8B1A9953-4C2F-4A5B-9C2E-0D1F2A3B4C5D","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"payload":{}}',
    );

    expect($envelope->jobId)->toBe('8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d');
});

it('measures the age from the first enqueue and never goes negative', function (): void {
    $clock = new FrozenClock(1_000);
    $envelope = Envelope::new(new Greet('ada'), $clock);

    expect($envelope->age($clock)->toMs())->toBe(0);

    $clock->advance(Duration::seconds(5));
    expect($envelope->age($clock)->toMs())->toBe(5_000);
    expect($envelope->nextAttempt()->age($clock)->toMs())
        ->toBe(5_000, 'a retry keeps the original enqueue time');

    $clock->setMs(0);
    expect($envelope->age($clock)->toMs())->toBe(0, 'a clock that went backwards saturates at 0');
});

// --- verbatim payload bytes ----------------------------------------------

it('round trips a payload too big for php ints byte for byte', function (): void {
    expect(Envelope::fromJson(ENVELOPE_FIXTURE_BIG_INT)->toJson())->toBe(
        ENVELOPE_FIXTURE_BIG_INT,
        'a u64 payload field must not be re-encoded through a float',
    );
});

it('keeps the verbatim payload across a retry', function (): void {
    $expected = str_replace('"attempt":1', '"attempt":2', ENVELOPE_FIXTURE_BIG_INT);

    expect(Envelope::fromJson(ENVELOPE_FIXTURE_BIG_INT)->nextAttempt()->toJson())->toBe($expected);
});

it('keeps the verbatim payload across a deferral', function (): void {
    $expected = str_replace('"deferrals":0,"priority":0', '"deferrals":1,"priority":10', ENVELOPE_FIXTURE_BIG_INT);

    expect(Envelope::fromJson(ENVELOPE_FIXTURE_BIG_INT)->deferred(10)->toJson())->toBe($expected);
});

it('keeps the verbatim payload across a priority change', function (): void {
    $expected = str_replace('"priority":0', '"priority":7', ENVELOPE_FIXTURE_BIG_INT);
    $envelope = Envelope::fromJson(ENVELOPE_FIXTURE_BIG_INT)->withPriority(7);

    expect($envelope->toJson())->toBe($expected);
    expect($envelope->deferrals)->toBe(0, 'a priority change is not a deferral');
    expect($envelope->attempt)->toBe(1);
});

it('keeps the verbatim payload when a correlation id is added', function (): void {
    $json = Envelope::fromJson(ENVELOPE_FIXTURE_BIG_INT)->withCorrelationId('trace-abc')->toJson();

    expect($json)->toContain('"correlation_id":"trace-abc","payload":{"n":12345678901234567890');
    expect($json)->toEndWith('"a":[{}],"e":{}}}');
});

it('still decodes a verbatim payload though php widens the integer', function (): void {
    $payload = Envelope::fromJson(ENVELOPE_FIXTURE_BIG_INT)->payload;

    expect($payload)->toBeInstanceOf(\stdClass::class);

    /** @var \stdClass $payload */
    // A PHP limitation, documented on `Envelope::$payloadJson`: `json_decode`
    // has no integer this wide. The bytes on the wire are untouched anyway.
    expect($payload->n)->toBeFloat();
    expect($payload->s)->toBe('x"}{');
    expect($payload->a)->toEqual([new \stdClass()]);
    expect($payload->e)->toEqual(new \stdClass());
});

it('has no verbatim span on an envelope built in php', function (): void {
    $envelope = Envelope::raw('t', 'q', ['n' => 1]);

    expect($envelope->toJson())->toEndWith('"payload":{"n":1}}');
    expect($envelope->nextAttempt()->toJson())->toEndWith('"payload":{"n":1}}');
    expect($envelope->deferred(3)->toJson())->toEndWith('"payload":{"n":1}}');
});

it('republishes an unusually spelled payload verbatim', function (): void {
    $body = '{"job_id":"8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d","job_type":"t","queue":"q","attempt":1,"enqueued_at_ms":1,"deferrals":0,"priority":0,"payload":{"url":"https:\\/\\/a\\/b","name":"\\u00e6"}}';

    expect(Envelope::fromJson($body)->toJson())->toBe($body);
});

it('refuses a next attempt at the ceiling', function (): void {
    $envelope = new Envelope(
        jobId: '8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d',
        jobType: 't',
        queue: 'q',
        attempt: Envelope::MAX_U32,
        enqueuedAtMs: 1,
    );

    expect(static fn(): Envelope => $envelope->nextAttempt())
        ->toThrow(InvalidConfiguration::class, '`attempt` must be between 1 and 4294967295');
});

it('does not escape slashes and unicode', function (): void {
    $envelope = Envelope::raw('t', 'q', ['url' => 'https://a/b', 'name' => 'æøå']);

    expect($envelope->toJson())->toContain('https://a/b');
    expect($envelope->toJson())->toContain('æøå');
});
