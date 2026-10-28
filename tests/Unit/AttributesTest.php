<?php

declare(strict_types=1);

use PHPUnit\Framework\Assert;
use Queuey\Attributes\OnQueue;
use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\Attributes\Retry;
use Queuey\BackoffKind;
use Queuey\Exception\InvalidConfiguration;
use Queuey\Internal\QueueSetRegistry;
use Queuey\Tests\Fixtures\HugeTtlQueues;
use Queuey\Tests\Fixtures\TestQueues;

// --- queue-set resolution -------------------------------------------------

it('names queues in snake_case and prefixes them by default', function (): void {
    expect(TestQueues::Alpha->queueName())->toBe('test.alpha');
    expect(TestQueues::SendEmails->queueName())->toBe('test.send_emails');
    expect(TestQueues::HTTPCalls->queueName())->toBe('test.http_calls');
});

it('lets an explicit name win over the snake_case form', function (): void {
    expect(TestQueues::Beta->queueName())->toBe('test.beta-queue');
});

it('gives a case without a #[Queue] attribute every default', function (): void {
    $config = TestQueues::Alpha->config();

    expect($config->name)->toBe('test.alpha');
    expect($config->prefetch)->toBe(16);
    expect($config->durable)->toBeTrue();
    expect($config->messageTtl)->toBeNull();
    expect($config->maxPriority)->toBe(10);
    expect($config->retry->maxAttempts)->toBe(1);
});

it('carries every queue key through to the config', function (): void {
    $config = TestQueues::Beta->config();

    expect($config->prefetch)->toBe(4);
    expect($config->durable)->toBeFalse();
    expect($config->messageTtl?->toMs())->toBe(30_000);
    expect($config->maxPriority)->toBeNull('maxPriority: 0 turns priorities off');
});

it('turns a queue #[Retry] into the policy', function (): void {
    $retry = TestQueues::SendEmails->config()->retry;

    expect($retry->maxAttempts)->toBe(3);
    expect($retry->backoff->kind())->toBe(BackoffKind::Fixed);
    expect($retry->backoff->delay()->toMs())->toBe(2_000);
});

it('lists every case and looks one up by queue name', function (): void {
    expect(TestQueues::all())->toBe(
        [TestQueues::Alpha, TestQueues::Beta, TestQueues::SendEmails, TestQueues::HTTPCalls],
    );
    expect(TestQueues::fromQueueName('test.beta-queue'))->toBe(TestQueues::Beta);
    expect(TestQueues::fromQueueName('test.http_calls'))->toBe(TestQueues::HTTPCalls);
    expect(TestQueues::fromQueueName('test.nope'))->toBeNull();
    expect(TestQueues::fromQueueName('alpha'))->toBeNull('the lookup uses the qualified name');
});

// --- snake_case -----------------------------------------------------------

it('converts an identifier to snake_case', function (string $identifier, string $expected): void {
    expect(QueueSetRegistry::snakeCase($identifier))->toBe($expected);
})->with([
    'Emails' => ['Emails', 'emails'],
    'SendEmails' => ['SendEmails', 'send_emails'],
    'HTTPCalls' => ['HTTPCalls', 'http_calls'],
    'HTTP' => ['HTTP', 'http'],
    'A' => ['A', 'a'],
    'ImageResize2' => ['ImageResize2', 'image_resize2'],
    'Resize2xImages' => ['Resize2xImages', 'resize2x_images'],
    'Already_Snake' => ['Already_Snake', 'already_snake'],
    'already_snake' => ['already_snake', 'already_snake'],
    'XMLHTTPRequest' => ['XMLHTTPRequest', 'xmlhttp_request'],
    'SendEmailsV2' => ['SendEmailsV2', 'send_emails_v2'],
    'underscores only' => ['__', ''],
    'an empty identifier' => ['', ''],
    // Rust's `to_snake_case` is Unicode-aware; so is this one.
    'Ärger' => ['Ärger', 'ärger'],
    'ÄrgerMitÖl' => ['ÄrgerMitÖl', 'ärger_mit_öl'],
]);

// --- prefix ---------------------------------------------------------------

it('joins the prefix to the queue name', function (): void {
    expect((new Queues('myapp'))->qualify('emails'))->toBe('myapp.emails');
    expect((new Queues())->qualify('emails'))->toBe('emails');
    expect((new Queues('myapp.jobs'))->qualify('emails'))
        ->toBe('myapp.jobs.emails', 'dots inside a prefix are a normal AMQP namespace');
});

it('rejects an empty prefix', function (): void {
    expect(static fn(): Queues => new Queues(''))
        ->toThrow(InvalidConfiguration::class, '`prefix` must not be empty');
});

it('rejects a blank prefix', function (): void {
    expect(static fn(): Queues => new Queues('   '))
        ->toThrow(InvalidConfiguration::class, '`prefix` must not be only whitespace');
});

it('rejects a prefix ending in the separator', function (): void {
    expect(static fn(): Queues => new Queues('a.'))
        ->toThrow(InvalidConfiguration::class, '`prefix` must not end with `.`');
});

// --- #[Queue] validation --------------------------------------------------

it('rejects a queue prefetch of zero', function (): void {
    expect(static fn(): Queue => new Queue(prefetch: 0))
        ->toThrow(InvalidConfiguration::class, '`prefetch` must be between 1 and 65535');
});

it('rejects a queue prefetch of 65536', function (): void {
    expect(static fn(): Queue => new Queue(prefetch: 65536))
        ->toThrow(InvalidConfiguration::class, '`prefetch` must be between 1 and 65535');
});

it('bounds the queue max priority', function (): void {
    expect(static fn(): Queue => new Queue(maxPriority: 256))->toThrow(InvalidConfiguration::class);
});

it('rejects a blank queue name', function (): void {
    expect(static fn(): Queue => new Queue(name: '  '))->toThrow(InvalidConfiguration::class);
});

it('rejects a zero message ttl on a queue', function (): void {
    expect(static fn(): Queue => new Queue(messageTtl: '0s'))
        ->toThrow(InvalidConfiguration::class, '`messageTtl` must be greater than zero');
});

it('rejects an unparsable message ttl on a queue', function (): void {
    expect(static fn(): Queue => new Queue(messageTtl: 'soon'))->toThrow(InvalidConfiguration::class);
});

// --- #[Retry] validation --------------------------------------------------

it('defaults to a single attempt without backoff', function (): void {
    $policy = (new Retry())->toPolicy();

    expect($policy->maxAttempts)->toBe(1);
    expect($policy->backoff->kind())->toBe(BackoffKind::None);
});

it('builds a fixed backoff policy', function (): void {
    $policy = (new Retry(maxAttempts: 4, backoff: 'fixed', delay: '250ms'))->toPolicy();

    expect($policy->maxAttempts)->toBe(4);
    expect($policy->backoff->kind())->toBe(BackoffKind::Fixed);
    expect($policy->backoff->delay()->toMs())->toBe(250);
});

it('fills in the exponential backoff defaults', function (): void {
    $policy = (new Retry(maxAttempts: 5, backoff: 'exponential'))->toPolicy();

    expect($policy->maxAttempts)->toBe(5);
    expect($policy->backoff->kind())->toBe(BackoffKind::Exponential);
    expect($policy->backoff->base()->toMs())->toBe(1_000);
    expect($policy->backoff->factor())->toBe(2.0);
    expect($policy->backoff->max()->toMs())->toBe(300_000);
    expect($policy->backoff->jitter())->toBeTrue();
});

it('honours every exponential knob', function (): void {
    $policy = (new Retry(
        maxAttempts: 7,
        backoff: 'exponential',
        base: '250ms',
        factor: 1.5,
        max: '1m',
        jitter: false,
    ))->toPolicy();

    expect($policy->backoff->base()->toMs())->toBe(250);
    expect($policy->backoff->factor())->toBe(1.5);
    expect($policy->backoff->max()->toMs())->toBe(60_000);
    expect($policy->backoff->jitter())->toBeFalse();
    expect($policy->backoff->delayFor(2)->toMs())->toBe(375);
});

it('rejects a maxAttempts of zero', function (): void {
    expect(static fn(): Retry => new Retry(maxAttempts: 0))->toThrow(
        InvalidConfiguration::class,
        '`maxAttempts` must be between 1 and 4294967295; 1 means no retries',
    );
});

it('rejects a zero delay', function (string $text): void {
    expect(static fn(): Retry => new Retry(backoff: 'fixed', delay: $text))
        ->toThrow(InvalidConfiguration::class, '`delay` must be greater than zero');
})->with([
    // PHP casts the numeric key `'0'` to an int, so Pest labels that row `('0')`
    // by its value; the other three keep their names.
    '0' => ['0'],
    '0s' => ['0s'],
    '0ms' => ['0ms'],
    ' 0 ms ' => [' 0 ms '],
]);

it('rejects a zero base', function (): void {
    expect(static fn(): Retry => new Retry(backoff: 'exponential', base: '0ms'))
        ->toThrow(InvalidConfiguration::class, '`base` must be greater than zero');
});

it('accepts a delay only with a fixed backoff', function (): void {
    expect(static fn(): Retry => new Retry(backoff: 'exponential', delay: '1s'))
        ->toThrow(InvalidConfiguration::class, "`delay` is only valid with `backoff: 'fixed'`");
});

it('rejects a delay with no backoff too', function (): void {
    expect(static fn(): Retry => new Retry(backoff: 'none', delay: '1s'))
        ->toThrow(InvalidConfiguration::class, "`delay` is only valid with `backoff: 'fixed'`");
});

it('accepts the exponential keys only with an exponential backoff', function (): void {
    $cases = [
        'base' => static fn(): Retry => new Retry(backoff: 'fixed', delay: '1s', base: '1s'),
        'factor' => static fn(): Retry => new Retry(backoff: 'fixed', delay: '1s', factor: 2.0),
        'max' => static fn(): Retry => new Retry(backoff: 'fixed', delay: '1s', max: '1m'),
        'jitter' => static fn(): Retry => new Retry(backoff: 'fixed', delay: '1s', jitter: true),
    ];

    foreach ($cases as $key => $build) {
        expect($build)->toThrow(
            InvalidConfiguration::class,
            \sprintf("`%s` is only valid with `backoff: 'exponential'`", $key),
        );
    }
});

it('requires a delay with a fixed backoff', function (): void {
    expect(static fn(): Retry => new Retry(backoff: 'fixed'))
        ->toThrow(InvalidConfiguration::class, "`backoff: 'fixed'` requires `delay: \"...\"`");
});

it('refuses a base larger than the max', function (): void {
    expect(static fn(): Retry => new Retry(backoff: 'exponential', base: '10m', max: '1m'))
        ->toThrow(InvalidConfiguration::class, '`base` must be <= `max`');
});

it('accepts a base equal to the max', function (): void {
    $policy = (new Retry(backoff: 'exponential', base: '1m', max: '1m'))->toPolicy();

    expect($policy->backoff->base()->toMs())->toBe(60_000);
});

it('rejects a degenerate factor', function (float $factor): void {
    expect(static fn(): Retry => new Retry(backoff: 'exponential', factor: $factor))
        ->toThrow(InvalidConfiguration::class, '`factor` must be a finite number greater than 0');
})->with([
    'zero' => [0.0],
    'negative' => [-2.0],
    'nan' => [\NAN],
    'infinite' => [\INF],
]);

it('rejects an unknown backoff', function (): void {
    expect(static fn(): Retry => new Retry(backoff: 'linear'))->toThrow(
        InvalidConfiguration::class,
        'unknown backoff `linear`, expected "none", "fixed" or "exponential"',
    );
});

// --- #[OnQueue] validation ------------------------------------------------

it('keeps the arguments given to #[OnQueue]', function (): void {
    $attribute = new OnQueue(TestQueues::Alpha, name: 'myapp::jobs::SendEmail');

    expect($attribute->queue)->toBe(TestQueues::Alpha);
    expect($attribute->name)->toBe('myapp::jobs::SendEmail');
    expect($attribute->retry)->toBeNull();
});

it('rejects a blank name on #[OnQueue]', function (): void {
    expect(static fn(): OnQueue => new OnQueue(TestQueues::Alpha, name: ' '))
        ->toThrow(InvalidConfiguration::class);
});

// --- message TTL bounds ---------------------------------------------------

it('rejects a message ttl a broker cannot express', function (string $ttl): void {
    expect(static fn(): Queue => new Queue(messageTtl: $ttl))
        ->toThrow(InvalidConfiguration::class, '`messageTtl` must be between 1ms and 4294967295ms (~49.7 days)');
})->with([
    '60d' => ['60d'],
    '100d' => ['100d'],
    'one past the ceiling' => ['4294967296ms'],
]);

it('accepts the message ttl ceiling itself', function (): void {
    $attribute = new Queue(messageTtl: '4294967295ms');

    expect($attribute->toConfig('q')->messageTtl?->toMs())->toBe(4294967295);
    expect((new Queue(messageTtl: '7d'))->toConfig('q')->messageTtl?->toMs())->toBe(604_800_000);
});

it('names the enum case in a message ttl error that comes from one', function (): void {
    try {
        HugeTtlQueues::Alpha->config();
        Assert::fail('expected a rejection');
    } catch (InvalidConfiguration $e) {
        expect($e->getMessage())->toContain('HugeTtlQueues::Alpha');
        expect($e->getMessage())->toContain('`messageTtl` must be between 1ms and 4294967295ms');
    }
});
