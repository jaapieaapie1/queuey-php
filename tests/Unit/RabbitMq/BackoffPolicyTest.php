<?php

declare(strict_types=1);

use Queuey\Backoff;
use Queuey\BackoffKind;
use Queuey\Duration;
use Queuey\RabbitMq\Attempt;
use Queuey\RabbitMq\BackoffPolicy;
use Queuey\RabbitMq\Rebuilding;
use Queuey\RabbitMq\ReconnectPolicy;

it('retries forever with a jittered exponential backoff by default', function (): void {
    $policy = BackoffPolicy::new();

    expect($policy->maxAttempts)->toBeNull();
    expect($policy->backoff->kind())->toBe(BackoffKind::Exponential);
    expect($policy->backoff->base()->toMs())->toBe(500);
    expect($policy->backoff->factor())->toBe(2.0);
    expect($policy->backoff->max()->toMs())->toBe(30_000);
    expect($policy->backoff->jitter())->toBeTrue('a fleet must not reconnect in lockstep');
});

it('makes the first attempt after a drop immediately', function (): void {
    expect(backoffDelay(BackoffPolicy::new(), 0)?->toMs())->toBe(0);
});

it('always offers another attempt on an unlimited policy', function (): void {
    $policy = BackoffPolicy::new();
    foreach ([0, 1, 100, 4294967295] as $failures) {
        expect(backoffDelay($policy, $failures))->not->toBeNull(\sprintf('failures = %d', $failures));
    }
});

it('stops a bounded policy at the limit', function (): void {
    $policy = BackoffPolicy::new()->withMaxAttempts(3);

    // Three attempts are made: after 0, 1 and 2 failures.
    expect(backoffDelay($policy, 0))->not->toBeNull();
    expect(backoffDelay($policy, 1))->not->toBeNull();
    expect(backoffDelay($policy, 2))->not->toBeNull();
    expect(backoffDelay($policy, 3))->toBeNull('the third failure is the last');
    expect(backoffDelay($policy, 4))->toBeNull();
});

it('never reconnects on a zero-attempt policy', function (): void {
    expect(backoffDelay(BackoffPolicy::new()->withMaxAttempts(0), 0))->toBeNull();
});

it('grows the delay and caps it', function (): void {
    // Without jitter the schedule is exact, so it can be asserted on.
    $policy = BackoffPolicy::new()->withBackoff(Backoff::exponential(
        base: Duration::ms(500),
        factor: 2.0,
        max: Duration::seconds(30),
        jitter: false,
    ));

    expect(backoffDelay($policy, 1)?->toMs())->toBe(500);
    expect(backoffDelay($policy, 2)?->toMs())->toBe(1_000);
    expect(backoffDelay($policy, 3)?->toMs())->toBe(2_000);
    expect(backoffDelay($policy, 20)?->toMs())->toBe(30_000, 'capped');
});

it('never lets a jittered delay exceed the cap', function (): void {
    $policy = BackoffPolicy::new();
    for ($failures = 1; $failures < 40; ++$failures) {
        $delay = backoffDelay($policy, $failures);
        expect($delay)->not->toBeNull();
        expect($delay?->toMs())->toBeLessThanOrEqual(BackoffPolicy::DEFAULT_MAX_MS);
    }
});

it('lets a custom policy decide on the error and the target', function (): void {
    $picky = new class implements ReconnectPolicy {
        public function nextDelay(Attempt $attempt): ?Duration
        {
            // A subscription failing on a live connection is usually a queue
            // that is gone; waiting does not bring it back.
            if ($attempt->rebuilding === Rebuilding::Consumer && $attempt->failures >= 1) {
                return null;
            }
            if ($attempt->error !== null && str_contains($attempt->error->getMessage(), 'ACCESS_REFUSED')) {
                return null;
            }

            return Duration::seconds(1);
        }
    };

    $refused = new \RuntimeException('ACCESS_REFUSED - login was refused');
    expect($picky->nextDelay(Attempt::after(Rebuilding::Connection, 1, $refused)))
        ->toBeNull('credentials will not fix themselves');

    $flaky = new \RuntimeException('connection reset by peer');
    expect($picky->nextDelay(Attempt::after(Rebuilding::Connection, 9, $flaky))?->toMs())
        ->toBe(1_000, 'a network blip is retried regardless of the count');
    expect($picky->nextDelay(Attempt::after(Rebuilding::Consumer, 1, $flaky)))
        ->toBeNull('but a consumer is given up on');
    expect($picky->nextDelay(Attempt::first(Rebuilding::Consumer))?->toMs())->toBe(1_000);
});

it('carries no error on the first attempt', function (): void {
    $attempt = Attempt::first(Rebuilding::Connection);
    expect($attempt->failures)->toBe(0);
    expect($attempt->error)->toBeNull();
    expect($attempt->rebuilding)->toBe(Rebuilding::Connection);
});

function backoffDelay(ReconnectPolicy $policy, int $failures): ?Duration
{
    $error = new \RuntimeException('broker went away');
    $attempt = $failures === 0
        ? Attempt::first(Rebuilding::Connection)
        : Attempt::after(Rebuilding::Connection, $failures, $error);

    return $policy->nextDelay($attempt);
}
