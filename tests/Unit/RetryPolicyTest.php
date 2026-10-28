<?php

declare(strict_types=1);

use Queuey\Backoff;
use Queuey\BackoffKind;
use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;
use Queuey\RetryDecision;
use Queuey\RetryPolicy;

const RETRY_POLICY_U32_MAX = 4294967295;

it('does not retry by default', function (): void {
    $policy = RetryPolicy::none();

    expect($policy->maxAttempts)->toBe(1);
    expect($policy->backoff->kind())->toBe(BackoffKind::None);
    expect($policy->decide(1)->isGiveUp())->toBeTrue();
});

it('gives up immediately when a single attempt is allowed', function (): void {
    $policy = RetryPolicy::new(1, Backoff::fixed(Duration::seconds(1)));

    expect($policy->decide(1)->isGiveUp())->toBeTrue();
    expect($policy->decide(1)->delay())->toBeNull();
});

it('decides the boundaries for three attempts', function (): void {
    $policy = RetryPolicy::new(3, Backoff::fixed(Duration::seconds(2)));

    expect($policy->decide(1)->equals(RetryDecision::retry(Duration::seconds(2))))->toBeTrue();
    expect($policy->decide(2)->equals(RetryDecision::retry(Duration::seconds(2))))->toBeTrue();
    expect($policy->decide(3)->isGiveUp())->toBeTrue();
    expect($policy->decide(4)->isGiveUp())->toBeTrue();
    expect($policy->decide(RETRY_POLICY_U32_MAX)->isGiveUp())->toBeTrue();
});

it('uses the failed attempt to work out the delay', function (): void {
    $policy = RetryPolicy::new(5, Backoff::exponential(Duration::seconds(1), 2.0, Duration::seconds(600), false));

    expect($policy->decide(1)->delay()?->toMs())->toBe(1_000);
    expect($policy->decide(3)->delay()?->toMs())->toBe(4_000);
});

it('builds the same policies through its named constructors', function (): void {
    $fixed = RetryPolicy::fixed(3, Duration::ms(250));
    expect($fixed->maxAttempts)->toBe(3);
    expect($fixed->backoff->kind())->toBe(BackoffKind::Fixed);
    expect($fixed->backoff->delay()->toMs())->toBe(250);

    $exponential = RetryPolicy::exponential(5);
    expect($exponential->maxAttempts)->toBe(5);
    expect($exponential->backoff->equals(Backoff::exponential()))->toBeTrue();

    expect(RetryPolicy::new(2, Backoff::none())->equals(new RetryPolicy(2, Backoff::none())))->toBeTrue();
});

it('requires at least one attempt', function (): void {
    expect(static fn(): RetryPolicy => new RetryPolicy(0, Backoff::none()))
        ->toThrow(InvalidConfiguration::class, '`maxAttempts` must be between 1 and 4294967295');
});

it('requires maxAttempts to fit an unsigned 32-bit integer', function (): void {
    expect(static fn(): RetryPolicy => new RetryPolicy(RETRY_POLICY_U32_MAX + 1, Backoff::none()))
        ->toThrow(InvalidConfiguration::class);
});

it('reads a retry decision through its accessors', function (): void {
    $retry = RetryDecision::retry(Duration::seconds(2));
    expect($retry->isRetry())->toBeTrue();
    expect($retry->isGiveUp())->toBeFalse();
    expect($retry->delay()?->toMs())->toBe(2_000);

    $giveUp = RetryDecision::giveUp();
    expect($giveUp->isRetry())->toBeFalse();
    expect($giveUp->isGiveUp())->toBeTrue();
    expect($giveUp->delay())->toBeNull();

    expect($retry->equals($giveUp))->toBeFalse();
    expect($giveUp->equals($retry))->toBeFalse();
    expect($giveUp->equals(RetryDecision::giveUp()))->toBeTrue();
    expect($retry->equals(RetryDecision::retry(Duration::seconds(3))))->toBeFalse();
});
