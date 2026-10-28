<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\FrozenClock;
use Queuey\SystemClock;

it('reads a system clock that is somewhere around now', function (): void {
    expect((new SystemClock())->nowMs())->toBeGreaterThan(1_700_000_000_000);
});

it('only moves a frozen clock when told to', function (): void {
    $clock = new FrozenClock(1_000);

    expect($clock->nowMs())->toBe(1_000);
    expect($clock->nowMs())->toBe(1_000);

    $clock->advance(Duration::seconds(2));
    expect($clock->nowMs())->toBe(3_000);

    $clock->advance(Duration::zero());
    expect($clock->nowMs())->toBe(3_000);

    $clock->setMs(42);
    expect($clock->nowMs())->toBe(42);
});

it('starts a frozen clock at zero', function (): void {
    expect((new FrozenClock())->nowMs())->toBe(0);
});
