<?php

declare(strict_types=1);

use Queuey\Backoff;
use Queuey\BackoffKind;
use Queuey\Duration;

/*
 * A port of the Rust test module in `crates/core/src/retry.rs`, value for value.
 */

/** The largest attempt number the Rust suite uses (`u32::MAX`). */
const BACKOFF_U32_MAX = 4294967295;

it('makes none a zero delay', function (): void {
    $backoff = Backoff::none();

    expect($backoff->kind())->toBe(BackoffKind::None);
    foreach ([0, 1, BACKOFF_U32_MAX] as $attempt) {
        expect($backoff->delayFor($attempt)->toMs())->toBe(0);
    }
});

it('keeps a fixed backoff constant', function (): void {
    $backoff = Backoff::fixed(Duration::ms(250));

    expect($backoff->kind())->toBe(BackoffKind::Fixed);
    expect($backoff->delay()->toMs())->toBe(250);
    foreach ([0, 1, 2, 7, BACKOFF_U32_MAX] as $attempt) {
        expect($backoff->delayFor($attempt)->toMs())->toBe(250);
    }
});

it('starts an exponential curve at the base', function (): void {
    $backoff = Backoff::exponential(Duration::ms(500), 3.0, Duration::seconds(600), false);

    expect($backoff->delayFor(1)->toMs())->toBe(500);
});

it('grows an exponential curve by its factor', function (): void {
    $backoff = Backoff::exponential(Duration::seconds(1), 2.0, Duration::seconds(600), false);

    expect($backoff->delayFor(1)->toMs())->toBe(1_000);
    expect($backoff->delayFor(2)->toMs())->toBe(2_000);
    expect($backoff->delayFor(3)->toMs())->toBe(4_000);
    expect($backoff->delayFor(4)->toMs())->toBe(8_000);
});

it('grows an exponential curve by a fractional factor', function (): void {
    $backoff = Backoff::exponential(Duration::ms(250), 1.5, Duration::seconds(60), false);

    expect($backoff->delayFor(1)->toMs())->toBe(250);
    expect($backoff->delayFor(2)->toMs())->toBe(375);
});

it('respects the exponential cap', function (): void {
    $max = Duration::seconds(10);
    $backoff = Backoff::exponential(Duration::seconds(1), 2.0, $max, false);

    expect($backoff->delayFor(5)->toMs())->toBe(10_000);
    expect($backoff->delayFor(50)->toMs())->toBe($max->toMs());
    expect($backoff->delayFor(BACKOFF_U32_MAX)->toMs())->toBe($max->toMs());
});

it('treats attempt zero as the base', function (): void {
    $backoff = Backoff::exponential(Duration::ms(120), 2.0, Duration::seconds(60), false);

    expect($backoff->delayFor(0)->toMs())->toBe(120);
});

it('handles degenerate factors', function (float $factor): void {
    $max = Duration::seconds(60);
    $backoff = Backoff::exponential(Duration::ms(100), $factor, $max, false);

    foreach ([0, 1, 2, 1_000, BACKOFF_U32_MAX] as $attempt) {
        expect($backoff->delayFor($attempt)->toMs())->toBeLessThanOrEqual($max->toMs());
    }
    expect($backoff->delayFor(3)->toMs())->toBe(100, 'degenerate factors collapse to a constant base');
})->with([
    'nan' => [\NAN],
    'inf' => [\INF],
    '-inf' => [-\INF],
    'zero' => [0.0],
    'negative' => [-2.0],
]);

it('never underflows with a shrinking factor', function (): void {
    $backoff = Backoff::exponential(Duration::seconds(1), 0.5, Duration::seconds(60), false);

    expect($backoff->delayFor(2)->toMs())->toBe(500);
    expect($backoff->delayFor(BACKOFF_U32_MAX)->toMs())->toBe(0);
});

it('stays at zero when the base or the max is zero', function (): void {
    $zeroBase = Backoff::exponential(Duration::zero(), 2.0, Duration::seconds(60), true);
    $zeroMax = Backoff::exponential(Duration::seconds(1), 2.0, Duration::zero(), false);

    foreach ([0, 1, 9, BACKOFF_U32_MAX] as $attempt) {
        expect($zeroBase->delayFor($attempt)->toMs())->toBe(0);
        expect($zeroMax->delayFor($attempt)->toMs())->toBe(0);
    }
});

it('keeps jitter within bounds', function (): void {
    $max = Duration::seconds(30);
    $backoff = Backoff::exponential(Duration::seconds(1), 2.0, $max, true);

    $sawBelowCap = false;
    for ($i = 0; $i < 2_000; ++$i) {
        $delay = $backoff->delayFor(3);
        expect($delay->toMs())->toBeLessThanOrEqual(4_000, 'jitter exceeded the uncapped value');
        expect($backoff->delayFor(99)->toMs())->toBeLessThanOrEqual($max->toMs());
        if ($delay->toMs() < 4_000) {
            $sawBelowCap = true;
        }
    }

    expect($sawBelowCap)->toBeTrue('jitter never produced a value below the computed delay');
});

it('does not produce a constant jitter', function (): void {
    $backoff = Backoff::exponential(Duration::seconds(10), 2.0, Duration::seconds(600), true);
    $first = $backoff->delayFor(4)->toMs();

    $differs = false;
    for ($i = 0; $i < 100; ++$i) {
        if ($backoff->delayFor(4)->toMs() !== $first) {
            $differs = true;

            break;
        }
    }

    expect($differs)->toBeTrue('jitter produced the same value 100 times');
});

it('keeps huge attempts within the cap with jitter', function (): void {
    $backoff = Backoff::exponential();

    foreach ([0, 1, intdiv(BACKOFF_U32_MAX, 2), BACKOFF_U32_MAX] as $attempt) {
        expect($backoff->delayFor($attempt)->toMs())->toBeLessThanOrEqual(300_000);
    }
});

it('defaults to the documented curve', function (): void {
    $backoff = Backoff::exponential();

    expect($backoff->kind())->toBe(BackoffKind::Exponential);
    expect($backoff->base()->toMs())->toBe(1_000);
    expect($backoff->factor())->toBe(2.0);
    expect($backoff->max()->toMs())->toBe(300_000);
    expect($backoff->jitter())->toBeTrue();
    expect($backoff->equals(Backoff::exponential(Duration::seconds(1), 2.0, Duration::minutes(5), true)))->toBeTrue();
});

it('distinguishes kinds when comparing', function (): void {
    expect(Backoff::none()->equals(Backoff::none()))->toBeTrue();
    expect(Backoff::fixed(Duration::ms(5))->equals(Backoff::fixed(Duration::ms(5))))->toBeTrue();
    expect(Backoff::fixed(Duration::ms(5))->equals(Backoff::fixed(Duration::ms(6))))->toBeFalse();
    expect(Backoff::none()->equals(Backoff::fixed(Duration::zero())))->toBeFalse();
});

it('rounds sub-millisecond delays up rather than to zero', function (): void {
    // Rust keeps nanosecond precision; PHP stores whole milliseconds, and
    // rounding a positive delay to 0 would turn a backoff into a busy loop.
    $backoff = Backoff::exponential(Duration::seconds(1), 0.5, Duration::seconds(60), false);

    expect($backoff->delayFor(12)->toMs())->toBe(1, '1000 * 0.5^11 = 0.488ms rounds up to 1ms');
    expect($backoff->delayFor(20)->toMs())->toBe(1);
    expect($backoff->delayFor(11)->toMs())->toBe(1, '0.977ms rounds up to 1ms');
    expect($backoff->delayFor(10)->toMs())->toBe(2, '1.953ms rounds up to 2ms, it does not round down');
});

it('never throws for a max near the integer ceiling', function (): void {
    $huge = Duration::ms(\PHP_INT_MAX);

    foreach ([false, true] as $jitter) {
        $backoff = Backoff::exponential($huge, 2.0, $huge, $jitter);
        foreach ([0, 1, 2, 64, BACKOFF_U32_MAX] as $attempt) {
            $delay = $backoff->delayFor($attempt);
            expect($delay->toMs())->toBeGreaterThanOrEqual(0);
            expect($delay->toMs())->toBeLessThanOrEqual(\PHP_INT_MAX);
        }
    }

    expect(Backoff::exponential($huge, 2.0, $huge, false)->delayFor(1)->toMs())
        ->toBe(\PHP_INT_MAX, 'the cap is the answer, not a wrapped negative');
});

it('still caps with a cap near the ceiling', function (): void {
    $backoff = Backoff::exponential(Duration::seconds(1), 2.0, Duration::ms(\PHP_INT_MAX), false);

    expect($backoff->delayFor(1)->toMs())->toBe(1_000);
    expect($backoff->delayFor(BACKOFF_U32_MAX)->toMs())->toBe(\PHP_INT_MAX, 'an infinite curve saturates at max');
});
