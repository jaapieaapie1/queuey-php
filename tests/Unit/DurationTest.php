<?php

declare(strict_types=1);

use Queuey\Duration;
use Queuey\Exception\InvalidConfiguration;

it('produces milliseconds from every constructor', function (): void {
    expect(Duration::zero()->toMs())->toBe(0);
    expect(Duration::ms(500)->toMs())->toBe(500);
    expect(Duration::seconds(1)->toMs())->toBe(1_000);
    expect(Duration::minutes(2)->toMs())->toBe(120_000);
    expect(Duration::hours(1)->toMs())->toBe(3_600_000);
    expect(Duration::days(1)->toMs())->toBe(86_400_000);
    expect(Duration::days(7)->toMs())->toBe(604_800_000);
});

it('rounds fractional inputs up', function (): void {
    expect(Duration::seconds(0.0001)->toMs())->toBe(1, 'sub-millisecond rounds up, never down');
    expect(Duration::seconds(1.5001)->toMs())->toBe(1_501);
    expect(Duration::seconds(1.5)->toMs())->toBe(1_500);
    expect(Duration::seconds(0.03)->toMs())->toBe(30);
    expect(Duration::minutes(1.5)->toMs())->toBe(90_000);
});

it('keeps zero at zero', function (): void {
    expect(Duration::seconds(0)->isZero())->toBeTrue();
    expect(Duration::seconds(0.0)->isZero())->toBeTrue();
    expect(Duration::ms(1)->isZero())->toBeFalse();
});

it('rejects negative milliseconds', function (): void {
    expect(static fn(): Duration => Duration::ms(-1))->toThrow(InvalidConfiguration::class);
});

it('rejects negative seconds', function (): void {
    expect(static fn(): Duration => Duration::seconds(-0.5))->toThrow(InvalidConfiguration::class);
});

it('parses a duration', function (string $text, int $expectedMs): void {
    expect(Duration::parse($text)->toMs())->toBe($expectedMs);
})->with([
    'milliseconds' => ['500ms', 500],
    'seconds' => ['1s', 1_000],
    'minutes' => ['2m', 120_000],
    'hours' => ['1h', 3_600_000],
    'days' => ['7d', 604_800_000],
    'bare integer is seconds' => ['30', 30_000],
    'bare zero' => ['0', 0],
    'zero with unit' => ['0ms', 0],
    'surrounding whitespace' => ['  5 s ', 5_000],
    'inner whitespace' => ['5 ms', 5],
    'tabs and newlines' => ["\t300\tms\n", 300],
    'leading zeroes' => ['007s', 7_000],
]);

it('rejects unparsable input', function (string $text): void {
    expect(static fn(): Duration => Duration::parse($text))->toThrow(InvalidConfiguration::class);
})->with(static function (): iterable {
    foreach (['', '   ', 's', 'abc', '1x', '1.5s', '-1s', '1sm', '1 m s', 'ms1', '+2s', '1 sec'] as $bad) {
        yield \sprintf('`%s`', $bad) => [$bad];
    }
});

it('mentions the input and the units in a parse error', function (): void {
    $parse = static fn(): Duration => Duration::parse('1x');

    expect($parse)->toThrow(InvalidConfiguration::class, '`1x`');
    expect($parse)->toThrow(InvalidConfiguration::class, '`ms`');
});

it('rejects an overflowing parse', function (): void {
    expect(static fn(): Duration => Duration::parse('99999999999999999999999'))
        ->toThrow(InvalidConfiguration::class);
});

it('rejects a parse that overflows once scaled', function (): void {
    expect(static fn(): Duration => Duration::parse('9223372036854775807d'))
        ->toThrow(InvalidConfiguration::class);
});

it('converts to seconds', function (): void {
    expect(Duration::ms(1_500)->toSeconds())->toBe(1.5);
    expect(Duration::zero()->toSeconds())->toBe(0.0);
});

it('compares two durations', function (): void {
    $short = Duration::ms(100);
    $long = Duration::seconds(1);

    expect($short->equals(Duration::ms(100)))->toBeTrue();
    expect($short->equals($long))->toBeFalse();
    expect($short->compare($long))->toBe(-1);
    expect($long->compare($short))->toBe(1);
    expect($short->compare(Duration::ms(100)))->toBe(0);
    expect($short->min($long)->toMs())->toBe(100);
    expect($short->max($long)->toMs())->toBe(1_000);
    expect($short->plus($long)->toMs())->toBe(1_100);
});

it('prints itself as milliseconds', function (): void {
    expect((string) Duration::ms(1_500))->toBe('1500ms');
});

it('rejects the overflow boundary rather than wrapping', function (): void {
    // `(float) PHP_INT_MAX` is 2^63, so this used to sail past the guard and
    // come back out as a negative millisecond count.
    expect(static fn(): Duration => Duration::seconds(9223372036854775.5))
        ->toThrow(InvalidConfiguration::class, 'does not fit in a millisecond count');
});

it('accepts just below the overflow boundary', function (): void {
    expect(Duration::seconds(9223372036854.0)->toMs())->toBeGreaterThan(0);
    expect(Duration::ms(\PHP_INT_MAX)->toMs())->toBe(\PHP_INT_MAX);
});

it('rejects an overflowing unit conversion', function (): void {
    expect(static fn(): Duration => Duration::days(1e18))->toThrow(InvalidConfiguration::class);
});

it('adds two durations', function (): void {
    expect(Duration::seconds(1)->plus(Duration::ms(500))->toMs())->toBe(1_500);
    expect(Duration::zero()->plus(Duration::zero())->toMs())->toBe(0);
    expect(Duration::ms(\PHP_INT_MAX)->plus(Duration::zero())->toMs())->toBe(\PHP_INT_MAX);

    $base = Duration::ms(100);
    $base->plus(Duration::ms(1));
    expect($base->toMs())->toBe(100, 'the original is untouched');
});

it('rejects an overflowing addition instead of throwing a type error', function (): void {
    expect(static fn(): Duration => Duration::ms(\PHP_INT_MAX)->plus(Duration::ms(1)))
        ->toThrow(InvalidConfiguration::class, 'does not fit in a millisecond count');
});

/* Rust's `str::trim()` drops every `White_Space` char, not just the ASCII five. */
it('trims unicode whitespace when parsing', function (string $text, int $expectedMs): void {
    expect(Duration::parse($text)->toMs())->toBe($expectedMs);
})->with([
    'leading nbsp' => ["\u{00A0}5s", 5_000],
    'trailing nbsp' => ["5s\u{00A0}", 5_000],
    'nbsp between value and unit' => ["5\u{00A0}s", 5_000],
    'nbsp on both sides' => ["\u{00A0} 300ms \u{00A0}", 300],
    'ideographic space' => ["\u{3000}2m\u{3000}", 120_000],
]);

it('still rejects non-whitespace noise when parsing', function (): void {
    expect(static fn(): Duration => Duration::parse("5\u{00A0}sec"))->toThrow(InvalidConfiguration::class);
});
