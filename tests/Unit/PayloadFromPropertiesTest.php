<?php

declare(strict_types=1);

use Queuey\Exception\DecodeError;
use Queuey\Internal\Json;
use Queuey\Tests\Fixtures\Assorted;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Loose;
use Queuey\Tests\Fixtures\Nothing;

it('round trips a job through its payload', function (): void {
    $job = new Assorted('a@b.c', 3, true, ['x', 'y']);
    $payload = Json::toArray(Json::normalize($job->toPayload()));

    expect(Assorted::fromPayload($payload))->toEqual($job);
});

it('builds a payload object in declaration order', function (): void {
    $payload = (new Assorted('a@b.c'))->toPayload();

    expect($payload)->toBeInstanceOf(\stdClass::class);
    expect(array_keys(get_object_vars($payload)))->toBe(['to', 'count', 'urgent', 'tags']);
    expect($payload->to)->toBe('a@b.c');
    expect($payload->count)->toBeNull();
    expect($payload->urgent)->toBeFalse();
    expect($payload->tags)->toBe([]);
});

it('encodes a job without properties as an empty object', function (): void {
    expect(Json::encode((new Nothing())->toPayload()))->toBe('{}');
    expect(Nothing::fromPayload([]))->toEqual(new Nothing());
});

it('fills defaults in for missing keys', function (): void {
    $job = Assorted::fromPayload(['to' => 'a@b.c']);

    expect($job->to)->toBe('a@b.c');
    expect($job->count)->toBeNull();
    expect($job->urgent)->toBeFalse();
    expect($job->tags)->toBe([]);
});

it('accepts an object payload', function (): void {
    expect(Greet::fromPayload((object) ['name' => 'ada']))->toEqual(new Greet('ada'));
});

it('rejects a missing required field', function (): void {
    expect(static fn(): Greet => Greet::fromPayload([]))
        ->toThrow(DecodeError::class, 'missing field `name`');
});

it('rejects an unknown field', function (): void {
    expect(static fn(): Greet => Greet::fromPayload(['name' => 'ada', 'nope' => 1]))
        ->toThrow(DecodeError::class, 'unknown field `nope`');
});

it('rejects an unknown field for a job without a constructor', function (): void {
    expect(static fn(): Nothing => Nothing::fromPayload(['nope' => 1]))
        ->toThrow(DecodeError::class, 'unknown field `nope`');
});

it('rejects a type mismatch', function (): void {
    expect(static fn(): Greet => Greet::fromPayload(['name' => ['not', 'a', 'string']]))
        ->toThrow(DecodeError::class);
});

it('rejects a list payload', function (): void {
    expect(static fn(): Greet => Greet::fromPayload([1, 2, 3]))
        ->toThrow(DecodeError::class, 'must be a JSON object');
});

it('rejects a scalar payload', function (): void {
    expect(static fn(): Greet => Greet::fromPayload('ada'))
        ->toThrow(DecodeError::class, 'must be a JSON object, got string');
});

it('does not silently null an untyped or mixed parameter', function (): void {
    $job = Loose::fromPayload(['anything' => 1, 'untyped' => 'two', 'maybe' => 'x']);

    expect($job->anything)->toBe(1);
    expect($job->untyped)->toBe('two');
    expect($job->maybe)->toBe('x');
});

it('reports a missing mixed field rather than defaulting it', function (): void {
    expect(static fn(): Loose => Loose::fromPayload(['untyped' => 1, 'maybe' => null]))
        ->toThrow(DecodeError::class, 'missing field `anything`');
});

it('reports a missing untyped field rather than defaulting it', function (): void {
    expect(static fn(): Loose => Loose::fromPayload(['anything' => 1, 'maybe' => null]))
        ->toThrow(DecodeError::class, 'missing field `untyped`');
});

it('still defaults a missing explicitly nullable field to null', function (): void {
    $job = Loose::fromPayload(['anything' => 1, 'untyped' => 2]);

    expect($job->maybe)->toBeNull();
});

it('still accepts an explicit null for a mixed field', function (): void {
    $job = Loose::fromPayload(['anything' => null, 'untyped' => null, 'maybe' => null]);

    expect($job->anything)->toBeNull();
    expect($job->untyped)->toBeNull();
});
