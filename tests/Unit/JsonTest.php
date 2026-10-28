<?php

declare(strict_types=1);

use Queuey\Internal\Json;

it('encodes with the wire flags', function (): void {
    expect(Json::encode('https://a/b'))->toBe('"https://a/b"', 'slashes are not escaped');
    expect(Json::encode('æøå'))->toBe('"æøå"', 'unicode is not escaped');
    expect(Json::encode(1.0))->toBe('1.0', 'a zero fraction is preserved');
});

it('decodes faithfully into objects', function (): void {
    $decoded = Json::decode('{"a":{"b":1},"c":[]}');

    expect($decoded)->toBeInstanceOf(\stdClass::class);

    /** @var \stdClass $decoded */
    expect($decoded->a)->toBeInstanceOf(\stdClass::class);
    expect($decoded->c)->toBe([]);
});

it('turns associative arrays into objects when normalizing', function (): void {
    expect(Json::encode(Json::normalize(['a' => 1])))->toBe('{"a":1}');
    expect(Json::encode(Json::normalize(new \stdClass())))->toBe('{}');
    expect(Json::encode(Json::normalize([])))->toBe('[]', 'an empty list stays a list');
    expect(Json::encode(Json::normalize([1, 2])))->toBe('[1,2]');
});

it('unwraps nested objects when converting to an array', function (): void {
    expect(Json::toArray(Json::decode('{"a":{"b":1},"c":[1,2]}')))->toBe(['a' => ['b' => 1], 'c' => [1, 2]]);
    expect(Json::toArray(new \stdClass()))->toBe([]);
    expect(Json::toArray(7))->toBe(7);
    expect(Json::toArray(null))->toBeNull();
});

it('throws when decoding garbage', function (): void {
    expect(static fn(): mixed => Json::decode('{'))->toThrow(\JsonException::class);
});

it('throws when encoding an unencodable value', function (): void {
    expect(static fn(): string => Json::encode(\NAN))->toThrow(\JsonException::class);
});

// --- the raw top-level value scanner --------------------------------------

it('finds the raw top-level value', function (string $json, string $key, ?string $expected): void {
    expect(Json::rawTopLevelValue($json, $key))->toBe($expected);
})->with(jsonRawValueCases());

it('uses scanner fixtures that are valid json, or deliberately not', function (): void {
    // The scanner is only ever handed bodies `json_decode` already accepted,
    // so the fixtures above have to be real JSON where a value is expected.
    foreach (jsonRawValueCases() as [$json, $key, $expected]) {
        if ($expected === null) {
            continue;
        }
        json_decode($json, false, 512, \JSON_THROW_ON_ERROR);
        json_decode($expected, false, 512, \JSON_THROW_ON_ERROR);
        expect(json_last_error())->toBe(\JSON_ERROR_NONE, $json);
    }
});

it('decodes the raw span to the same value as the whole body', function (): void {
    $body = '{"a":1,"payload":{"n":12345678901234567890,"s":"x\"}{","a":[{}],"e":{}}}';
    $raw = Json::rawTopLevelValue($body, 'payload');
    $whole = Json::decode($body);

    expect($raw)->not->toBeNull();
    expect($whole)->toBeInstanceOf(\stdClass::class);

    /**
     * @var string    $raw
     * @var \stdClass $whole
     */
    expect(Json::decode($raw))->toEqual($whole->payload);
});

/**
 * @return array<string, array{string, string, string|null}>
 */
function jsonRawValueCases(): array
{
    return [
        'only key' => ['{"payload":{"a":1}}', 'payload', '{"a":1}'],
        'key first' => ['{"payload":1,"b":2}', 'payload', '1'],
        'key in the middle' => ['{"a":1,"payload":[1,2],"b":2}', 'payload', '[1,2]'],
        'key last' => ['{"a":1,"payload":"x"}', 'payload', '"x"'],
        'an integer too wide for php' => ['{"payload":12345678901234567890}', 'payload', '12345678901234567890'],
        'a float' => ['{"payload":-1.5e+3}', 'payload', '-1.5e+3'],
        'literals' => ['{"payload":null,"a":true}', 'payload', 'null'],
        'an empty object' => ['{"payload":{}}', 'payload', '{}'],
        'an empty list' => ['{"payload":[]}', 'payload', '[]'],
        'nested braces' => ['{"payload":{"a":{"b":[{"c":{}}]}},"z":1}', 'payload', '{"a":{"b":[{"c":{}}]}}'],
        'a value that spells the key' => ['{"a":"payload","payload":2}', 'payload', '2'],
        'a key-shaped string value' => ['{"a":"\"payload\":9","payload":2}', 'payload', '2'],
        'escaped quotes in the value' => ['{"payload":"x\"}{","z":1}', 'payload', '"x\"}{"'],
        'braces inside a nested string' => ['{"payload":{"s":"}]}\"["},"z":1}', 'payload', '{"s":"}]}\"["}'],
        'a trailing backslash escape' => ['{"payload":"c:\\\\","z":1}', 'payload', '"c:\\\\"'],
        'unicode in the value' => ['{"payload":{"s":"æ→😀"}}', 'payload', '{"s":"æ→😀"}'],
        'unicode in a key' => ['{"ærger":1,"payload":2}', 'payload', '2'],
        'an escaped key' => ['{"pa\u0079load":7}', 'payload', '7'],
        'whitespace everywhere' => ["{\n  \"a\" : 1 ,\n  \"payload\" : { \"b\" : 2 }\n}", 'payload', '{ "b" : 2 }'],
        'a duplicate key takes the last, like json_decode' => ['{"payload":1,"payload":2}', 'payload', '2'],
        'a missing key' => ['{"a":1}', 'payload', null],
        'a nested key is not top level' => ['{"a":{"payload":1}}', 'payload', null],
        'an empty object has no keys' => ['{}', 'payload', null],
        'a json list is not an object' => ['[1,2,3]', 'payload', null],
        'a json scalar is not an object' => ['42', 'payload', null],
        'the empty string' => ['', 'payload', null],
        'another key entirely' => ['{"a":1,"b":2}', 'b', '2'],
    ];
}
