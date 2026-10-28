<?php

declare(strict_types=1);

namespace Queuey\Internal;

/**
 * The JSON codec every wire value goes through.
 *
 * Objects decode to `stdClass`, never to associative arrays: an empty JSON
 * object has to survive a worker re-publishing the payload it was handed, and
 * `[]` and `{}` are the same PHP array. See INTEROP-SPEC §1.
 *
 * @internal
 */
final class Json
{
    public const ENCODE_FLAGS = \JSON_THROW_ON_ERROR
        | \JSON_UNESCAPED_SLASHES
        | \JSON_UNESCAPED_UNICODE
        | \JSON_PRESERVE_ZERO_FRACTION;

    /**
     * @throws \JsonException
     */
    public static function encode(mixed $value): string
    {
        return json_encode($value, self::ENCODE_FLAGS);
    }

    /**
     * Decode object-faithfully: JSON objects become `stdClass`.
     *
     * @throws \JsonException
     */
    public static function decode(string $json): mixed
    {
        return json_decode($json, false, 512, \JSON_THROW_ON_ERROR);
    }

    /**
     * Put an arbitrary PHP value into the same shape a decode would have produced.
     *
     * An associative array becomes a `stdClass`, an empty array stays an empty
     * array (and therefore still encodes as `[]`), and anything with a
     * `JsonSerializable`/public-property representation is reduced to it once,
     * here, rather than at every re-encode.
     *
     * @throws \JsonException
     */
    public static function normalize(mixed $value): mixed
    {
        return self::decode(self::encode($value));
    }

    /**
     * The verbatim JSON text of the value of top-level object key `$key`.
     *
     * `json_decode` is lossy for integers a PHP `int` cannot hold: a Rust `u64`
     * payload field above `PHP_INT_MAX` comes back as a float and re-encodes as
     * `1.2345678901234567e+19`, a different value on the wire. A worker that
     * re-publishes the payload it was handed (INTEROP-SPEC §1) therefore splices
     * the original bytes back in rather than re-encoding them.
     *
     * `$json` is assumed to have been validated by {@see self::decode()} first;
     * the scanner walks well-formed JSON and returns `null` for anything else.
     * When a key occurs more than once the **last** value wins, as in
     * `json_decode`.
     *
     * @return string|null the raw value, or `null` when `$json` is not an object or has no such key
     */
    public static function rawTopLevelValue(string $json, string $key): ?string
    {
        $length = \strlen($json);
        $i = self::skipWhitespace($json, 0, $length);
        if ($i >= $length || $json[$i] !== '{') {
            return null;
        }
        ++$i;

        $found = null;
        while (true) {
            $i = self::skipWhitespace($json, $i, $length);
            if ($i >= $length || $json[$i] !== '"') {
                // `}` (end of object) or something this scanner does not parse.
                return $found;
            }

            $keyStart = $i;
            $i = self::skipString($json, $i, $length);
            $rawKey = substr($json, $keyStart, $i - $keyStart);

            $i = self::skipWhitespace($json, $i, $length);
            if ($i >= $length || $json[$i] !== ':') {
                return $found;
            }
            $i = self::skipWhitespace($json, $i + 1, $length);

            $valueStart = $i;
            $i = self::skipValue($json, $i, $length);
            if ($i === $valueStart) {
                return $found;
            }
            if (self::decodeStringLiteral($rawKey) === $key) {
                $found = substr($json, $valueStart, $i - $valueStart);
            }

            $i = self::skipWhitespace($json, $i, $length);
            if ($i >= $length || $json[$i] !== ',') {
                return $found;
            }
            ++$i;
        }
    }

    /** The same value with every `stdClass` turned into an associative array. */
    public static function toArray(mixed $value): mixed
    {
        if ($value instanceof \stdClass) {
            $value = get_object_vars($value);
        }

        if (\is_array($value)) {
            return array_map(self::toArray(...), $value);
        }

        return $value;
    }

    /** `$i` points at the opening quote; returns the index just past the closing one. */
    private static function skipString(string $json, int $i, int $length): int
    {
        ++$i;
        while ($i < $length) {
            $char = $json[$i];
            if ($char === '\\') {
                // An escape hides the next byte, `\"` included. Multi-byte UTF-8
                // never contains a bare `"` or `\`, so bytes are safe here.
                $i += 2;

                continue;
            }
            ++$i;
            if ($char === '"') {
                return $i;
            }
        }

        return $i;
    }

    /** Returns the index just past the value starting at `$i`. */
    private static function skipValue(string $json, int $i, int $length): int
    {
        if ($i >= $length) {
            return $i;
        }

        $char = $json[$i];
        if ($char === '"') {
            return self::skipString($json, $i, $length);
        }

        if ($char === '{' || $char === '[') {
            $depth = 0;
            while ($i < $length) {
                $char = $json[$i];
                if ($char === '"') {
                    $i = self::skipString($json, $i, $length);

                    continue;
                }
                if ($char === '{' || $char === '[') {
                    ++$depth;
                } elseif ($char === '}' || $char === ']') {
                    --$depth;
                    if ($depth === 0) {
                        return $i + 1;
                    }
                }
                ++$i;
            }

            return $i;
        }

        // A number, `true`, `false` or `null`: runs until the next structural byte.
        while ($i < $length && strpos(" \t\n\r,}]", $json[$i]) === false) {
            ++$i;
        }

        return $i;
    }

    private static function skipWhitespace(string $json, int $i, int $length): int
    {
        while ($i < $length && strpos(" \t\n\r", $json[$i]) !== false) {
            ++$i;
        }

        return $i;
    }

    /** `$raw` is a JSON string literal including its quotes; `null` when it is not. */
    private static function decodeStringLiteral(string $raw): ?string
    {
        $decoded = json_decode($raw);

        return \is_string($decoded) ? $decoded : null;
    }
}
