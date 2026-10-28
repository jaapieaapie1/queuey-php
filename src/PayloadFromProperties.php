<?php

declare(strict_types=1);

namespace Queuey;

use Queuey\Exception\DecodeError;

/**
 * Implements {@see Job} for a class whose public properties are its payload.
 *
 * `toPayload()` is an **object** of the public (including promoted readonly)
 * properties — an object, not an array, so a job with no properties encodes as
 * `{}` rather than `[]` (INTEROP-SPEC §1). `fromPayload()` matches the decoded
 * keys to constructor parameters by name.
 *
 * Good enough for scalar, array and nullable fields. A job with anything richer
 * writes the two methods by hand.
 *
 * @phpstan-require-implements Job
 */
trait PayloadFromProperties
{
    public function toPayload(): \stdClass
    {
        $values = [];
        foreach ((new \ReflectionObject($this))->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
            if ($property->isStatic() || !$property->isInitialized($this)) {
                continue;
            }
            $values[$property->getName()] = $property->getValue($this);
        }

        return (object) $values;
    }

    public static function fromPayload(mixed $payload): static
    {
        $fields = self::payloadFields($payload);
        $reflection = new \ReflectionClass(static::class);
        $constructor = $reflection->getConstructor();

        if ($constructor === null) {
            if ($fields !== []) {
                throw new DecodeError(\sprintf(
                    'decoding %s: unknown field `%s`; the class takes no constructor arguments',
                    static::class,
                    (string) array_key_first($fields),
                ));
            }

            return $reflection->newInstance();
        }

        $arguments = [];
        foreach ($constructor->getParameters() as $parameter) {
            $name = $parameter->getName();
            if (\array_key_exists($name, $fields)) {
                $arguments[$name] = $fields[$name];
                unset($fields[$name]);

                continue;
            }
            if ($parameter->isDefaultValueAvailable() || $parameter->isVariadic()) {
                continue;
            }
            // `allowsNull()` is also true for an untyped or `mixed` parameter,
            // where a missing key would silently become null instead of being
            // reported. Only an explicitly nullable type defaults.
            $type = $parameter->getType();
            if ($type !== null && $type->allowsNull() && !self::isMixedType($type)) {
                $arguments[$name] = null;

                continue;
            }

            throw new DecodeError(\sprintf('decoding %s: missing field `%s`', static::class, $name));
        }

        if ($fields !== []) {
            throw new DecodeError(\sprintf(
                'decoding %s: unknown field `%s`',
                static::class,
                (string) array_key_first($fields),
            ));
        }

        try {
            return $reflection->newInstanceArgs($arguments);
        } catch (\Throwable $e) {
            // A `TypeError` from a field of the wrong type, and anything the
            // job's own constructor rejects: from the outside both mean the
            // payload did not fit this class.
            throw new DecodeError(\sprintf('decoding %s: %s', static::class, $e->getMessage()), 0, $e);
        }
    }

    private static function isMixedType(\ReflectionType $type): bool
    {
        return $type instanceof \ReflectionNamedType && $type->getName() === 'mixed';
    }

    /**
     * @return array<string, mixed>
     */
    private static function payloadFields(mixed $payload): array
    {
        if ($payload === null) {
            return [];
        }
        if ($payload instanceof \stdClass) {
            $payload = get_object_vars($payload);
        }
        if (!\is_array($payload)) {
            throw new DecodeError(\sprintf(
                'decoding %s: payload must be a JSON object, got %s',
                static::class,
                get_debug_type($payload),
            ));
        }

        $fields = [];
        foreach ($payload as $key => $value) {
            if (!\is_string($key)) {
                throw new DecodeError(\sprintf(
                    'decoding %s: payload must be a JSON object, got a list',
                    static::class,
                ));
            }
            $fields[$key] = $value;
        }

        return $fields;
    }
}
