<?php

declare(strict_types=1);

namespace Queuey\Internal;

use Queuey\Attributes\Queue;
use Queuey\Attributes\Queues;
use Queuey\Exception\InvalidConfiguration;
use Queuey\QueueConfig;

/**
 * Resolves and caches the `#[Queues]` / `#[Queue]` attributes of a queue-set enum.
 *
 * The cache lives here rather than in {@see \Queuey\QueueSetFromAttributes}
 * because PHP enums may not carry properties of any kind, static ones included.
 *
 * @internal
 */
final class QueueSetRegistry
{
    /**
     * Resolved configs by enum class, then by case name, in declaration order.
     *
     * @var array<class-string, array<string, QueueConfig>>
     */
    private static array $cache = [];

    /**
     * @param class-string<\UnitEnum> $enumClass
     *
     * @return array<string, QueueConfig>
     *
     * @throws InvalidConfiguration
     */
    public static function configs(string $enumClass): array
    {
        return self::$cache[$enumClass] ??= self::resolve($enumClass);
    }

    /** Drop everything resolved so far. For tests that define enums on the fly. */
    public static function reset(): void
    {
        self::$cache = [];
    }

    /**
     * Convert a case name to its default queue name: `SendEmails` becomes
     * `send_emails`, `HTTPCalls` becomes `http_calls`.
     */
    public static function snakeCase(string $identifier): string
    {
        // Rust's `to_snake_case` walks `char`s and uses the Unicode case
        // properties, so this walks UTF-8 characters rather than bytes. ASCII
        // identifiers come out exactly as they did before.
        $chars = mb_str_split($identifier, 1, 'UTF-8');
        $length = \count($chars);
        $out = '';

        for ($i = 0; $i < $length; ++$i) {
            $current = $chars[$i];

            if ($current === '_') {
                if ($out !== '' && !str_ends_with($out, '_')) {
                    $out .= '_';
                }

                continue;
            }

            if (self::isUpper($current)) {
                $previous = $i > 0 ? $chars[$i - 1] : null;
                $next = $i + 1 < $length ? $chars[$i + 1] : null;

                if ($previous !== null && $previous !== '_' && !str_ends_with($out, '_')) {
                    $boundary = self::isLower($previous) || self::isDigit($previous)
                        // `HTTPCalls`: split before the last capital of a run.
                        ? true
                        : ($next !== null && self::isLower($next));
                    if ($boundary) {
                        $out .= '_';
                    }
                }
            }

            $out .= mb_strtolower($current, 'UTF-8');
        }

        return $out;
    }

    /**
     * @param class-string<\UnitEnum> $enumClass
     *
     * @return array<string, QueueConfig>
     */
    private static function resolve(string $enumClass): array
    {
        $enum = new \ReflectionEnum($enumClass);
        $cases = $enum->getCases();
        if ($cases === []) {
            // Rust's derive refuses the same thing: a queue set with no variants
            // names no queue, so every `all()`/`declare` over it is a silent no-op.
            throw new InvalidConfiguration(\sprintf(
                'enum %s has no cases: a queue set requires at least one variant',
                $enumClass,
            ));
        }

        $container = self::containerAttribute($enum, $enumClass);
        $configs = [];
        $seen = [];
        foreach ($cases as $case) {
            $config = self::caseConfig($enumClass, $case, $container);
            if (isset($seen[$config->name])) {
                throw new InvalidConfiguration(\sprintf(
                    'enum %s maps both %s and %s to the queue `%s`',
                    $enumClass,
                    $seen[$config->name],
                    $case->getName(),
                    $config->name,
                ));
            }
            $seen[$config->name] = $case->getName();
            $configs[$case->getName()] = $config;
        }

        return $configs;
    }

    /**
     * @param \ReflectionEnum<\UnitEnum> $enum
     * @param class-string<\UnitEnum>    $enumClass
     */
    private static function containerAttribute(\ReflectionEnum $enum, string $enumClass): Queues
    {
        $attributes = $enum->getAttributes(Queues::class);
        if ($attributes === []) {
            return new Queues();
        }
        if (\count($attributes) > 1) {
            throw new InvalidConfiguration(\sprintf('enum %s carries more than one #[Queues]', $enumClass));
        }

        try {
            return $attributes[0]->newInstance();
        } catch (\Throwable $e) {
            throw new InvalidConfiguration(\sprintf('#[Queues] on enum %s: %s', $enumClass, $e->getMessage()), 0, $e);
        }
    }

    /**
     * @param class-string<\UnitEnum> $enumClass
     */
    private static function caseConfig(string $enumClass, \ReflectionEnumUnitCase $case, Queues $container): QueueConfig
    {
        $attributes = $case->getAttributes(Queue::class);
        if (\count($attributes) > 1) {
            throw new InvalidConfiguration(\sprintf('case %s::%s carries more than one #[Queue]', $enumClass, $case->getName()));
        }

        try {
            $attribute = $attributes === [] ? new Queue() : $attributes[0]->newInstance();
            $bare = $attribute->name ?? self::snakeCase($case->getName());
            if ($bare === '') {
                throw new InvalidConfiguration('queue name is empty; give the case a #[Queue(name: \'...\')]');
            }

            return $attribute->toConfig($container->qualify($bare));
        } catch (\Throwable $e) {
            throw new InvalidConfiguration(
                \sprintf('#[Queue] on %s::%s: %s', $enumClass, $case->getName(), $e->getMessage()),
                0,
                $e,
            );
        }
    }

    /** `IntlChar` is not guaranteed to be there; PCRE's Unicode properties are. */
    private static function isUpper(string $char): bool
    {
        return preg_match('/^\p{Lu}$/u', $char) === 1;
    }

    private static function isLower(string $char): bool
    {
        return preg_match('/^\p{Ll}$/u', $char) === 1;
    }

    private static function isDigit(string $char): bool
    {
        return preg_match('/^\p{Nd}$/u', $char) === 1;
    }
}
