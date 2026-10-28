<?php

declare(strict_types=1);

namespace Queuey;

use Queuey\Exception\InvalidConfiguration;
use Queuey\Internal\QueueSetRegistry;

/**
 * Implements {@see QueueSet} for an enum by reading `#[Queues]` on the enum and
 * `#[Queue]` on its cases.
 *
 * ```php
 * #[Queues(prefix: 'myapp')]
 * enum AppQueues implements QueueSet
 * {
 *     use QueueSetFromAttributes;
 *
 *     #[Queue(prefetch: 4)]
 *     case Emails;             // -> myapp.emails
 *     #[Queue(name: 'img')]
 *     case Images;             // -> myapp.img
 * }
 * ```
 *
 * A case with no `#[Queue]` gets every default and the `snake_case` form of its
 * name. Resolution runs once per enum and is cached in {@see QueueSetRegistry}.
 *
 * @phpstan-require-implements QueueSet
 */
trait QueueSetFromAttributes
{
    public function queueName(): string
    {
        return $this->config()->name;
    }

    public function config(): QueueConfig
    {
        $configs = QueueSetRegistry::configs(static::class);

        return $configs[$this->name] ?? throw new InvalidConfiguration(\sprintf(
            'enum %s has no resolved configuration for case %s',
            static::class,
            $this->name,
        ));
    }

    /**
     * @return list<static>
     */
    public static function all(): array
    {
        /** @var list<static> $cases */
        $cases = array_values(array_map(
            static fn(\ReflectionEnumUnitCase $case): object => $case->getValue(),
            (new \ReflectionEnum(static::class))->getCases(),
        ));

        return $cases;
    }

    public static function fromQueueName(string $name): ?static
    {
        foreach (static::all() as $case) {
            if ($case->queueName() === $name) {
                return $case;
            }
        }

        return null;
    }
}
