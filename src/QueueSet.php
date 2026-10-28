<?php

declare(strict_types=1);

namespace Queuey;

/**
 * A closed set of queues, normally a backed or pure enum using
 * {@see QueueSetFromAttributes}.
 *
 * Extending `UnitEnum` is deliberate: the set has to be enumerable and its
 * members have to be singletons that can be compared with `===` and written in
 * an attribute argument, which is exactly what an enum is. Implementing this
 * on a plain class is not possible, and does not need to be.
 */
interface QueueSet extends \UnitEnum
{
    /** Fully qualified broker queue name, prefix applied. */
    public function queueName(): string;

    /** Full configuration of this queue. */
    public function config(): QueueConfig;

    /**
     * Every queue in the set, in declaration order.
     *
     * @return list<static>
     */
    public static function all(): array;

    /** Look up a queue by its broker name. */
    public static function fromQueueName(string $name): ?static;
}
