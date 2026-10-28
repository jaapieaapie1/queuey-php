<?php

declare(strict_types=1);

namespace Queuey;

/** A {@see DeadLetterHook} made from any callable. */
final class FnDeadLetterHook implements DeadLetterHook
{
    /** @var \Closure(DeadLetter): void */
    private readonly \Closure $fn;

    /**
     * @param callable(DeadLetter): void $fn
     */
    public function __construct(callable $fn)
    {
        $this->fn = $fn(...);
    }

    /**
     * @param callable(DeadLetter): void $fn
     */
    public static function for(callable $fn): self
    {
        return new self($fn);
    }

    public function onDeadLetter(DeadLetter $dead): void
    {
        ($this->fn)($dead);
    }
}
