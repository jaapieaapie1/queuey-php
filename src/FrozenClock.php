<?php

declare(strict_types=1);

namespace Queuey;

/** A clock that only moves when a test moves it. */
final class FrozenClock implements Clock
{
    public function __construct(private int $ms = 0) {}

    public function nowMs(): int
    {
        return $this->ms;
    }

    /** Move the clock forward. */
    public function advance(Duration $by): void
    {
        $this->ms += $by->toMs();
    }

    /** Move the clock to an exact instant. */
    public function setMs(int $ms): void
    {
        $this->ms = $ms;
    }
}
