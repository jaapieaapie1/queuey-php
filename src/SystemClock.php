<?php

declare(strict_types=1);

namespace Queuey;

/** The wall clock. */
final class SystemClock implements Clock
{
    public function nowMs(): int
    {
        return (int) (microtime(true) * 1000);
    }
}
