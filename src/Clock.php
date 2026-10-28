<?php

declare(strict_types=1);

namespace Queuey;

/** Source of "now", so everything that measures time is testable. */
interface Clock
{
    /** Unix epoch milliseconds. */
    public function nowMs(): int;
}
