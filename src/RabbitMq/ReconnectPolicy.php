<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use Queuey\Duration;

/**
 * How hard the backend tries to get its connection — or a consumer — back.
 *
 * This is about *pacing* only: whether to try again, and how long to wait
 * first. What actually gets rebuilt is {@see Connection}'s business.
 */
interface ReconnectPolicy
{
    /**
     * How long to wait before `$attempt`, or `null` to give up.
     *
     * Asked before every attempt, the first included, so a policy controls
     * whether to start at all. Returning {@see Duration::zero()} for the first
     * attempt is usually right: a failover often completes in the time it took
     * to notice the drop.
     */
    public function nextDelay(Attempt $attempt): ?Duration;
}
