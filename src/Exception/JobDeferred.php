<?php

declare(strict_types=1);

namespace Queuey\Exception;

use Queuey\Duration;

/**
 * Not a failure: the job cannot run *yet* and must be delivered again in `$delay`.
 *
 * The attempt counter is untouched and the retry policy is never consulted, so a
 * job may defer itself indefinitely. `Envelope::deferrals` is incremented and the
 * envelope comes back at its queue's highest priority, ahead of the backlog.
 */
final class JobDeferred extends \RuntimeException implements QueueyException
{
    public function __construct(
        public readonly Duration $delay,
        public readonly string $reason = 'deferred',
    ) {
        parent::__construct(\sprintf('job deferred for %s: %s', (string) $delay, $reason));
    }

    public static function for(Duration $delay, ?string $reason = null): self
    {
        return new self($delay, $reason ?? 'deferred');
    }
}
