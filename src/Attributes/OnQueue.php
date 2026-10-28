<?php

declare(strict_types=1);

namespace Queuey\Attributes;

use Queuey\Exception\InvalidConfiguration;
use Queuey\QueueSet;

/**
 * Binds a job class to one queue: `#[OnQueue(AppQueues::Emails)]`.
 *
 * `name` is the `job_type` that travels on the wire and dispatches to a handler;
 * it defaults to the fully-qualified class name. Rust's default is
 * `module_path!()::Type`, so a job consumed by both languages must state the
 * name explicitly on at least one side. See INTEROP-SPEC §7.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final class OnQueue
{
    public function __construct(
        public readonly QueueSet $queue,
        public readonly ?string $name = null,
        public readonly ?Retry $retry = null,
    ) {
        if ($name !== null && trim($name) === '') {
            throw new InvalidConfiguration('`name` must not be empty or only whitespace');
        }
    }
}
