<?php

declare(strict_types=1);

namespace Queuey;

/** Why a job was dead-lettered. */
enum DeadLetterCause
{
    /** No handler was registered for the envelope's `job_type`; no user code ran. */
    case NoHandler;
    /** The payload did not decode into the handler's job class; nothing ran. */
    case Decode;
    /** The handler threw `FatalFailure`, so the retry policy was never consulted. */
    case Fatal;
    /** The handler failed on its last allowed attempt and the policy gave up. */
    case Exhausted;

    /**
     * Whether user code ran at all.
     *
     * `false` for {@see self::NoHandler} and {@see self::Decode}, which are
     * wiring faults rather than job failures.
     */
    public function reachedHandler(): bool
    {
        return $this === self::Fatal || $this === self::Exhausted;
    }
}
