<?php

declare(strict_types=1);

namespace Queuey;

/** The shape of a {@see Backoff} curve. */
enum BackoffKind
{
    /** Retry immediately. */
    case None;
    /** A constant delay between attempts. */
    case Fixed;
    /** `min(max, base * factor^(attempt - 1))`, optionally with full jitter. */
    case Exponential;
}
