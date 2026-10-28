<?php

declare(strict_types=1);

namespace Queuey;

/**
 * Called by the worker for every job it dead-letters, for all four causes.
 *
 * The hook runs *before* the delivery is settled, so a crash in between means
 * the broker redelivers the job and the hook runs again: at-least-once, like the
 * rest of the system. Hooks must tolerate duplicate invocation, and a throwing
 * hook is logged and ignored — it is a notification, not a veto.
 */
interface DeadLetterHook
{
    public function onDeadLetter(DeadLetter $dead): void;
}
