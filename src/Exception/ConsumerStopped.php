<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A consumer ended on its own, which means the backend went away. */
final class ConsumerStopped extends \RuntimeException implements QueueyException
{
    public static function forQueue(string $queue, ?\Throwable $previous = null): self
    {
        return new self(\sprintf('consumer for queue `%s` stopped unexpectedly', $queue), 0, $previous);
    }

    /**
     * Every consumer is gone at once, so the backend itself went away: a closed
     * or disconnected backend cannot deliver anything again, and a worker that
     * kept waiting on it would spin instead of stopping.
     */
    public static function backendGone(\Throwable $previous): self
    {
        return new self(\sprintf('the backend stopped delivering: %s', $previous->getMessage()), 0, $previous);
    }
}
