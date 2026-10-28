<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** The transport failed. Broker-specific errors are wrapped in a subclass of this. */
class BackendException extends \RuntimeException implements QueueyException
{
    final public function __construct(string $message = '', int $code = 0, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }

    /** Wrap a transport error. */
    public static function because(\Throwable $cause): static
    {
        return new static(\sprintf('backend error: %s', $cause->getMessage()), 0, $cause);
    }
}
