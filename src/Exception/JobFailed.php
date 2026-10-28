<?php

declare(strict_types=1);

namespace Queuey\Exception;

/**
 * Base class of the two failure outcomes a handler signals on purpose.
 *
 * Anything else escaping `JobHandler::handle()` is treated as a
 * {@see RetryableFailure} carrying {@see JobFailed::describe()} of the throwable.
 */
abstract class JobFailed extends \RuntimeException implements QueueyException
{
    final public function __construct(string $message = '', ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    /** This failure, with a plain message. */
    public static function withMessage(string $message): static
    {
        return new static($message);
    }

    /** This failure, caused by `$cause`; the message is `Class: message`. */
    public static function because(\Throwable $cause): static
    {
        return new static(self::describe($cause), $cause);
    }

    /**
     * How an unexpected throwable is rendered in a dead-letter reason: `Class: message`.
     *
     * The wire contract pins this format, so the worker and this class share it.
     */
    public static function describe(\Throwable $throwable): string
    {
        return \sprintf('%s: %s', $throwable::class, $throwable->getMessage());
    }
}
