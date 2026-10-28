<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use Queuey\Exception\BackendException;
use Queuey\Exception\PublishNacked;
use Queuey\Exception\PublishReturned;

/**
 * What the broker said about one publish, other than "ack".
 *
 * php-amqplib reports a `basic.return` and a `basic.nack` through listeners
 * registered on the channel rather than as a return value, so the outcome is
 * parked here for the publish that is waiting on its confirm. Exactly one
 * publish is in flight per channel, which is what makes that attribution sound.
 */
final class ConfirmOutcome
{
    private function __construct(
        private readonly bool $wasReturned,
        private readonly string $detail,
    ) {}

    /** The broker could not route a mandatory publish: nobody declared the queue. */
    public static function returned(string $reply, string $routingKey): self
    {
        return new self(true, \sprintf('%s (routing key `%s`)', $reply, $routingKey));
    }

    /** The broker refused to take responsibility for the message. */
    public static function nacked(): self
    {
        return new self(false, '');
    }

    public function toException(string $routingKey): BackendException
    {
        if ($this->wasReturned) {
            return new PublishReturned(\sprintf(
                'publish to `%s` was returned by the broker: %s (nobody has declared that queue)',
                $routingKey,
                $this->detail,
            ));
        }

        return new PublishNacked(
            \sprintf('the broker refused to take responsibility for a publish to `%s`', $routingKey),
        );
    }
}
