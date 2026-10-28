<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use PhpAmqpLib\Exception\AMQPBasicCancelException;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPHeartbeatMissedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPSocketException;

/** Classification of php-amqplib failures. */
final class AmqpError
{
    private function __construct() {}

    /**
     * Whether `$error` means "the connection is gone", as opposed to "the broker
     * said no".
     *
     * The distinction decides whether an operation reconnects and tries again or
     * surfaces: a channel-level error such as `PRECONDITION_FAILED` on a declare
     * leaves the connection perfectly healthy, and reconnecting over it would
     * just loop. `AMQPChannelClosedException` is here because php-amqplib raises
     * it when a *closed connection* is noticed through a channel.
     *
     * The heartbeat case is a subclass of the connection-closed one; it is named
     * explicitly because it is the one every operator asks about.
     */
    public static function isConnectionLost(\Throwable $error): bool
    {
        return $error instanceof AMQPHeartbeatMissedException
            || $error instanceof AMQPConnectionClosedException
            || $error instanceof AMQPChannelClosedException
            || $error instanceof AMQPIOException
            || $error instanceof AMQPSocketException;
    }

    /**
     * Whether `$error` means "this consumer is no longer subscribed", with the
     * connection itself perfectly healthy.
     *
     * Two shapes, both routine. The broker closed the channel over something it
     * refused on it (`AMQPProtocolChannelException`); php-amqplib has then
     * already nulled that channel's connection reference, so waiting on it
     * again raises a bare `\Error` rather than anything catchable. Or the broker
     * cancelled the consumer and left the channel open
     * (`AMQPBasicCancelException`), which is what a deleted queue looks like
     * from here: nothing fails, deliveries simply stop for good.
     *
     * Either way the answer is the same — resubscribe — and neither is a reason
     * to rebuild the connection.
     */
    public static function isConsumerGone(\Throwable $error): bool
    {
        return $error instanceof AMQPProtocolChannelException
            || $error instanceof AMQPBasicCancelException;
    }
}
