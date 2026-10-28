<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

/** What a {@see ReconnectPolicy} is being asked about. */
enum Rebuilding
{
    /** The connection itself went away and has to be dialled again. */
    case Connection;

    /** The connection is up but a consumer has to be subscribed again. */
    case Consumer;
}
