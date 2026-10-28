<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** The connection to the broker went away mid-operation. */
final class ConnectionLost extends BackendException {}
