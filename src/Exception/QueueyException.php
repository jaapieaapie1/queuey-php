<?php

declare(strict_types=1);

namespace Queuey\Exception;

/**
 * Marker implemented by every exception this library throws on its own behalf.
 *
 * Catching `QueueyException` catches configuration problems, envelope and
 * payload decoding problems, worker wiring problems and broker problems, and
 * nothing else.
 */
interface QueueyException extends \Throwable {}
