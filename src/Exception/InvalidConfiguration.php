<?php

declare(strict_types=1);

namespace Queuey\Exception;

/**
 * A queue, job, attribute or policy was configured with values that cannot work.
 *
 * This is PHP's stand-in for the compile-time errors the Rust derive macros
 * produce, so the message always names the offending class, enum case or key.
 */
class InvalidConfiguration extends \InvalidArgumentException implements QueueyException {}
