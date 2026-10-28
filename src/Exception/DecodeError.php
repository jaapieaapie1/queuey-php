<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A payload did not fit the job class it was decoded into. */
final class DecodeError extends \RuntimeException implements QueueyException {}
