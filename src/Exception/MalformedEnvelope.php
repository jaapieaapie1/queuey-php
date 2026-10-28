<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A message body is not valid JSON, or not a valid envelope. */
final class MalformedEnvelope extends \RuntimeException implements QueueyException {}
