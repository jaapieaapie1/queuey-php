<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A delivery was acked, retried, deferred or dead-lettered twice. */
final class AlreadySettled extends BackendException {}
