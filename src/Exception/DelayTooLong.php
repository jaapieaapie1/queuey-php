<?php

declare(strict_types=1);

namespace Queuey\Exception;

/**
 * A requested delay is longer than a hold queue can express.
 *
 * Nothing is ever clamped: releasing a job early is worse than refusing to
 * schedule it at all.
 */
final class DelayTooLong extends BackendException {}
