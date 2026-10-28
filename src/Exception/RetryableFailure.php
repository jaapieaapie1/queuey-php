<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A transient failure; the resolved retry policy decides what happens next. */
final class RetryableFailure extends JobFailed {}
