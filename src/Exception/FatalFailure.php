<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** A permanent failure; the job is dead-lettered without consulting the policy. */
final class FatalFailure extends JobFailed {}
