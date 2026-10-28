<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Queuey\Job;
use Queuey\PayloadFromProperties;

/** A job class that forgot its `#[OnQueue]`. */
final class Unattributed implements Job
{
    use PayloadFromProperties;
}
