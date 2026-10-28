<?php

declare(strict_types=1);

use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

/*
|--------------------------------------------------------------------------
| Test Case bindings
|--------------------------------------------------------------------------
|
| The broker-backed suite runs on top of `RabbitMqTestCase`, so its helpers
| (the raw control channel, the unique queue prefixes, the queue cleanup and
| the open-channel count) stay ordinary methods on `$this`. That test case
| also carries the `AMQP_URL` gate in its `setUp()`: without the variable
| every test in the directory skips with a notice, exactly as it did under
| PHPUnit.
|
| Test bodies are closures, so PHPStan cannot see what `$this` is bound to.
| Every closure that reaches for a helper therefore opens with an inline
| `@var RabbitMqTestCase $this` docblock, which is what keeps `tests/` at
| level max.
|
*/

pest()->extend(RabbitMqTestCase::class)->in('Integration');
