<?php

declare(strict_types=1);

use Queuey\Tests\Integration\RabbitMq\RabbitMqTestCase;

/*
 * Placeholder for the broker-backed suite: everything here needs `AMQP_URL`.
 *
 * Keeping one test in the suite means `composer test` reports the skip rather
 * than silently running nothing.
 */

it('reaches the broker', function (): void {
    /** @var RabbitMqTestCase $this */
    expect($this->url)->toStartWith('amqp');
});
