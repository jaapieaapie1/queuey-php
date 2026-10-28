<?php

declare(strict_types=1);

use Queuey\FnHandler;
use Queuey\JobContext;
use Queuey\Tests\Fixtures\Greet;
use Queuey\Tests\Fixtures\Ping;

it('reports the job class it was built with', function (): void {
    $handler = FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx): void {});

    expect($handler->jobClass())->toBe(Greet::class);
});

it('calls the closure with the job and the context', function (): void {
    $seen = [];
    $handler = FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx) use (&$seen): void {
        $seen[] = [$job->name, $ctx->attempt];
    });

    $handler->handle(
        new Greet('ada'),
        new JobContext('67e55044-10b1-426f-9247-bb680e5fe0c8', 'test::Greet', 'test.alpha', 2, 3),
    );

    expect($seen)->toBe([['ada', 2]]);
});

it('lets two handlers report their own job class', function (): void {
    $first = FnHandler::for(Greet::class, static function (Greet $job, JobContext $ctx): void {});
    $second = FnHandler::for(Ping::class, static function (Ping $job, JobContext $ctx): void {});

    expect($first->jobClass())->toBe(Greet::class);
    expect($second->jobClass())->toBe(Ping::class);
});
