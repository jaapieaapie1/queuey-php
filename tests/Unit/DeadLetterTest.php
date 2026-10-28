<?php

declare(strict_types=1);

use Queuey\DeadLetter;
use Queuey\DeadLetterCause;
use Queuey\Envelope;
use Queuey\FnDeadLetterHook;

it('knows which causes reached the handler', function (): void {
    expect(DeadLetterCause::NoHandler->reachedHandler())->toBeFalse();
    expect(DeadLetterCause::Decode->reachedHandler())->toBeFalse();
    expect(DeadLetterCause::Fatal->reachedHandler())->toBeTrue();
    expect(DeadLetterCause::Exhausted->reachedHandler())->toBeTrue();
});

it('carries the envelope and the attempt count', function (): void {
    $envelope = Envelope::raw('SendEmail', 'emails', new \stdClass())->nextAttempt();
    $dead = new DeadLetter($envelope, DeadLetterCause::Exhausted, 'smtp timeout', 3);

    expect($dead->envelope)->toBe($envelope);
    expect($dead->cause)->toBe(DeadLetterCause::Exhausted);
    expect($dead->reason)->toBe('smtp timeout');
    expect($dead->maxAttempts)->toBe(3);
    expect($dead->attempts())->toBe(2);
});

it('defaults maxAttempts to null for a wiring fault', function (): void {
    $dead = new DeadLetter(
        Envelope::raw('SendEmail', 'emails', new \stdClass()),
        DeadLetterCause::NoHandler,
        'no handler for job type `SendEmail`',
    );

    expect($dead->maxAttempts)->toBeNull();
    expect($dead->attempts())->toBe(1);
});

it('hands the dead letter to a closure hook', function (): void {
    $seen = [];
    $hook = FnDeadLetterHook::for(static function (DeadLetter $dead) use (&$seen): void {
        $seen[] = $dead->reason;
    });

    $hook->onDeadLetter(new DeadLetter(
        Envelope::raw('t', 'q', new \stdClass()),
        DeadLetterCause::Fatal,
        'fatal error: nope',
    ));

    expect($seen)->toBe(['fatal error: nope']);
});
