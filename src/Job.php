<?php

declare(strict_types=1);

namespace Queuey;

/**
 * A unit of work: a JSON-encodable payload bound to one queue by `#[OnQueue]`.
 *
 * ```php
 * #[OnQueue(AppQueues::Emails)]
 * final class SendEmail implements Job
 * {
 *     use PayloadFromProperties;
 *
 *     public function __construct(public readonly string $to) {}
 * }
 * ```
 *
 * {@see PayloadFromProperties} implements both methods for the common case of a
 * job whose constructor parameters are its payload.
 */
interface Job
{
    /**
     * The value that goes into `Envelope::payload`.
     *
     * Use an object (or an associative array) for struct-like jobs: an empty PHP
     * array encodes as `[]`, not `{}`, which a Rust worker will not decode into
     * a unit struct.
     */
    public function toPayload(): mixed;

    /**
     * Rebuild the job from a decoded payload.
     *
     * JSON objects arrive as associative arrays.
     *
     * @throws \Queuey\Exception\DecodeError when the payload does not fit this class
     */
    public static function fromPayload(mixed $payload): static;
}
