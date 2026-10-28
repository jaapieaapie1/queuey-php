<?php

declare(strict_types=1);

namespace Queuey\Exception;

/**
 * A job was handed to a producer or worker whose queue set does not contain the
 * job's own queue.
 *
 * Rust rejects this when the crate is compiled; PHP can only notice it at the
 * moment the job is enqueued or dispatched.
 */
final class JobBelongsToOtherQueueSet extends InvalidConfiguration {}
