<?php

declare(strict_types=1);

namespace Queuey\Memory;

/**
 * Why a delivery attempt was acked.
 *
 * Three different outcomes all end in an ack on the wire: the job succeeded, a
 * retry was scheduled, or a deferral was held. The ack alone cannot tell them
 * apart, so {@see MemoryBackend} records the reason: that is what lets
 * {@see MemoryBackend::succeeded()} mean "the handler returned" while
 * {@see MemoryBackend::acked()} keeps meaning "settled by an ack".
 *
 * A dead letter is deliberately absent: it is not an ack, and the Rust backend
 * records none for it either. Dead letters show up in
 * {@see MemoryBackend::dead()} and in {@see MemoryBackend::settled()}.
 */
enum AckKind
{
    /** {@see \Queuey\Delivery::ack()} settled the attempt: the job is done. */
    case Succeeded;

    /** {@see \Queuey\Delivery::retry()} acked the original; a copy with `attempt + 1` follows. */
    case Retried;

    /** {@see \Queuey\Delivery::defer()} acked the original; a copy with the same `attempt` follows. */
    case Deferred;
}
