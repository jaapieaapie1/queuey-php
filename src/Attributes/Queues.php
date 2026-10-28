<?php

declare(strict_types=1);

namespace Queuey\Attributes;

use Queuey\Exception\InvalidConfiguration;

/**
 * Container attribute for a queue-set enum: `#[Queues(prefix: 'myapp')]`.
 *
 * The prefix namespaces every queue in the set. The separator before the queue
 * name is added by the library, so `prefix: 'myapp'` already yields
 * `myapp.emails`; a prefix that ends in `.` is a configuration error.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final class Queues
{
    /** Separator this library puts between the prefix and the bare queue name. */
    public const SEPARATOR = '.';

    public function __construct(public readonly ?string $prefix = null)
    {
        if ($prefix === null) {
            return;
        }
        if ($prefix === '') {
            throw new InvalidConfiguration('`prefix` must not be empty');
        }
        if (trim($prefix) === '') {
            throw new InvalidConfiguration('`prefix` must not be only whitespace');
        }
        if (str_ends_with($prefix, self::SEPARATOR)) {
            throw new InvalidConfiguration(\sprintf(
                '`prefix` must not end with `%s`: the separator before the queue name is added for you, so `prefix: \'myapp\'` already yields `myapp.emails`',
                self::SEPARATOR,
            ));
        }
    }

    /** Apply the prefix to a bare queue name. */
    public function qualify(string $name): string
    {
        return $this->prefix === null ? $name : $this->prefix . self::SEPARATOR . $name;
    }
}
