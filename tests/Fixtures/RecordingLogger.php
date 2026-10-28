<?php

declare(strict_types=1);

namespace Queuey\Tests\Fixtures;

use Psr\Log\AbstractLogger;

/** A PSR-3 logger that keeps everything, so tests can assert on levels. */
final class RecordingLogger extends AbstractLogger
{
    /** @var list<array{level: string, message: string, context: array<mixed>}> */
    public array $records = [];

    /**
     * @param mixed        $level
     * @param array<mixed> $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = [
            'level' => \is_string($level) ? $level : get_debug_type($level),
            'message' => (string) $message,
            'context' => $context,
        ];
    }

    /**
     * Messages logged at `$level`, in order.
     *
     * @return list<string>
     */
    public function messages(string $level): array
    {
        return array_values(array_map(
            static fn(array $record): string => $record['message'],
            array_filter($this->records, static fn(array $record): bool => $record['level'] === $level),
        ));
    }

    /**
     * The context of the first record with `$message`, or `null`.
     *
     * @return array<mixed>|null
     */
    public function contextOf(string $message): ?array
    {
        foreach ($this->records as $record) {
            if ($record['message'] === $message) {
                return $record['context'];
            }
        }

        return null;
    }
}
