<?php

declare(strict_types=1);

namespace Queuey\RabbitMq;

use Queuey\Exception\InvalidConfiguration;

/**
 * A parsed `amqp://` or `amqps://` URL.
 *
 * Follows the AMQP URI spec: the vhost is whatever follows the first `/`,
 * percent-decoded, so `amqp://host/%2f` is the default vhost `/` and a URL with
 * no path at all means the same.
 *
 * The query string carries connection tunables, spelled and scaled exactly as
 * the Rust side's `lapin` reads them, so one `AMQP_URL` can configure both
 * applications: `heartbeat` in **seconds**, `connection_timeout` in
 * **milliseconds**. They are applied over the options passed to
 * {@see RabbitMqBackend::connect()}, which is what makes the URL the more
 * specific of the two. `channel_max` is parsed and ignored — this backend opens
 * the handful of channels it needs and never pools them — and anything else is
 * refused rather than silently dropped, because a misspelled `heartbeat` that
 * does nothing is the kind of thing that is discovered during an outage.
 */
final class AmqpUrl
{
    public const DEFAULT_PORT = 5672;
    public const DEFAULT_TLS_PORT = 5671;
    public const DEFAULT_VHOST = '/';

    /** Used only when the URL carries no userinfo at all. */
    public const DEFAULT_USER = 'guest';
    public const DEFAULT_PASSWORD = 'guest';

    public const PARAM_HEARTBEAT = 'heartbeat';
    public const PARAM_CONNECTION_TIMEOUT = 'connection_timeout';
    public const PARAM_CHANNEL_MAX = 'channel_max';

    /**
     * @param int|null   $heartbeat         `heartbeat` in seconds, or `null` when the URL is silent
     * @param float|null $connectionTimeout `connection_timeout`, converted from lapin's milliseconds to seconds
     * @param int|null   $channelMax        `channel_max`, parsed for the diagnostics and otherwise ignored
     */
    public function __construct(
        public readonly string $host,
        public readonly int $port,
        public readonly string $user,
        public readonly string $password,
        public readonly string $vhost,
        public readonly bool $secure,
        public readonly ?int $heartbeat = null,
        public readonly ?float $connectionTimeout = null,
        public readonly ?int $channelMax = null,
    ) {}

    /**
     * Parse `amqp[s]://[user[:pass]@]host[:port][/vhost][?params]`.
     *
     * @throws InvalidConfiguration when the scheme is not `amqp`/`amqps`, no host is given,
     *                              or a query parameter is unknown or not a number
     */
    public static function parse(string $url): self
    {
        $parts = parse_url($url);
        if ($parts === false || !isset($parts['scheme'])) {
            throw new InvalidConfiguration(\sprintf('`%s` is not a valid AMQP URL', $url));
        }

        $scheme = strtolower($parts['scheme']);
        $secure = match ($scheme) {
            'amqp' => false,
            'amqps' => true,
            default => throw new InvalidConfiguration(
                \sprintf('AMQP URL scheme must be `amqp` or `amqps`, got `%s`', $parts['scheme']),
            ),
        };

        $host = $parts['host'] ?? '';
        if ($host === '') {
            throw new InvalidConfiguration(\sprintf('AMQP URL `%s` names no host', $url));
        }

        // No path at all is the default vhost; a path is the vhost verbatim,
        // percent-decoded, so `/%2f` is `/` and a bare trailing slash is the
        // empty-named vhost.
        $path = $parts['path'] ?? null;
        $vhost = $path === null ? self::DEFAULT_VHOST : rawurldecode(substr($path, 1));

        // `guest`/`guest` stands in for "no credentials given at all". Once a
        // user is named, the password is whatever the URL says and nothing
        // else: `amqp://svc@broker/` must not quietly authenticate as
        // `svc`/`guest` and log in as somebody it was never told to be.
        $hasUserInfo = isset($parts['user']) || isset($parts['pass']);
        $user = isset($parts['user']) ? rawurldecode($parts['user']) : ($hasUserInfo ? '' : self::DEFAULT_USER);
        $password = isset($parts['pass']) ? rawurldecode($parts['pass']) : ($hasUserInfo ? '' : self::DEFAULT_PASSWORD);

        $params = self::parseQuery($url, $parts['query'] ?? null);

        return new self(
            host: $host,
            port: $parts['port'] ?? ($secure ? self::DEFAULT_TLS_PORT : self::DEFAULT_PORT),
            user: $user,
            password: $password,
            vhost: $vhost,
            secure: $secure,
            heartbeat: $params[self::PARAM_HEARTBEAT],
            connectionTimeout: $params[self::PARAM_CONNECTION_TIMEOUT] === null
                ? null
                : $params[self::PARAM_CONNECTION_TIMEOUT] / 1000,
            channelMax: $params[self::PARAM_CHANNEL_MAX],
        );
    }

    /**
     * `$options` with whatever this URL had to say about them.
     *
     * The URL wins: it is the per-deployment configuration, while the options
     * are the application's defaults compiled into it.
     *
     * @throws InvalidConfiguration when the URL's values cannot hold together with the rest
     *                              of `$options` (a heartbeat the read timeout cannot cover)
     */
    public function applyTo(RabbitMqOptions $options): RabbitMqOptions
    {
        try {
            if ($this->heartbeat !== null) {
                $options = $options->withHeartbeat($this->heartbeat);
            }
            if ($this->connectionTimeout !== null) {
                $options = $options->withConnectionTimeout($this->connectionTimeout);
            }
        } catch (InvalidConfiguration $error) {
            throw new InvalidConfiguration(
                \sprintf('the AMQP URL conflicts with the options it was passed with: %s', $error->getMessage()),
                0,
                $error,
            );
        }

        return $options;
    }

    /**
     * @return array{heartbeat: ?int, connection_timeout: ?int, channel_max: ?int}
     *
     * @throws InvalidConfiguration
     */
    private static function parseQuery(string $url, ?string $query): array
    {
        $heartbeat = null;
        $connectionTimeout = null;
        $channelMax = null;

        if ($query !== null && $query !== '') {
            $parsed = [];
            parse_str($query, $parsed);

            foreach ($parsed as $name => $value) {
                match ((string) $name) {
                    self::PARAM_HEARTBEAT => $heartbeat = self::intParam($url, self::PARAM_HEARTBEAT, $value),
                    self::PARAM_CONNECTION_TIMEOUT => $connectionTimeout = self::intParam(
                        $url,
                        self::PARAM_CONNECTION_TIMEOUT,
                        $value,
                    ),
                    self::PARAM_CHANNEL_MAX => $channelMax = self::intParam($url, self::PARAM_CHANNEL_MAX, $value),
                    default => throw new InvalidConfiguration(\sprintf(
                        'AMQP URL `%s` carries the unknown parameter `%s`; this backend understands '
                        . '`%s`, `%s` and `%s`',
                        $url,
                        (string) $name,
                        self::PARAM_HEARTBEAT,
                        self::PARAM_CONNECTION_TIMEOUT,
                        self::PARAM_CHANNEL_MAX,
                    )),
                };
            }
        }

        return [
            self::PARAM_HEARTBEAT => $heartbeat,
            self::PARAM_CONNECTION_TIMEOUT => $connectionTimeout,
            self::PARAM_CHANNEL_MAX => $channelMax,
        ];
    }

    /**
     * @throws InvalidConfiguration
     */
    private static function intParam(string $url, string $name, mixed $value): int
    {
        if (!\is_string($value) || preg_match('/^\d+$/', $value) !== 1) {
            throw new InvalidConfiguration(\sprintf(
                'the `%s` parameter of AMQP URL `%s` must be a non-negative integer, got `%s`',
                $name,
                $url,
                \is_string($value) ? $value : get_debug_type($value),
            ));
        }

        return (int) $value;
    }
}
