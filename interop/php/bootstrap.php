<?php

declare(strict_types=1);

/**
 * Autoloading for the PHP half of the interop harness.
 *
 * The harness is not part of the package: it is a consumer of it, and the two
 * classes it needs (`InteropQueues`, `EchoJob`) live under `Queuey\Interop\`
 * rather than `Queuey\`. Wiring them into the package's own composer autoload
 * would ship test scaffolding to every user of the library, so the prefix gets
 * a four-line loader of its own instead.
 */

$vendor = \dirname(__DIR__, 2) . '/vendor/autoload.php';
if (!is_file($vendor)) {
    fwrite(STDERR, "queuey-interop: run `composer install` in the repository root first\n");
    exit(1);
}
require_once $vendor;

spl_autoload_register(static function (string $class): void {
    $prefix = 'Queuey\\Interop\\';
    if (!str_starts_with($class, $prefix)) {
        return;
    }

    $file = __DIR__ . '/src/' . str_replace('\\', '/', substr($class, \strlen($prefix))) . '.php';
    if (is_file($file)) {
        require_once $file;
    }
});
