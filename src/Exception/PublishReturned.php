<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** The broker returned a mandatory publish: nobody had declared the queue. */
final class PublishReturned extends BackendException {}
