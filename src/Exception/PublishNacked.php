<?php

declare(strict_types=1);

namespace Queuey\Exception;

/** The broker refused to take responsibility for a publish. */
final class PublishNacked extends BackendException {}
