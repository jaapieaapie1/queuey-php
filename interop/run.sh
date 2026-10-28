#!/usr/bin/env bash
#
# Cross-language interop harness for queuey (Rust) and queuey-php.
#
# Runs every scenario in both directions — a Rust producer feeding a PHP worker,
# and a PHP producer feeding a Rust worker — against a live RabbitMQ, and asserts
# on the JSON event lines both binaries emit. A scenario that behaves differently
# depending on which language wrote the message is a wire-compatibility bug.
#
#   AMQP_URL=amqp://guest:guest@localhost:5672/%2f interop/run.sh
#   interop/run.sh --only retry --direction php2rust
#   QUEUEY_INTEROP_KEEP=1 interop/run.sh        # leave the captured output behind
#
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2f}"
# Both binaries log to stderr; the harness only reads stdout, but a quiet run is
# easier to read when a scenario fails.
export RUST_LOG="${RUST_LOG:-warn}"
export QUEUEY_LOG="${QUEUEY_LOG:-warn}"

rust="$root/interop/rust/target/release/queuey-interop"
php="$root/interop/php/queuey-interop"

only=""
direction_filter=""

while [ $# -gt 0 ]; do
    case "$1" in
        --only)
            only="${2:-}"
            shift 2
            ;;
        --only=*)
            only="${1#--only=}"
            shift
            ;;
        --direction)
            direction_filter="${2:-}"
            shift 2
            ;;
        --direction=*)
            direction_filter="${1#--direction=}"
            shift
            ;;
        -h | --help)
            sed -n '3,12p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            echo "run.sh: unknown argument \`$1\`" >&2
            exit 2
            ;;
    esac
done

case "$direction_filter" in
    "" | php2rust | rust2php) ;;
    *)
        echo "run.sh: --direction must be php2rust or rust2php, got \`$direction_filter\`" >&2
        exit 2
        ;;
esac

# --- prerequisites ---------------------------------------------------------

if [ ! -x "$rust" ]; then
    echo "building the Rust half (cargo build --release)..." >&2
    (cd "$root/interop/rust" && cargo build --release >&2)
fi
if [ ! -x "$rust" ]; then
    echo "run.sh: $rust is missing even after a build" >&2
    exit 1
fi
if [ ! -x "$php" ]; then
    echo "run.sh: $php is missing or not executable" >&2
    exit 1
fi

if command -v jq > /dev/null 2>&1 && [ "${QUEUEY_INTEROP_NO_JQ:-0}" != "1" ]; then
    HAVE_JQ=1
else
    HAVE_JQ=0
    echo "note: jq not found; falling back to php for the JSON assertions" >&2
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/queuey-interop.XXXXXX")"
cleanup() {
    if [ "${QUEUEY_INTEROP_KEEP:-0}" = "1" ]; then
        echo "output kept in $tmp" >&2
    else
        rm -rf "$tmp"
    fi
}
trap cleanup EXIT

# --- JSON assertions -------------------------------------------------------

# pick FILE EVENT FIELD [SUBFIELD]
#
# The first object on FILE whose `.event` is EVENT, printed as a bare scalar:
# a string without its quotes, a number, `true`/`false`, or `null` when the
# object, the field or the subfield is not there.
pick() {
    if [ "$HAVE_JQ" -eq 1 ]; then
        jq -rs --arg e "$2" --arg f "$3" --arg s "${4:-}" '
            [ .[] | select(.event == $e) ][0] as $o
            | (if $o == null then null
               elif $s == "" then $o[$f]
               else (($o[$f] // {})[$s]) end)
            | if . == null then "null" else tostring end
        ' "$1"
    else
        php -r '
            $lines = file($argv[1], FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];
            foreach ($lines as $line) {
                $o = json_decode($line, true);
                if (!is_array($o) || ($o["event"] ?? null) !== $argv[2]) { continue; }
                $v = $o[$argv[3]] ?? null;
                if (($argv[4] ?? "") !== "") { $v = is_array($v) ? ($v[$argv[4]] ?? null) : null; }
                echo $v === null ? "null" : (is_bool($v) ? ($v ? "true" : "false") : (string) $v), "\n";
                exit(0);
            }
            echo "null\n";
        ' -- "$1" "$2" "$3" "${4:-}"
    fi
}

# count_of FILE EVENT — how many objects on FILE carry that `.event`.
count_of() {
    if [ "$HAVE_JQ" -eq 1 ]; then
        jq -rs --arg e "$2" '[ .[] | select(.event == $e) ] | length' "$1"
    else
        php -r '
            $lines = file($argv[1], FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];
            $n = 0;
            foreach ($lines as $line) {
                $o = json_decode($line, true);
                if (is_array($o) && ($o["event"] ?? null) === $argv[2]) { $n++; }
            }
            echo $n, "\n";
        ' -- "$1" "$2"
    fi
}

# --- scenario plumbing -----------------------------------------------------

total_pass=0
total_fail=0
case_fails=0
case_notes=""

bin_for() { [ "$1" = "php" ] && echo "$php" || echo "$rust"; }
producer_lang() { [ "$1" = "php2rust" ] && echo "php" || echo "rust"; }
consumer_lang() { [ "$1" = "php2rust" ] && echo "rust" || echo "php"; }

# check DESCRIPTION ACTUAL EXPECTED
check() {
    if [ "$2" != "$3" ]; then
        case_fails=$((case_fails + 1))
        case_notes="${case_notes}      - $1: expected \`$3\`, got \`$2\`"$'\n'
    fi
}

# check_rc DESCRIPTION ACTUAL_CODE EXPECTED_CODE
check_rc() { check "$1 exit code" "$2" "$3"; }

C_OUT=""
C_ERR=""
P_OUT=""
D_OUT=""
consumer_pid=""
consumer_rc=0
producer_rc=0

# run_case ID DIRECTION EXPECT TIMEOUT -- PRODUCER ARGS...
#
# Purges through both binaries, starts the consumer in the background, gives it a
# second to subscribe, runs the producer, then waits for the consumer to stop on
# its own (it shuts down as soon as `--expect` terminal events have arrived).
run_case() {
    local id="$1" dir="$2" expect="$3" timeout="$4"
    shift 4

    local producer consumer
    producer="$(bin_for "$(producer_lang "$dir")")"
    consumer="$(bin_for "$(consumer_lang "$dir")")"

    case_fails=0
    case_notes=""

    local stem="$tmp/$id.$dir"
    C_OUT="$stem.consumer.jsonl"
    C_ERR="$stem.consumer.err"
    P_OUT="$stem.producer.jsonl"
    D_OUT="$stem.drain.jsonl"

    # Both sides purge: the scenario starts from a broker that has never heard of
    # either queue, whichever library declared them last time.
    "$producer" purge > "$stem.purge.producer.jsonl" 2> "$stem.purge.err" || true
    "$consumer" purge > "$stem.purge.consumer.jsonl" 2>> "$stem.purge.err" || true

    "$consumer" consume --expect "$expect" --timeout-secs "$timeout" > "$C_OUT" 2> "$C_ERR" &
    consumer_pid=$!
    sleep 1

    producer_rc=0
    "$producer" produce "$@" > "$P_OUT" 2> "$stem.producer.err" || producer_rc=$?

    # The consumer bounds itself with --timeout-secs; this guard only catches a
    # process that has stopped bounding itself at all.
    local guard=$((timeout + 20)) waited=0
    while kill -0 "$consumer_pid" 2> /dev/null && [ "$waited" -lt "$guard" ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if kill -0 "$consumer_pid" 2> /dev/null; then
        kill -TERM "$consumer_pid" 2> /dev/null || true
        case_fails=$((case_fails + 1))
        case_notes="${case_notes}      - consumer had to be killed after ${guard}s"$'\n'
    fi

    consumer_rc=0
    wait "$consumer_pid" || consumer_rc=$?

    check_rc "producer" "$producer_rc" 0
    check_rc "consumer" "$consumer_rc" 0
}

# drain ID DIRECTION EXPECT — drain the dead queue with the *other* language's
# binary, so the AMQP shape one side wrote is always reported by the other.
drain() {
    local id="$1" dir="$2" expect="$3"
    local drainer rc=0
    drainer="$(bin_for "$(producer_lang "$dir")")"
    "$drainer" drain-dead --expect "$expect" --timeout-secs 20 > "$D_OUT" 2> "$tmp/$id.$dir.drain.err" || rc=$?
    check_rc "drain-dead" "$rc" 0
}

# finish ID DIRECTION DESCRIPTION
finish() {
    if [ "$case_fails" -eq 0 ]; then
        total_pass=$((total_pass + 1))
        printf 'PASS  %-12s %-9s %s\n' "$1" "$2" "$3"
    else
        total_fail=$((total_fail + 1))
        printf 'FAIL  %-12s %-9s %s\n' "$1" "$2" "$3"
        printf '%s' "$case_notes"
        printf '      consumer stdout: %s\n' "$C_OUT"
        sed -n '1,8p' "$C_ERR" | cut -c 1-180 | sed 's/^/      stderr| /'
    fi
}

selected() {
    if [ -n "$only" ] && [ "$only" != "$1" ] && [ "$only" != "$2" ]; then
        return 1
    fi
    return 0
}

# --- the scenarios ---------------------------------------------------------

scenario() {
    local id="$1" dir="$2"

    case "$id" in
        a)
            run_case "$id" "$dir" 3 40 --count 3 --start-seq 200
            check "handled events" "$(count_of "$C_OUT" handled)" 3
            check "dead events" "$(count_of "$C_OUT" dead)" 0
            check "enqueued events" "$(count_of "$P_OUT" enqueued)" 3
            check "first attempt" "$(pick "$C_OUT" handled attempt)" 1
            check "max_attempts" "$(pick "$C_OUT" handled max_attempts)" 3
            finish "$id/success" "$dir" 'three jobs enqueued and handled'
            ;;
        b)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 300 --fail-times 2
            check "attempt events" "$(count_of "$C_OUT" attempt)" 2
            check "first attempt outcome" "$(pick "$C_OUT" attempt outcome)" retryable
            check "handled events" "$(count_of "$C_OUT" handled)" 1
            check "handled on attempt" "$(pick "$C_OUT" handled attempt)" 3
            check "handled seq" "$(pick "$C_OUT" handled seq)" 300
            check "dead events" "$(count_of "$C_OUT" dead)" 0
            finish "$id/retry" "$dir" 'two retries then success on attempt 3'
            ;;
        c)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 400 --fail-times 5
            check "attempt events" "$(count_of "$C_OUT" attempt)" 3
            check "handled events" "$(count_of "$C_OUT" handled)" 0
            check "dead cause" "$(pick "$C_OUT" dead cause)" Exhausted
            check "dead reason" "$(pick "$C_OUT" dead reason)" \
                'max attempts (3) exhausted after attempt 3: flaky by request'
            check "dead attempts" "$(pick "$C_OUT" dead attempts)" 3
            check "dead max_attempts" "$(pick "$C_OUT" dead max_attempts)" 3
            check "dead seq" "$(pick "$C_OUT" dead seq)" 400
            drain "$id" "$dir" 1
            check "dead_message type" "$(pick "$D_OUT" dead_message type)" 'interop::Echo'
            check "dead_message content_type" "$(pick "$D_OUT" dead_message content_type)" application/json
            check "dead_message delivery_mode" "$(pick "$D_OUT" dead_message delivery_mode)" 2
            check "header x-death-reason" "$(pick "$D_OUT" dead_message headers x-death-reason)" \
                'max attempts (3) exhausted after attempt 3: flaky by request'
            check "header x-original-queue" "$(pick "$D_OUT" dead_message headers x-original-queue)" interop.jobs
            check "header x-attempts" "$(pick "$D_OUT" dead_message headers x-attempts)" 3
            check "header x-attempt" "$(pick "$D_OUT" dead_message headers x-attempt)" 3
            check "header x-deferrals" "$(pick "$D_OUT" dead_message headers x-deferrals)" 0
            finish "$id/exhausted" "$dir" 'retries exhausted, dead-lettered, AMQP shape checked'
            ;;
        d)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 500 --fatal
            check "attempt events" "$(count_of "$C_OUT" attempt)" 1
            check "attempt outcome" "$(pick "$C_OUT" attempt outcome)" fatal
            check "handled events" "$(count_of "$C_OUT" handled)" 0
            check "dead cause" "$(pick "$C_OUT" dead cause)" Fatal
            check "dead reason" "$(pick "$C_OUT" dead reason)" 'fatal error: fatal by request'
            check "dead attempts" "$(pick "$C_OUT" dead attempts)" 1
            check "dead job_type" "$(pick "$C_OUT" dead job_type)" 'interop::Echo'
            check "dead seq" "$(pick "$C_OUT" dead seq)" 500
            finish "$id/fatal" "$dir" 'fatal failure dead-lettered on the first attempt'
            ;;
        e)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 600 --defer-once-ms 500
            check "attempt events" "$(count_of "$C_OUT" attempt)" 1
            check "attempt outcome" "$(pick "$C_OUT" attempt outcome)" deferred
            check "handled events" "$(count_of "$C_OUT" handled)" 1
            check "handled attempt" "$(pick "$C_OUT" handled attempt)" 1
            check "handled deferrals" "$(pick "$C_OUT" handled deferrals)" 1
            check "handled priority" "$(pick "$C_OUT" handled priority)" 10
            finish "$id/deferral" "$dir" 'handler deferral returns at the front, attempt unchanged'
            ;;
        f)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 700 --after-ms 1200
            check "handled events" "$(count_of "$C_OUT" handled)" 1
            check "handled attempt" "$(pick "$C_OUT" handled attempt)" 1
            check "handled deferrals" "$(pick "$C_OUT" handled deferrals)" 0
            check "handled priority" "$(pick "$C_OUT" handled priority)" 0
            finish "$id/after" "$dir" 'enqueue_after lands behind the backlog at priority 0'
            ;;
        g)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 800 --defer-ms 1200
            check "handled events" "$(count_of "$C_OUT" handled)" 1
            check "handled attempt" "$(pick "$C_OUT" handled attempt)" 1
            check "handled deferrals" "$(pick "$C_OUT" handled deferrals)" 0
            check "handled priority" "$(pick "$C_OUT" handled priority)" 10
            finish "$id/defer" "$dir" 'producer defer lands at the front, deferrals still 0'
            ;;
        h)
            run_case "$id" "$dir" 1 40 --count 1 --start-seq 900 --correlation-id trace-abc
            check "handled events" "$(count_of "$C_OUT" handled)" 1
            check "correlation_id" "$(pick "$C_OUT" handled correlation_id)" trace-abc
            check "handled seq" "$(pick "$C_OUT" handled seq)" 900
            finish "$id/correlation" "$dir" 'correlation id round-trips to the handler'
            ;;
        *)
            echo "run.sh: no scenario \`$id\`" >&2
            exit 2
            ;;
    esac
}

names_a="success"
names_b="retry"
names_c="exhausted"
names_d="fatal"
names_e="deferral"
names_f="after"
names_g="defer"
names_h="correlation"

echo "queuey interop harness"
echo "  broker:   $AMQP_URL"
echo "  rust:     $rust"
echo "  php:      $php"
echo "  output:   $tmp"
echo

matched=0
for id in a b c d e f g h; do
    eval "name=\$names_$id"
    selected "$id" "$name" || continue
    for dir in php2rust rust2php; do
        if [ -n "$direction_filter" ] && [ "$direction_filter" != "$dir" ]; then
            continue
        fi
        matched=$((matched + 1))
        scenario "$id" "$dir"
    done
done

if [ "$matched" -eq 0 ]; then
    echo "run.sh: --only \`$only\` matched no scenario" >&2
    exit 2
fi

echo
echo "summary: $total_pass passed, $total_fail failed, $((total_pass + total_fail)) run"
[ "$total_fail" -eq 0 ] || exit 1
