//! The fixed interop protocol: the queue set, the job, and the handler.
//!
//! Everything in this module is mirrored **exactly** by the PHP half of the
//! harness. Nothing here is a knob: changing a queue name, a job name, a field
//! name or a handler branch breaks the cross-language tests, so the two sides
//! are edited together or not at all.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use queuey::prelude::*;
use serde_json::{Value, json};

use crate::emit;

/// The main work queue both languages declare.
pub const MAIN_QUEUE: &str = "interop.jobs";

/// Its dead-letter companion.
pub const DEAD_QUEUE: &str = "interop.jobs.dead";

/// One queue, `interop.jobs`, with the default `max_priority` (10) and the
/// default `durable = true`.
///
/// The retry policy is three attempts one second apart, fixed rather than
/// exponential so a PHP run and a Rust run take the same wall-clock time and a
/// test can assert on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "interop")]
pub enum InteropQueues {
    #[queue(prefetch = 4, retry(max_attempts = 3, backoff = "fixed", delay = "1s"))]
    Jobs,
}

/// The one job type on the wire.
///
/// `name = "interop::Echo"` is the point: Rust's default `job_type` would be
/// `queuey_interop::interop::Echo` and PHP's would be an FQCN, so the name is
/// stated explicitly on both sides (INTEROP-SPEC.md §7).
///
/// Every field is always present in the JSON — no `Option`, no
/// `skip_serializing_if` — so the payload a PHP producer writes and the payload
/// a Rust producer writes are the same five keys either way.
#[derive(Debug, Clone, Serialize, Deserialize, Job)]
#[job(queue = InteropQueues::Jobs, name = "interop::Echo")]
pub struct Echo {
    /// Sequence number within a `produce` run; how a test follows one job.
    pub seq: u32,
    /// Opaque text, carried through untouched.
    pub text: String,
    /// Fail retryably while `ctx.attempt <= fail_times`.
    pub fail_times: u32,
    /// Fail fatally on the first attempt.
    pub fatal: bool,
    /// Defer once, for this many milliseconds, when non-zero.
    pub defer_once_ms: u64,
}

/// Counts jobs that will never be seen again: successes plus dead letters.
///
/// `consume --expect N` stops once this reaches `N`, which is why both the
/// handler and the dead-letter hook bump it.
#[derive(Debug, Default)]
pub struct Terminals(AtomicUsize);

impl Terminals {
    /// One more job has reached a terminal state.
    pub fn bump(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// How many have so far.
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// The handler whose branching the PHP side reproduces line for line.
pub struct EchoHandler {
    /// Shared with the dead-letter hook in `main`.
    pub terminals: Arc<Terminals>,
}

#[async_trait]
impl JobHandler for EchoHandler {
    type Job = Echo;

    async fn handle(&self, job: Echo, ctx: JobContext) -> Result<(), JobError> {
        // The order of these three tests is part of the protocol: a job that is
        // both `fatal` and `defer_once_ms` dies rather than waiting.
        if job.fatal {
            emit(&attempt_event(&job, &ctx, "fatal"));
            return Err(JobError::fatal_msg("fatal by request"));
        }

        if job.defer_once_ms > 0 && ctx.deferrals == 0 {
            emit(&attempt_event(&job, &ctx, "deferred"));
            return Err(JobError::deferred_msg(
                Duration::from_millis(job.defer_once_ms),
                "deferred by request",
            ));
        }

        if ctx.attempt <= job.fail_times {
            emit(&attempt_event(&job, &ctx, "retryable"));
            return Err(JobError::retryable_msg("flaky by request"));
        }

        emit(&handled_event(&job, &ctx));
        self.terminals.bump();
        Ok(())
    }
}

/// `{"event":"attempt",...}`: one line per attempt that did not succeed.
fn attempt_event(job: &Echo, ctx: &JobContext, outcome: &str) -> Value {
    json!({
        "event": "attempt",
        "seq": job.seq,
        "attempt": ctx.attempt,
        "deferrals": ctx.deferrals,
        "outcome": outcome,
    })
}

/// `{"event":"handled",...}`: one line per job that finally ran.
fn handled_event(job: &Echo, ctx: &JobContext) -> Value {
    json!({
        "event": "handled",
        "seq": job.seq,
        "job_id": ctx.job_id.to_string(),
        "attempt": ctx.attempt,
        "max_attempts": ctx.max_attempts,
        "deferrals": ctx.deferrals,
        "priority": ctx.priority,
        "correlation_id": ctx.correlation_id,
        "age_ms": u64::try_from(ctx.age.as_millis()).unwrap_or(u64::MAX),
    })
}

/// `{"event":"dead",...}`: one line per job the worker gave up on, whatever the
/// cause — including the ones no handler ever saw.
pub fn dead_event(dead: &DeadLetter) -> Value {
    let cause = match dead.cause {
        DeadLetterCause::NoHandler => "NoHandler",
        DeadLetterCause::Decode => "Decode",
        DeadLetterCause::Fatal => "Fatal",
        DeadLetterCause::Exhausted => "Exhausted",
        // `DeadLetterCause` is `#[non_exhaustive]`; a cause added in a later
        // queuey release still produces a line rather than a panic.
        _ => "Unknown",
    };

    json!({
        "event": "dead",
        "job_id": dead.envelope.job_id.to_string(),
        "job_type": dead.envelope.job_type,
        "cause": cause,
        "reason": dead.reason,
        "attempts": dead.attempts(),
        "max_attempts": dead.max_attempts,
        "seq": seq_of(&dead.envelope.payload),
    })
}

/// The `seq` of a payload the worker may never have decoded (a `NoHandler` or
/// `Decode` dead letter still carries raw JSON), or `null`.
fn seq_of(payload: &Value) -> Value {
    payload
        .get("seq")
        .and_then(Value::as_u64)
        .and_then(|seq| u32::try_from(seq).ok())
        .map_or(Value::Null, Value::from)
}
