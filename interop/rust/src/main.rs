//! `queuey-interop`: the Rust half of the queuey / queuey-php interop harness.
//!
//! One binary, five subcommands, one broker. Two of them (`produce`, `consume`)
//! go through the queuey API and are the thing under test; three (`purge`,
//! `drain-dead`, `inspect`) go straight to RabbitMQ through `lapin` and are the
//! harness's way of checking what was actually left on the wire.
//!
//! **stdout carries JSON lines and nothing else.** One object per line, no
//! pretty printing, no banner, no trailing summary, so a test on either side of
//! the language boundary can read it with a line-by-line JSON parser. Every
//! diagnostic — tracing, error text, timeout messages — goes to stderr.
//!
//! The broker comes from `AMQP_URL`, defaulting to
//! `amqp://guest:guest@localhost:5672/%2f`. Log level comes from `RUST_LOG`,
//! defaulting to `info`.

mod interop;
mod raw;

use std::{error::Error, io::Write, process::ExitCode, sync::Arc, time::Duration};

use clap::{Args, Parser, Subcommand};
use queuey::prelude::*;
use serde_json::{Value, json};

use crate::interop::{Echo, EchoHandler, InteropQueues, Terminals, dead_event};

/// Broker to talk to when `AMQP_URL` is not set.
const DEFAULT_URL: &str = "amqp://guest:guest@localhost:5672/%2f";

/// How often the `consume` loop checks whether it is done.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Parser)]
#[command(
    name = "queuey-interop",
    about = "Rust half of the queuey / queuey-php interop harness",
    long_about = "Produces and consumes `interop::Echo` jobs on `interop.jobs` through queuey, \
                  and inspects the broker directly through lapin. stdout is JSON lines only; \
                  logging goes to stderr. Broker from $AMQP_URL."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Declare the topology and enqueue N `interop::Echo` jobs.
    Produce(ProduceArgs),
    /// Run a worker until N jobs have reached a terminal state.
    Consume(ConsumeArgs),
    /// Delete both interop queues, ignoring the ones that are not there.
    Purge,
    /// Consume and ack N messages off `interop.jobs.dead`, reporting their AMQP shape.
    DrainDead(DrainDeadArgs),
    /// Report the depth and consumer count of both interop queues.
    Inspect,
}

#[derive(Debug, Args)]
struct ProduceArgs {
    /// How many jobs to enqueue.
    #[arg(long)]
    count: u32,
    /// `seq` of the first job; the rest follow it.
    #[arg(long, default_value_t = 0)]
    start_seq: u32,
    /// The `text` field, the same on every job.
    #[arg(long, default_value = "hello")]
    text: String,
    /// The `fail_times` field: fail retryably while `attempt <= fail_times`.
    #[arg(long, default_value_t = 0)]
    fail_times: u32,
    /// The `fatal` field: fail fatally on the first attempt.
    #[arg(long)]
    fatal: bool,
    /// The `defer_once_ms` field: defer once for this long, when non-zero.
    #[arg(long, default_value_t = 0)]
    defer_once_ms: u64,
    /// Tag every envelope with this correlation id.
    #[arg(long)]
    correlation_id: Option<String>,
    /// Enqueue with a plain delay: back of the queue when it lands.
    #[arg(long, conflicts_with = "defer_ms")]
    after_ms: Option<u64>,
    /// Enqueue deferred: front of the queue when it lands.
    #[arg(long)]
    defer_ms: Option<u64>,
}

#[derive(Debug, Args)]
struct ConsumeArgs {
    /// Terminal events (handled + dead) to wait for before shutting down.
    #[arg(long)]
    expect: usize,
    /// Give up, with exit code 1, after this long.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
}

#[derive(Debug, Args)]
struct DrainDeadArgs {
    /// Messages to consume and ack off the dead queue.
    #[arg(long)]
    expect: usize,
    /// Give up, with exit code 1, after this long.
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    match run(Cli::parse()).await {
        Ok(code) => code,
        Err(err) => {
            // stderr, not stdout: the contract is that stdout is parseable JSON
            // lines whatever happens.
            eprintln!("queuey-interop: {err}");
            tracing::error!(error = %err, "command failed");
            ExitCode::FAILURE
        }
    }
}

/// `info` by default, `RUST_LOG` when set, **stderr** always.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
}

async fn run(cli: Cli) -> Result<ExitCode, Box<dyn Error>> {
    let url = std::env::var("AMQP_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    tracing::debug!(%url, "broker");

    match cli.command {
        Command::Produce(args) => produce(&url, &args).await,
        Command::Consume(args) => consume(&url, &args).await,
        Command::Purge => raw::purge(&url).await,
        Command::DrainDead(args) => raw::drain_dead(&url, args.expect, args.timeout_secs).await,
        Command::Inspect => raw::inspect(&url).await,
    }
}

/// Write one JSON object to stdout, on its own line.
///
/// `Stdout` is line buffered, so a consumer reading this process's pipe sees
/// each event as it happens rather than at exit; the explicit flush makes that
/// true even if the buffering policy ever changes underneath us.
pub(crate) fn emit(value: &Value) {
    let mut out = std::io::stdout().lock();
    if writeln!(out, "{value}").is_ok() {
        let _ = out.flush();
    }
}

/// Declare the topology and enqueue `count` jobs.
///
/// `Producer::new` is what declares `interop.jobs` and `interop.jobs.dead`, so
/// this is also the command that creates the queues a PHP consumer will bind to.
async fn produce(url: &str, args: &ProduceArgs) -> Result<ExitCode, Box<dyn Error>> {
    let backend = Arc::new(RabbitMqBackend::connect(url).await?);
    let producer = Producer::<InteropQueues, _>::new(Arc::clone(&backend)).await?;

    for offset in 0..args.count {
        let job = Echo {
            seq: args.start_seq + offset,
            text: args.text.clone(),
            fail_times: args.fail_times,
            fatal: args.fatal,
            defer_once_ms: args.defer_once_ms,
        };

        // `enqueue_with` is the general form the other three are shorthands for,
        // and the only one that takes a correlation id — so the plain methods
        // are used when there is none, which keeps them covered too.
        let job_id = match (args.correlation_id.as_deref(), args.after_ms, args.defer_ms) {
            (Some(correlation_id), after_ms, defer_ms) => {
                let options = match (after_ms, defer_ms) {
                    (Some(ms), _) => EnqueueOptions::after(Duration::from_millis(ms)),
                    (None, Some(ms)) => EnqueueOptions::deferred(Duration::from_millis(ms)),
                    (None, None) => EnqueueOptions::now(),
                };
                producer
                    .enqueue_with(&job, options.correlation_id(correlation_id))
                    .await?
            }
            (None, Some(ms), _) => {
                producer
                    .enqueue_after(&job, Duration::from_millis(ms))
                    .await?
            }
            (None, None, Some(ms)) => producer.defer(&job, Duration::from_millis(ms)).await?,
            (None, None, None) => producer.enqueue(&job).await?,
        };

        emit(&json!({
            "event": "enqueued",
            "seq": job.seq,
            "job_id": job_id.to_string(),
        }));
    }

    // Nothing else in this process wants the connection, and the test that runs
    // it wants the process gone.
    backend.close().await?;
    Ok(ExitCode::SUCCESS)
}

/// Run a worker until `expect` jobs have reached a terminal state.
///
/// Terminal means "will never be seen again": a success, or a dead letter of any
/// cause. A retry and a deferral are not terminal, which is why the `attempt`
/// lines are reported but not counted.
async fn consume(url: &str, args: &ConsumeArgs) -> Result<ExitCode, Box<dyn Error>> {
    let backend = Arc::new(RabbitMqBackend::connect(url).await?);
    let terminals = Arc::new(Terminals::default());

    let hook_terminals = Arc::clone(&terminals);
    let worker = Worker::<InteropQueues, _>::builder(backend)
        // This process exists to run the worker; let it close the connection.
        .close_backend_on_shutdown(true)
        .handler(EchoHandler {
            terminals: Arc::clone(&terminals),
        })
        .on_dead_letter(FnDeadLetterHook::new(move |dead: DeadLetter| {
            let terminals = Arc::clone(&hook_terminals);
            async move {
                emit(&dead_event(&dead));
                terminals.bump();
            }
        }))
        .build()
        .await?;

    let handle = worker.handle();
    let running = tokio::spawn(worker.run());

    let expect = args.expect;
    let terminals_reached = {
        let terminals = Arc::clone(&terminals);
        async move {
            while terminals.count() < expect {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    };

    let timed_out = tokio::select! {
        () = terminals_reached => false,
        () = tokio::time::sleep(Duration::from_secs(args.timeout_secs)) => true,
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("interrupted; shutting down");
            false
        }
    };

    // Graceful either way: stop consuming, finish what is in flight, close the
    // connection. A timed-out run still leaves the broker tidy.
    tracing::info!(terminals = terminals.count(), "shutting down");
    handle.shutdown();
    running.await??;

    let settle_failures = handle.settle_failures();
    if settle_failures > 0 {
        tracing::warn!(settle_failures, "outcomes the worker could not report");
    }

    if timed_out {
        eprintln!(
            "timed out after {}s with {} of {} terminal events",
            args.timeout_secs,
            terminals.count(),
            expect
        );
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}
