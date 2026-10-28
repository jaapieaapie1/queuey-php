//! The three subcommands that talk to RabbitMQ through `lapin` directly.
//!
//! `purge`, `drain-dead` and `inspect` are deliberately *not* written against
//! the queuey API. They are the harness's way of looking at the broker from
//! outside the library under test: what queuey (or queuey-php) actually left on
//! the wire, byte for byte, rather than what queuey says it did. queuey-rabbitmq
//! exposes no `lapin` type on purpose, so this module opens its own connection.

use std::{
    error::Error,
    process::ExitCode,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use futures::StreamExt;
use lapin::{
    Connection, ConnectionProperties, ErrorKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable, ShortString},
};
use serde_json::{Value, json};

use crate::{
    emit,
    interop::{DEAD_QUEUE, MAIN_QUEUE},
};

/// AMQP reply code for a clean `close`.
const REPLY_SUCCESS: u16 = 200;

/// Headers the harness reports on every dead message, present or not.
const REPORTED_HEADERS: [&str; 5] = [
    "x-death-reason",
    "x-original-queue",
    "x-attempts",
    "x-attempt",
    "x-deferrals",
];

/// Delete `interop.jobs` and `interop.jobs.dead`, so the next run starts from a
/// broker that has never heard of either.
///
/// Deleting a queue that is not there is not an error in RabbitMQ 3.x and later
/// (it answers `queue.delete-ok` with a zero message count), but a broker that
/// *did* answer `404 NOT_FOUND` would close the channel underneath us, so each
/// delete gets a channel of its own and a missing queue is reported rather than
/// propagated.
pub async fn purge(url: &str) -> Result<ExitCode, Box<dyn Error>> {
    let connection = Connection::connect(url, ConnectionProperties::default()).await?;

    for name in [MAIN_QUEUE, DEAD_QUEUE] {
        let channel = connection.create_channel().await?;
        let options = QueueDeleteOptions {
            if_unused: false,
            if_empty: false,
            nowait: false,
        };

        match channel.queue_delete(ShortString::from(name), options).await {
            Ok(messages) => emit(&json!({
                "event": "purged",
                "name": name,
                "messages": messages,
            })),
            Err(err) if is_not_found(&err) => emit(&json!({
                "event": "missing",
                "name": name,
            })),
            Err(err) => return Err(err.into()),
        }

        // A channel closed by the broker cannot be closed again; either way the
        // connection close below tidies up, so this is best-effort.
        let _ = channel.close(REPLY_SUCCESS, ShortString::from("")).await;
    }

    connection
        .close(REPLY_SUCCESS, ShortString::from(""))
        .await?;
    Ok(ExitCode::SUCCESS)
}

/// Consume and ack `expect` messages off `interop.jobs.dead`, reporting the full
/// AMQP shape of each: properties, the five headers the spec pins, and the body.
pub async fn drain_dead(
    url: &str,
    expect: usize,
    timeout_secs: u64,
) -> Result<ExitCode, Box<dyn Error>> {
    let connection = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    channel
        .basic_qos(50, BasicQosOptions { global: false })
        .await?;

    // No passive declare first: the queue is either there (queuey declared it)
    // or this is a test failure worth seeing as a consume error.
    let mut consumer = channel
        .basic_consume(
            ShortString::from(DEAD_QUEUE),
            ShortString::from("queuey-interop-drain"),
            BasicConsumeOptions {
                no_local: false,
                no_ack: false,
                exclusive: false,
                nowait: false,
            },
            FieldTable::default(),
        )
        .await?;

    let seen = AtomicUsize::new(0);
    let drain = async {
        while seen.load(Ordering::SeqCst) < expect {
            match consumer.next().await {
                Some(Ok(delivery)) => {
                    emit(&dead_message_event(&delivery));
                    delivery.acker.ack(BasicAckOptions::default()).await?;
                    seen.fetch_add(1, Ordering::SeqCst);
                }
                Some(Err(err)) => return Err(err),
                // The consumer was cancelled (the queue was deleted underneath
                // us, say); stop rather than spin.
                None => break,
            }
        }
        Ok(())
    };

    let outcome = tokio::time::timeout(Duration::from_secs(timeout_secs), drain).await;
    let drained = seen.load(Ordering::SeqCst);
    let code = match outcome {
        Ok(Ok(())) if drained >= expect => ExitCode::SUCCESS,
        Ok(Ok(())) => {
            eprintln!("consumer cancelled after {drained} of {expect} dead messages");
            ExitCode::FAILURE
        }
        Ok(Err(err)) => return Err(err.into()),
        Err(_) => {
            eprintln!(
                "timed out after {timeout_secs}s with {drained} of {expect} dead messages on {DEAD_QUEUE}"
            );
            ExitCode::FAILURE
        }
    };

    let _ = channel.close(REPLY_SUCCESS, ShortString::from("")).await;
    connection
        .close(REPLY_SUCCESS, ShortString::from(""))
        .await?;
    Ok(code)
}

/// Report the depth and consumer count of both interop queues, without
/// declaring anything: `passive = true` asks about a queue and never creates it.
///
/// A passive declare of a missing queue is a `404 NOT_FOUND`, which closes the
/// channel it ran on, so each queue gets its own channel.
pub async fn inspect(url: &str) -> Result<ExitCode, Box<dyn Error>> {
    let connection = Connection::connect(url, ConnectionProperties::default()).await?;

    for name in [MAIN_QUEUE, DEAD_QUEUE] {
        let channel = connection.create_channel().await?;
        let options = QueueDeclareOptions {
            passive: true,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
        };

        match channel
            .queue_declare(ShortString::from(name), options, FieldTable::default())
            .await
        {
            Ok(queue) => emit(&json!({
                "event": "queue",
                "name": name,
                "messages": queue.message_count(),
                "consumers": queue.consumer_count(),
            })),
            Err(err) if is_not_found(&err) => emit(&json!({
                "event": "missing",
                "name": name,
            })),
            Err(err) => return Err(err.into()),
        }

        let _ = channel.close(REPLY_SUCCESS, ShortString::from("")).await;
    }

    connection
        .close(REPLY_SUCCESS, ShortString::from(""))
        .await?;
    Ok(ExitCode::SUCCESS)
}

/// `{"event":"dead_message",...}`: everything about one message on `q.dead`.
fn dead_message_event(delivery: &Delivery) -> Value {
    let properties = &delivery.properties;
    let headers = properties.headers().as_ref();

    let reported = REPORTED_HEADERS
        .iter()
        .map(|key| {
            let value = headers
                .and_then(|table| table.inner().get(*key))
                .map_or(Value::Null, amqp_value_to_json);
            ((*key).to_owned(), value)
        })
        .collect::<serde_json::Map<_, _>>();

    json!({
        "event": "dead_message",
        "job_id": short_string(properties.message_id().as_ref()),
        "type": short_string(properties.kind().as_ref()),
        "content_type": short_string(properties.content_type().as_ref()),
        "delivery_mode": properties.delivery_mode().map_or(Value::Null, Value::from),
        "priority": properties.priority().map_or(Value::Null, Value::from),
        "headers": Value::Object(reported),
        "body": body_to_json(&delivery.data),
    })
}

/// An optional AMQP short string as a JSON string or `null`.
fn short_string(value: Option<&ShortString>) -> Value {
    value.map_or(Value::Null, |s| Value::from(s.as_str()))
}

/// The message body as JSON when it parses, and as a JSON *string* when it does
/// not — a malformed-envelope dead letter is forwarded verbatim, so the harness
/// has to be able to report bytes that are not JSON at all.
fn body_to_json(data: &[u8]) -> Value {
    serde_json::from_slice::<Value>(data)
        .unwrap_or_else(|_| Value::from(String::from_utf8_lossy(data).into_owned()))
}

/// Render one AMQP field-table value as JSON.
///
/// Only the types queuey actually writes are mapped structurally (unsigned ints
/// for the counters, long strings for the reasons); anything else becomes its
/// debug form, which is more useful in a test failure than a silent `null`.
fn amqp_value_to_json(value: &AMQPValue) -> Value {
    match value {
        AMQPValue::Boolean(v) => Value::from(*v),
        AMQPValue::ShortShortInt(v) => Value::from(*v),
        AMQPValue::ShortShortUInt(v) => Value::from(*v),
        AMQPValue::ShortInt(v) => Value::from(*v),
        AMQPValue::ShortUInt(v) => Value::from(*v),
        AMQPValue::LongInt(v) => Value::from(*v),
        AMQPValue::LongUInt(v) => Value::from(*v),
        AMQPValue::LongLongInt(v) => Value::from(*v),
        AMQPValue::Float(v) => Value::from(*v),
        AMQPValue::Double(v) => Value::from(*v),
        AMQPValue::Timestamp(v) => Value::from(*v),
        AMQPValue::ShortString(v) => Value::from(v.as_str()),
        AMQPValue::LongString(v) => Value::from(String::from_utf8_lossy(v.as_bytes()).into_owned()),
        AMQPValue::Void => Value::Null,
        other => Value::from(format!("{other:?}")),
    }
}

/// Whether the broker answered `404 NOT_FOUND`.
fn is_not_found(err: &lapin::Error) -> bool {
    matches!(err.kind(), ErrorKind::ProtocolError(amqp) if amqp.get_id() == 404)
}
