//! The `JetStream` API as it travels over core NATS: a request is a PUB on a
//! `$JS.API.*` subject with a JSON body and a reply inbox, an answer is a
//! MSG on that inbox with a JSON body, and a message a consumer pulls
//! carries its acknowledgement subject as the reply. This file names the
//! subjects and writes and reads the bodies; nothing here touches a socket.

use std::time::Duration;

use serde_json::{Value, json};
use transport::error::{Result, TransportError, protocol_error};

/// Where every API request goes.
pub const API: &str = "$JS.API";
/// Where a client listens for answers; a unique suffix follows.
pub const INBOX: &str = "_INBOX";
/// What acknowledgement subjects open with.
pub const ACK_PREFIX: &str = "$JS.ACK";
/// The one word an acknowledgement carries.
pub const ACK: &[u8] = b"+ACK";

#[must_use]
pub fn stream_info(stream: &str) -> String {
    format!("{API}.STREAM.INFO.{stream}")
}

#[must_use]
pub fn stream_create(stream: &str) -> String {
    format!("{API}.STREAM.CREATE.{stream}")
}

#[must_use]
pub fn consumer_info(stream: &str, consumer: &str) -> String {
    format!("{API}.CONSUMER.INFO.{stream}.{consumer}")
}

#[must_use]
pub fn consumer_create(stream: &str, consumer: &str) -> String {
    format!("{API}.CONSUMER.CREATE.{stream}.{consumer}")
}

#[must_use]
pub fn msg_next(stream: &str, consumer: &str) -> String {
    format!("{API}.CONSUMER.MSG.NEXT.{stream}.{consumer}")
}

/// A stream kept on disk, bounded by limits alone, over `subjects`.
#[must_use]
pub fn stream_config(name: &str, subjects: &[&str]) -> Value {
    json!({
        "name": name,
        "subjects": subjects,
        "retention": "limits",
        "storage": "file",
    })
}

/// A durable pull consumer on `stream` that wants every message acknowledged
/// one by one.
#[must_use]
pub fn consumer_config(stream: &str, durable: &str) -> Value {
    json!({
        "stream_name": stream,
        "config": {
            "durable_name": durable,
            "ack_policy": "explicit",
        },
    })
}

/// A pull for up to `batch` messages, the request expiring after `expires`
/// on the server so it does not outlive the client's patience.
#[must_use]
pub fn next_request(batch: usize, expires: Option<Duration>) -> Value {
    let mut request = json!({ "batch": batch });
    if let Some(expires) = expires {
        let nanos = u64::try_from(expires.as_nanos()).unwrap_or(u64::MAX);
        request["expires"] = json!(nanos);
    }
    request
}

/// What the server answers a publish with: where it went and its sequence.
#[must_use]
pub fn pub_ack(stream: &str, seq: u64) -> Value {
    json!({ "stream": stream, "seq": seq })
}

/// The reply subject a delivered message carries — the acknowledgement goes
/// there — as the server writes it: stream, consumer, how many times
/// delivered, the stream and consumer sequences, a timestamp, how many are
/// still pending.
#[must_use]
pub fn ack_subject(
    stream: &str,
    consumer: &str,
    delivered: u64,
    stream_seq: u64,
    consumer_seq: u64,
    pending: u64,
) -> String {
    format!("{ACK_PREFIX}.{stream}.{consumer}.{delivered}.{stream_seq}.{consumer_seq}.0.{pending}")
}

/// The stream sequence an acknowledgement subject names, where it is one.
#[must_use]
pub fn stream_seq_of(ack_subject: &str) -> Option<u64> {
    let tokens: Vec<&str> = ack_subject.split('.').collect();
    match tokens.as_slice() {
        ["$JS", "ACK", _stream, _consumer, _delivered, stream_seq, ..] => stream_seq.parse().ok(),
        _ => None,
    }
}

/// An error the API answered with, as it wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    /// An HTTP-shaped status: 404 for a stream or consumer that is not there.
    pub code: u16,
    pub err_code: u32,
    pub description: String,
}

impl ApiError {
    #[must_use]
    pub fn new(code: u16, err_code: u32, description: impl Into<String>) -> Self {
        Self {
            code,
            err_code,
            description: description.into(),
        }
    }

    /// The stream or consumer asked about does not exist.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.code == 404
    }

    /// As the server writes it, inside an answer.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "error": {
                "code": self.code,
                "err_code": self.err_code,
                "description": self.description,
            },
        })
    }

    /// As a transport failure: the server's own trouble is retryable, a
    /// request it refused is not.
    #[must_use]
    pub fn into_transport(self) -> TransportError {
        let message = format!("JetStream answered {}: {}", self.code, self.description);
        if self.code >= 500 {
            TransportError::retryable(message)
        } else {
            TransportError::permanent(message)
        }
    }
}

/// What an API request came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    Ok(Value),
    Error(ApiError),
}

impl Answer {
    /// Read an answer's body.
    ///
    /// # Errors
    /// A body that is not JSON.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| protocol_error(format!("an answer that is not JSON: {e}")))?;
        let Some(error) = value.get("error") else {
            return Ok(Self::Ok(value));
        };
        let code = error.get("code").and_then(Value::as_u64).unwrap_or(500);
        let err_code = error.get("err_code").and_then(Value::as_u64).unwrap_or(0);
        let description = error
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("no description")
            .to_string();
        Ok(Self::Error(ApiError::new(
            u16::try_from(code).unwrap_or(500),
            u32::try_from(err_code).unwrap_or(0),
            description,
        )))
    }

    /// The answer's JSON, an error becoming a failure.
    ///
    /// # Errors
    /// The error the server answered with.
    pub fn into_value(self) -> Result<Value> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Error(error) => Err(error.into_transport()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_subjects_are_the_servers_and_the_ack_names_its_sequence() {
        assert_eq!(stream_info("orders"), "$JS.API.STREAM.INFO.orders");
        assert_eq!(stream_create("orders"), "$JS.API.STREAM.CREATE.orders");
        assert_eq!(
            consumer_create("orders", "xmip"),
            "$JS.API.CONSUMER.CREATE.orders.xmip"
        );
        assert_eq!(
            consumer_info("orders", "xmip"),
            "$JS.API.CONSUMER.INFO.orders.xmip"
        );
        assert_eq!(
            msg_next("orders", "xmip"),
            "$JS.API.CONSUMER.MSG.NEXT.orders.xmip"
        );
        let ack = ack_subject("orders", "xmip", 1, 42, 7, 3);
        assert_eq!(ack, "$JS.ACK.orders.xmip.1.42.7.0.3");
        assert_eq!(stream_seq_of(&ack), Some(42));
        assert_eq!(stream_seq_of("_INBOX.1.2"), None);
        assert_eq!(stream_seq_of("$JS.ACK.a.b.c.d"), None);
    }

    #[test]
    fn bodies_carry_what_the_server_reads() {
        let stream = stream_config("orders", &["orders.*"]);
        assert_eq!(stream["subjects"], json!(["orders.*"]));
        assert_eq!(stream["storage"], "file");
        let consumer = consumer_config("orders", "xmip");
        assert_eq!(consumer["config"]["ack_policy"], "explicit");
        let next = next_request(5, Some(Duration::from_secs(2)));
        assert_eq!(next["batch"], 5);
        assert_eq!(next["expires"], 2_000_000_000u64);
        assert!(next_request(1, None).get("expires").is_none());
        assert_eq!(pub_ack("orders", 3)["seq"], 3);
    }

    #[test]
    fn an_answer_is_its_value_or_its_error() {
        let ok = Answer::parse(br#"{"stream":"orders","seq":9}"#).expect("json");
        assert_eq!(ok.into_value().expect("ok")["seq"], 9);
        let error = ApiError::new(404, 10059, "stream not found");
        let body = serde_json::to_vec(&error.to_json()).expect("json");
        let parsed = Answer::parse(&body).expect("json");
        assert_eq!(parsed, Answer::Error(error.clone()));
        assert!(error.is_not_found());
        assert!(!parsed.into_value().expect_err("error").retryable);
        assert!(ApiError::new(503, 0, "busy").into_transport().retryable);
        assert!(Answer::parse(b"not json").is_err());
    }
}
