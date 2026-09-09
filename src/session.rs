//! The server's side of one connection, one stream's worth of `JetStream`:
//! what a test puts at the far end, and what the playground drives.
//!
//! Not a server. One session serves one client from a stream kept in
//! memory: it answers the API subjects for that stream, stores what is
//! published under its subjects and acknowledges by sequence, delivers a
//! pull's batch with acknowledgement subjects, and records each `+ACK`.
//! Clustering, disk, limits and the rest of a server are a server's.

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use nats::wire::{Line, encode, read};
use serde_json::{Value, json};
use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::api::{self, ApiError};

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client created the stream.
    StreamCreated(String),
    /// The client created a consumer.
    ConsumerCreated(String),
    /// The client published under the stream; here is the Stream.
    Published(Arrived),
    /// The client pulled; this many were delivered.
    Fetched { consumer: String, delivered: usize },
    /// The client acknowledged the message at this sequence.
    Acked(u64),
}

struct Stream {
    name: String,
    subjects: Vec<String>,
    /// Subject and payload, the sequence being the index plus one.
    messages: Vec<(String, Vec<u8>)>,
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    subscribed: Vec<(String, String)>,
    stream: Option<Stream>,
    /// Each consumer and the next sequence it will be delivered.
    consumers: BTreeMap<String, u64>,
    acked: Vec<u64>,
}

impl Session {
    /// Accept one client on `listener`, send INFO and take its CONNECT.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the client did not
    /// answer INFO with CONNECT.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut session = Self {
            reader,
            writer,
            peer,
            subscribed: Vec::new(),
            stream: None,
            consumers: BTreeMap::new(),
            acked: Vec::new(),
        };
        let info = json!({
            "server_id": "xmip",
            "version": "2.10.0",
            "jetstream": true,
            "headers": false,
            "max_payload": 1_048_576,
        });
        session.write(&Line::Info(info.to_string()))?;
        match read(&mut session.reader)? {
            Some(Line::Connect(_)) => Ok(session),
            _ => Err(protocol_error(
                "the client did not answer INFO with CONNECT",
            )),
        }
    }

    /// Serve `name` over `subjects` as though it had been created already.
    #[must_use]
    pub fn with_stream(mut self, name: &str, subjects: &[&str]) -> Self {
        self.stream = Some(Stream {
            name: name.to_string(),
            subjects: subjects.iter().map(ToString::to_string).collect(),
            messages: Vec::new(),
        });
        self
    }

    /// Hold `payloads` under `subject` in the stream for a consumer to pull.
    #[must_use]
    pub fn with_messages(mut self, subject: &str, payloads: &[&[u8]]) -> Self {
        if let Some(stream) = &mut self.stream {
            for payload in payloads {
                stream
                    .messages
                    .push((subject.to_string(), payload.to_vec()));
            }
        }
        self
    }

    /// The sequences acknowledged so far, in the order they were.
    #[must_use]
    pub fn acked(&self) -> &[u64] {
        &self.acked
    }

    /// How many messages the stream holds.
    #[must_use]
    pub fn stored(&self) -> usize {
        self.stream.as_ref().map_or(0, |s| s.messages.len())
    }

    /// The next message the client publishes under the stream, or `None`
    /// when it closed. Everything else is served on the way.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Arrived>> {
        loop {
            match self.next_event()? {
                Some(Event::Published(arrived)) => return Ok(Some(arrived)),
                Some(_) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next thing the client did, or `None` when it closed.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or
    /// the client sent what only a server sends.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            match read(&mut self.reader)? {
                Some(Line::Pub {
                    subject,
                    reply,
                    payload,
                }) => {
                    if let Some(event) = self.on_pub(&subject, reply.as_deref(), payload)? {
                        return Ok(Some(event));
                    }
                }
                Some(Line::Sub { subject, sid, .. }) => self.subscribed.push((subject, sid)),
                Some(Line::Unsub { sid }) => self.subscribed.retain(|(_, s)| *s != sid),
                Some(Line::Ping) => self.write(&Line::Pong)?,
                Some(Line::Pong | Line::Connect(_)) => {}
                None => return Ok(None),
                Some(other) => {
                    self.write(&Line::Err("Unknown Protocol Operation".to_string()))?;
                    return Err(protocol_error(format!("{other:?} from a client")));
                }
            }
        }
    }

    /// An API request, an acknowledgement, or a message for the stream.
    fn on_pub(
        &mut self,
        subject: &str,
        reply: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<Option<Event>> {
        if let Some(rest) = subject.strip_prefix(&format!("{}.", api::API)) {
            let rest = rest.to_string();
            return self.api(&rest, reply, &payload);
        }
        if subject.starts_with(api::ACK_PREFIX) {
            let seq = api::stream_seq_of(subject)
                .ok_or_else(|| protocol_error("an acknowledgement naming no sequence"))?;
            self.acked.push(seq);
            return Ok(Some(Event::Acked(seq)));
        }
        let Some(stream) = &mut self.stream else {
            return Ok(None);
        };
        if !stream.subjects.iter().any(|s| matches(s, subject)) {
            return Ok(None);
        }
        stream.messages.push((subject.to_string(), payload.clone()));
        let seq = u64::try_from(stream.messages.len()).unwrap_or(0);
        let origin = format!(
            "nats-jetstream://{}/{}/{subject}?seq={seq}",
            self.peer, stream.name
        );
        let ack = api::pub_ack(&stream.name, seq);
        self.answer(reply, &ack)?;
        Ok(Some(Event::Published(Arrived::new(origin, payload))))
    }

    fn api(&mut self, rest: &str, reply: Option<&str>, payload: &[u8]) -> Result<Option<Event>> {
        let parts: Vec<&str> = rest.split('.').collect();
        let known = self.stream.as_ref().map(|s| s.name.clone());
        let (answer, event) = match parts.as_slice() {
            ["STREAM", "INFO", name] if known.as_deref() == Some(*name) => {
                (self.stream_info(), None)
            }
            ["STREAM", "INFO", _] => (not_found(10059, "stream not found"), None),
            ["STREAM", "CREATE", name] => {
                let config = json_of(payload)?;
                let subjects = config["subjects"]
                    .as_array()
                    .map(|s| s.iter().filter_map(Value::as_str).collect::<Vec<_>>())
                    .unwrap_or_default();
                self.stream = Some(Stream {
                    name: (*name).to_string(),
                    subjects: subjects.iter().map(ToString::to_string).collect(),
                    messages: Vec::new(),
                });
                (
                    self.stream_info(),
                    Some(Event::StreamCreated((*name).to_string())),
                )
            }
            ["CONSUMER", "INFO", _, consumer] if self.consumers.contains_key(*consumer) => {
                (consumer_info(consumer), None)
            }
            ["CONSUMER", "INFO", _, _] => (not_found(10014, "consumer not found"), None),
            ["CONSUMER", "CREATE", _, consumer] => {
                self.consumers.entry((*consumer).to_string()).or_insert(1);
                (
                    consumer_info(consumer),
                    Some(Event::ConsumerCreated((*consumer).to_string())),
                )
            }
            ["CONSUMER", "MSG", "NEXT", _, consumer] => {
                let batch = json_of(payload)?["batch"].as_u64().unwrap_or(1);
                let delivered = self.deliver(consumer, batch, reply)?;
                let event = Event::Fetched {
                    consumer: (*consumer).to_string(),
                    delivered,
                };
                return Ok(Some(event));
            }
            _ => (ApiError::new(400, 10005, "not served here").to_json(), None),
        };
        self.answer(reply, &answer)?;
        Ok(event)
    }

    fn stream_info(&self) -> Value {
        let Some(stream) = &self.stream else {
            return not_found(10059, "stream not found");
        };
        json!({
            "config": { "name": stream.name, "subjects": stream.subjects },
            "state": { "messages": stream.messages.len() },
        })
    }

    /// Up to `batch` messages from the consumer's cursor on, each to the
    /// pull's inbox with its acknowledgement subject as the reply.
    fn deliver(&mut self, consumer: &str, batch: u64, reply: Option<&str>) -> Result<usize> {
        let Some(reply) = reply else {
            return Err(protocol_error("a pull without a reply inbox"));
        };
        let sid = self
            .sid_for(reply)
            .ok_or_else(|| protocol_error("a reply inbox nobody subscribed to"))?;
        let stream_name = self
            .stream
            .as_ref()
            .map(|s| s.name.clone())
            .unwrap_or_default();
        let cursor = *self.consumers.entry(consumer.to_string()).or_insert(1);
        let total = self.stream.as_ref().map_or(0, |s| s.messages.len());
        let mut delivered = 0;
        for seq in cursor..=u64::try_from(total).unwrap_or(0) {
            if u64::try_from(delivered).unwrap_or(0) >= batch {
                break;
            }
            let index = usize::try_from(seq - 1).unwrap_or(0);
            let (subject, payload) = self
                .stream
                .as_ref()
                .map(|s| s.messages[index].clone())
                .expect("a stream with messages");
            let pending = u64::try_from(total).unwrap_or(0) - seq;
            let ack = api::ack_subject(&stream_name, consumer, 1, seq, seq, pending);
            self.write(&Line::Msg {
                subject,
                sid: sid.clone(),
                reply: Some(ack),
                payload,
            })?;
            delivered += 1;
            self.consumers.insert(consumer.to_string(), seq + 1);
        }
        Ok(delivered)
    }

    /// MSG `value` to whoever subscribed to `reply`.
    fn answer(&mut self, reply: Option<&str>, value: &Value) -> Result<()> {
        let Some(reply) = reply else {
            return Ok(());
        };
        let sid = self
            .sid_for(reply)
            .ok_or_else(|| protocol_error("a reply inbox nobody subscribed to"))?;
        self.write(&Line::Msg {
            subject: reply.to_string(),
            sid,
            reply: None,
            payload: value.to_string().into_bytes(),
        })
    }

    fn sid_for(&self, subject: &str) -> Option<String> {
        self.subscribed
            .iter()
            .find(|(pattern, _)| matches(pattern, subject))
            .map(|(_, sid)| sid.clone())
    }

    fn write(&mut self, line: &Line) -> Result<()> {
        self.writer
            .write_all(&encode(line))
            .map_err(|e| classify("writing a protocol line", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a protocol line", &e))
    }
}

fn consumer_info(consumer: &str) -> Value {
    json!({
        "name": consumer,
        "config": { "durable_name": consumer, "ack_policy": "explicit" },
    })
}

fn not_found(err_code: u32, description: &str) -> Value {
    ApiError::new(404, err_code, description).to_json()
}

fn json_of(payload: &[u8]) -> Result<Value> {
    if payload.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(payload)
        .map_err(|e| protocol_error(format!("a request that is not JSON: {e}")))
}

/// Whether `subject` is under `pattern`: `*` is one token, `>` the rest.
#[must_use]
pub fn matches(pattern: &str, subject: &str) -> bool {
    let mut tokens = subject.split('.');
    for token in pattern.split('.') {
        match (token, tokens.next()) {
            (">", Some(_)) => return true,
            ("*", Some(_)) => {}
            (expected, Some(actual)) if expected == actual => {}
            _ => return false,
        }
    }
    tokens.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_subject_is_under_its_pattern_token_by_token() {
        assert!(matches("orders.*", "orders.new"));
        assert!(!matches("orders.*", "orders.new.eu"));
        assert!(matches("orders.>", "orders.new.eu"));
        assert!(!matches("orders.>", "orders"));
        assert!(matches("_INBOX.abc.*", "_INBOX.abc.3"));
        assert!(!matches("_INBOX.abc.*", "_INBOX.xyz.3"));
        assert!(matches("a", "a"));
        assert!(!matches("a", "a.b"));
    }
}
