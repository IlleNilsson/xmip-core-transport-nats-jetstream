//! The client's side of one connection to a server that runs `JetStream`:
//! the API requests a Location needs, a publish that waits for its
//! acknowledgement, a pull that takes a batch, and the acknowledgement of
//! each message taken.
//!
//! This sits on the nats technology's wire rather than on its `Client`,
//! because a request needs a reply subject on the PUB and the reply subject
//! off the MSG, and core NATS at-most-once has no use for either.

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nats::wire::{Line, encode, read};
use serde_json::{Value, json};
use transport::Arrived;
use transport::error::{Result, TransportError, classify, protocol_error};
use transport::socket;

use crate::api::{self, Answer};

pub struct JetStream {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    server: String,
    inbox: String,
    requests: u64,
    /// Where each message fetched and not yet acknowledged is acknowledged,
    /// by its origin.
    pending: BTreeMap<String, String>,
}

impl JetStream {
    /// Connect to `server`, take its INFO, answer with CONNECT and listen on
    /// an inbox of this connection's own.
    ///
    /// # Errors
    /// Where the server could not be reached, did not open with INFO, or
    /// does not run `JetStream`.
    pub fn connect(server: &str, name: &str, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(server, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let mut client = Self {
            reader,
            writer,
            server: server.to_string(),
            inbox: format!("{}.{unique:x}", api::INBOX),
            requests: 0,
            pending: BTreeMap::new(),
        };
        let Some(Line::Info(info)) = read(&mut client.reader)? else {
            return Err(protocol_error("the server did not open with INFO"));
        };
        let info: Value = serde_json::from_str(&info)
            .map_err(|e| protocol_error(format!("an INFO that is not JSON: {e}")))?;
        if info.get("jetstream").and_then(Value::as_bool) != Some(true) {
            return Err(protocol_error("the server does not run JetStream"));
        }
        let connect = json!({
            "verbose": false,
            "pedantic": false,
            "headers": false,
            "name": name,
            "lang": "rust",
            "version": "0.1.0",
        });
        client.write(&Line::Connect(connect.to_string()))?;
        client.write(&Line::Sub {
            subject: format!("{}.*", client.inbox),
            queue: None,
            sid: "1".to_string(),
        })?;
        Ok(client)
    }

    /// The stream, created over `subjects` where it is not there.
    ///
    /// # Errors
    /// Where the server went away or refused the stream.
    pub fn ensure_stream(&mut self, stream: &str, subjects: &[&str]) -> Result<()> {
        match self.request(&api::stream_info(stream), &Value::Null)? {
            Answer::Ok(_) => Ok(()),
            Answer::Error(error) if error.is_not_found() => {
                let config = api::stream_config(stream, subjects);
                self.request(&api::stream_create(stream), &config)?
                    .into_value()
                    .map(|_| ())
            }
            Answer::Error(error) => Err(error.into_transport()),
        }
    }

    /// The durable pull consumer, created where it is not there.
    ///
    /// # Errors
    /// Where the server went away or refused the consumer.
    pub fn ensure_consumer(&mut self, stream: &str, consumer: &str) -> Result<()> {
        match self.request(&api::consumer_info(stream, consumer), &Value::Null)? {
            Answer::Ok(_) => Ok(()),
            Answer::Error(error) if error.is_not_found() => {
                let config = api::consumer_config(stream, consumer);
                self.request(&api::consumer_create(stream, consumer), &config)?
                    .into_value()
                    .map(|_| ())
            }
            Answer::Error(error) => Err(error.into_transport()),
        }
    }

    /// Publish `bytes` on `subject` and wait for the stream that took it to
    /// say so; the sequence it was stored at.
    ///
    /// # Errors
    /// Where no stream answered before the timeout — no stream covers the
    /// subject, or the server is slow, and both are worth another try — or
    /// the server refused the message.
    pub fn publish(&mut self, subject: &str, bytes: &[u8]) -> Result<u64> {
        let reply = self.next_reply();
        self.write(&Line::Pub {
            subject: subject.to_string(),
            reply: Some(reply.clone()),
            payload: bytes.to_vec(),
        })?;
        let answer = self.wait_for(&reply).map_err(|error| {
            if error.retryable {
                TransportError::retryable(format!(
                    "no stream acknowledged {subject}: {}",
                    error.message
                ))
            } else {
                error
            }
        })?;
        let ack = Answer::parse(&answer)?.into_value()?;
        ack.get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| protocol_error("a publish acknowledged without a sequence"))
    }

    /// Pull up to `batch` messages from `consumer` on `stream`. The batch
    /// ends when it is full or the server has been quiet for the read
    /// timeout; nothing there is an empty vector.
    ///
    /// # Errors
    /// Where the connection broke before anything arrived, or the server
    /// reported an error.
    pub fn fetch(&mut self, stream: &str, consumer: &str, batch: usize) -> Result<Vec<Arrived>> {
        let reply = self.next_reply();
        let expires = self
            .reader
            .get_ref()
            .read_timeout()
            .ok()
            .flatten()
            .map(|t| t.mul_f32(0.9));
        let request = api::next_request(batch, expires);
        self.write(&Line::Pub {
            subject: api::msg_next(stream, consumer),
            reply: Some(reply),
            payload: request.to_string().into_bytes(),
        })?;
        let mut arrived = Vec::new();
        while arrived.len() < batch {
            match self.next_line() {
                Ok(Some(Line::Msg {
                    subject,
                    reply: Some(ack),
                    payload,
                    ..
                })) if ack.starts_with(api::ACK_PREFIX) => {
                    let seq = api::stream_seq_of(&ack).unwrap_or(0);
                    let origin = format!(
                        "nats-jetstream://{}/{stream}/{subject}?seq={seq}",
                        self.server
                    );
                    self.pending.insert(origin.clone(), ack);
                    arrived.push(Arrived::new(origin, payload));
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(error) if error.retryable => break,
                Err(error) => return Err(error),
            }
        }
        Ok(arrived)
    }

    /// Acknowledge a message [`JetStream::fetch`] handed back, so the server
    /// does not deliver it again.
    ///
    /// # Errors
    /// Where the message was not fetched on this connection, or the server
    /// went away.
    pub fn ack(&mut self, arrived: &Arrived) -> Result<()> {
        let subject = self.pending.remove(&arrived.origin_uri).ok_or_else(|| {
            TransportError::permanent(format!(
                "{} was not fetched on this connection",
                arrived.origin_uri
            ))
        })?;
        self.write(&Line::Pub {
            subject,
            reply: None,
            payload: api::ACK.to_vec(),
        })?;
        self.flush()
    }

    /// Wait for the server to catch up: PING, and the PONG that says so.
    ///
    /// # Errors
    /// Where the server went away or answered with an error.
    pub fn flush(&mut self) -> Result<()> {
        self.write(&Line::Ping)?;
        loop {
            match self.next_line()? {
                Some(Line::Pong) => return Ok(()),
                Some(_) => {}
                None => return Err(protocol_error("the server closed before PONG")),
            }
        }
    }

    /// One API request and its answer.
    fn request(&mut self, subject: &str, body: &Value) -> Result<Answer> {
        let reply = self.next_reply();
        let payload = if body.is_null() {
            Vec::new()
        } else {
            body.to_string().into_bytes()
        };
        self.write(&Line::Pub {
            subject: subject.to_string(),
            reply: Some(reply.clone()),
            payload,
        })?;
        Answer::parse(&self.wait_for(&reply)?)
    }

    /// The payload of the MSG that answers on `reply`; anything else on the
    /// way is ignored.
    fn wait_for(&mut self, reply: &str) -> Result<Vec<u8>> {
        loop {
            match self.next_line()? {
                Some(Line::Msg {
                    subject, payload, ..
                }) if subject == reply => return Ok(payload),
                Some(_) => {}
                None => return Err(protocol_error("the server closed before answering")),
            }
        }
    }

    /// The next line, pings answered and errors raised.
    fn next_line(&mut self) -> Result<Option<Line>> {
        loop {
            match read(&mut self.reader)? {
                Some(Line::Ping) => self.write(&Line::Pong)?,
                Some(Line::Err(message)) => return Err(protocol_error(message)),
                other => return Ok(other),
            }
        }
    }

    fn next_reply(&mut self) -> String {
        self.requests += 1;
        format!("{}.{}", self.inbox, self.requests)
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
