//! The client's side of one connection to a server that runs `JetStream`:
//! the API requests a Location needs, a publish that waits for its
//! acknowledgement, a pull that takes a batch, and the acknowledgement —
//! `+ACK` or `-NAK` — of each message taken.
//!
//! The connection is the nats technology's [`nats::Client`]: its connect,
//! INFO and CONNECT, its lines, its pings answered. What is here is the
//! request and reply `JetStream` speaks over it — a reply subject on the
//! PUB, and the MSG that answers on it.

use std::time::Duration;

use codec::{hex, random};
use nats::Client;
use nats::wire::Line;
use serde_json::Value;
use transport::error::{Result, TransportError, protocol_error};
use transport::pool::Pooled;

use crate::api::{self, Answer};

pub struct JetStream {
    client: Client,
    inbox: String,
    requests: u64,
}

/// One message a pull took: where it came from, its body, and the subject
/// its `+ACK` or `-NAK` is published to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pulled {
    pub origin_uri: String,
    pub payload: Vec<u8>,
    pub ack_subject: String,
}

impl JetStream {
    /// Connect to `server` as a NATS client presenting `name`, and listen on
    /// an inbox of this connection's own.
    ///
    /// # Errors
    /// Where the server could not be reached, did not open with INFO, or
    /// does not run `JetStream`.
    pub fn connect(server: &str, name: &str, timeout: Option<Duration>) -> Result<Self> {
        Self::open(Client::connect(server, name, timeout)?)
    }

    /// [`JetStream::connect`] for a publisher: a connection that says it
    /// sends headers, so a keyed publish can carry its `Nats-Msg-Id`. A
    /// consumer connects without, and is delivered messages without them.
    ///
    /// # Errors
    /// As [`JetStream::connect`].
    pub fn publishing(server: &str, name: &str, timeout: Option<Duration>) -> Result<Self> {
        Self::open(Client::connect_with_headers(server, name, timeout)?)
    }

    fn open(client: Client) -> Result<Self> {
        let info: Value = serde_json::from_str(client.info())
            .map_err(|e| protocol_error(format!("an INFO that is not JSON: {e}")))?;
        if info.get("jetstream").and_then(Value::as_bool) != Some(true) {
            return Err(protocol_error("the server does not run JetStream"));
        }
        let mut jetstream = Self {
            client,
            inbox: format!("{}.{}", api::INBOX, hex::encode(&random::array::<16>())),
            requests: 0,
        };
        let inbox = format!("{}.*", jetstream.inbox);
        jetstream.client.write(&Line::Sub {
            subject: inbox,
            queue: None,
            sid: "1".to_string(),
        })?;
        Ok(jetstream)
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

    /// Publish `bytes` on `subject`, under `key` as its `Nats-Msg-Id`
    /// where there is one, and wait for the stream that took it to say so;
    /// the sequence it was stored at. A stream that holds a message under
    /// that id within its duplicate window stores nothing and answers the
    /// first one's sequence.
    ///
    /// # Errors
    /// Where no stream answered before the timeout — no stream covers the
    /// subject, or the server is slow, and both are worth another try — or
    /// the server refused the message.
    pub fn publish(&mut self, subject: &str, bytes: &[u8], key: Option<&str>) -> Result<u64> {
        let reply = self.next_reply();
        let subject = subject.to_string();
        let payload = bytes.to_vec();
        self.client.write(&match key {
            Some(key) => Line::HPub {
                subject: subject.clone(),
                reply: Some(reply.clone()),
                headers: vec![(api::MSG_ID.to_string(), key.to_string())],
                payload,
            },
            None => Line::Pub {
                subject: subject.clone(),
                reply: Some(reply.clone()),
                payload,
            },
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

    /// Pull up to `batch` messages from `consumer` on `stream`, none of
    /// them acknowledged. The batch ends when it is full or the server has
    /// been quiet for the read timeout; nothing there is an empty vector.
    ///
    /// # Errors
    /// Where the connection broke before anything arrived, or the server
    /// reported an error.
    pub fn fetch(&mut self, stream: &str, consumer: &str, batch: usize) -> Result<Vec<Pulled>> {
        let reply = self.next_reply();
        let expires = self.client.read_timeout().map(|t| t.mul_f32(0.9));
        let request = api::next_request(batch, expires);
        self.client.write(&Line::Pub {
            subject: api::msg_next(stream, consumer),
            reply: Some(reply),
            payload: request.to_string().into_bytes(),
        })?;
        let mut arrived = Vec::new();
        while arrived.len() < batch {
            match self.client.next_line() {
                Ok(Some(Line::Msg {
                    subject,
                    reply: Some(ack),
                    payload,
                    ..
                })) if ack.starts_with(api::ACK_PREFIX) => {
                    let seq = api::stream_seq_of(&ack).unwrap_or(0);
                    let origin = format!(
                        "nats-jetstream://{}/{stream}/{subject}?seq={seq}",
                        self.client.server()
                    );
                    arrived.push(Pulled {
                        origin_uri: origin,
                        payload,
                        ack_subject: ack,
                    });
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(error) if error.retryable => break,
                Err(error) => return Err(error),
            }
        }
        Ok(arrived)
    }

    /// `+ACK` a message [`JetStream::fetch`] took, to its acknowledgement
    /// subject, so the server does not deliver it again; flushed, so the
    /// server has read it.
    ///
    /// # Errors
    /// Where the server went away.
    pub fn ack(&mut self, ack_subject: &str) -> Result<()> {
        self.answer(ack_subject, api::ACK)
    }

    /// `-NAK` a message [`JetStream::fetch`] took, so the server delivers
    /// it again now rather than after the consumer's ack wait; flushed.
    ///
    /// # Errors
    /// Where the server went away.
    pub fn nak(&mut self, ack_subject: &str) -> Result<()> {
        self.answer(ack_subject, api::NAK)
    }

    /// `+TERM` a message [`JetStream::fetch`] took: the server stops
    /// delivering it without counting it processed, and publishes a
    /// `MSG_TERMINATED` advisory where something listens for one; flushed.
    ///
    /// # Errors
    /// Where the server went away.
    pub fn term(&mut self, ack_subject: &str) -> Result<()> {
        self.answer(ack_subject, api::TERM)
    }

    fn answer(&mut self, ack_subject: &str, word: &[u8]) -> Result<()> {
        self.client.write(&Line::Pub {
            subject: ack_subject.to_string(),
            reply: None,
            payload: word.to_vec(),
        })?;
        self.flush()
    }

    /// Wait for the server to catch up: PING, and the PONG that says so.
    ///
    /// # Errors
    /// Where the server went away or answered with an error.
    pub fn flush(&mut self) -> Result<()> {
        self.client.flush()
    }

    /// One API request and its answer.
    fn request(&mut self, subject: &str, body: &Value) -> Result<Answer> {
        let reply = self.next_reply();
        let payload = if body.is_null() {
            Vec::new()
        } else {
            body.to_string().into_bytes()
        };
        self.client.write(&Line::Pub {
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
            match self.client.next_line()? {
                Some(Line::Msg {
                    subject, payload, ..
                }) if subject == reply => return Ok(payload),
                Some(_) => {}
                None => return Err(protocol_error("the server closed before answering")),
            }
        }
    }

    fn next_reply(&mut self) -> String {
        self.requests += 1;
        format!("{}.{}", self.inbox, self.requests)
    }
}

impl Pooled for JetStream {
    /// While the server has not closed the connection.
    fn usable(&mut self) -> bool {
        self.client.usable()
    }
}
