#![forbid(unsafe_code)]

//! Streams that arrive as `JetStream` messages. One message is one Stream,
//! the stream, subject and sequence kept beside it.
//!
//! `JetStream` is NATS made durable: a stream on the server stores what is
//! published under its subjects, a consumer keeps its place in that stream,
//! and both outlive the connection that made them. A Receive Location makes
//! sure its stream and durable pull consumer exist, pulls a batch and
//! acknowledges each message it hands up — a restart resumes from the last
//! acknowledgement. A Send Location publishes and waits for the stream to
//! answer with the sequence it stored the message at; no answer means no
//! stream took it, and that is retryable.
//!
//! It is all core NATS underneath — the API is request and reply on
//! `$JS.API.*` subjects with JSON bodies, and delivery is a MSG whose reply
//! subject is where the acknowledgement goes — so this sits on the nats
//! technology's wire. Headers are not spoken: a pull that expires is silent
//! and the read timeout ends the batch. TLS is `xmip-core-library-tls`'s,
//! per ADR-0033.
//!
//! The origin URI carries what the delivery knew:
//! `nats-jetstream://server/orders/orders.new?seq=42`. A send target is
//! `nats-jetstream://host:4222/orders.new`, `host:4222/orders.new`, or a
//! subject alone on the configured server.

pub mod api;
pub mod client;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::JetStream;
pub use session::{Event, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

#[derive(Clone)]
pub struct JetStreamTransport {
    server: String,
    stream: String,
    subject: String,
    consumer: String,
    name: String,
    batch: usize,
    timeout: Option<Duration>,
}

impl JetStreamTransport {
    /// Speak to the server at `server` about `stream`, which covers
    /// `subject`. The consumer is `xmip` until named.
    #[must_use]
    pub fn new(
        server: impl Into<String>,
        stream: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            server: server.into(),
            stream: stream.into(),
            subject: subject.into(),
            consumer: "xmip".to_string(),
            name: "xmip".to_string(),
            batch: 10,
            timeout: None,
        }
    }

    /// The durable consumer this Location pulls as. Two Locations with one
    /// name share one place in the stream.
    #[must_use]
    pub fn as_consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = consumer.into();
        self
    }

    /// The name this Location presents in CONNECT.
    #[must_use]
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// How many messages one receive pulls at most.
    #[must_use]
    pub const fn in_batches_of(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Give up on a server that stops mid-line, and end a batch when the
    /// server has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the server as a client.
    ///
    /// # Errors
    /// Where the server could not be reached or does not run `JetStream`.
    pub fn connect(&self) -> Result<JetStream> {
        JetStream::connect(&self.server, &self.name, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.server)
    }

    /// Accept one client on an already-bound listener, serving this
    /// transport's stream.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the handshake failed.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Ok(Session::accept(listener, self.timeout)?.with_stream(&self.stream, &[&self.subject]))
    }

    /// Where a target names the server and subject itself, or is a subject
    /// alone on this transport's server.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match socket::target("nats-jetstream", target) {
            Some((peer, "")) => (peer, &self.subject),
            Some(pair) => pair,
            None => match target.split_once('/') {
                Some((peer, subject)) if peer.contains(':') => (peer, subject),
                _ => (&self.server, target),
            },
        }
    }
}

impl Transport for JetStreamTransport {
    fn name(&self) -> &'static str {
        "nats-jetstream"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Make sure the stream and consumer exist, pull one batch, acknowledge
    /// each message.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        client.ensure_stream(&self.stream, &[&self.subject])?;
        client.ensure_consumer(&self.stream, &self.consumer)?;
        let arrived = client.fetch(&self.stream, &self.consumer, self.batch)?;
        for message in &arrived {
            client.ack(message)?;
        }
        Ok(arrived)
    }

    /// Publish and wait for the stream's acknowledgement.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, subject) = self.resolve(target);
        let mut client = JetStream::connect(server, &self.name, self.timeout)?;
        client.publish(subject, bytes).map(|_| ())
    }
}

impl JetStreamTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one stream called `probe` over one subject called `probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for JetStreamTransport {
    fn take_one(&self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener)?;
        // The acknowledgement goes out before the publish is reported, so
        // the client has its sequence by the time this returns.
        session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client closed without publishing"))
    }
}

impl Loopback for JetStreamTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = self.bind()?;
        Ok(Box::new(Listening::new(self.clone(), listener, address)))
    }

    /// A fresh client to `address`, publishing on this transport's subject
    /// and waiting for the stream's acknowledgement by sequence.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self {
            server: address.to_string(),
            ..self.clone()
        }
        .send(&self.subject, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::{edge_payloads, sized_payloads};

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn far_end() -> JetStreamTransport {
        JetStreamTransport::new("127.0.0.1:0", "orders", "orders.*").timing_out_after(secs(2))
    }

    #[test]
    fn a_publish_is_stored_and_acknowledged_by_sequence() {
        let far_end = far_end();
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            let near = JetStreamTransport::new(address.clone(), "orders", "orders.new")
                .timing_out_after(secs(2));
            near.send("orders.new", b"order 1\r\nline 2")?;
            near.send(&format!("nats-jetstream://{address}/orders.cancel"), b"")?;
            let mut client = near.connect()?;
            let seq = client.publish("orders.new", b"third")?;
            drop(client);
            let mut impatient = JetStream::connect(&address, "probe", Some(secs(1)))?;
            let refused = impatient.publish("other.subject", b"nobody");
            Ok::<_, transport::TransportError>((seq, refused))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        let first = session.next_publish().expect("first").expect("one");
        assert_eq!(first.bytes, b"order 1\r\nline 2");
        assert!(first.origin_uri.ends_with("/orders/orders.new?seq=1"));
        assert!(session.next_publish().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener).expect("second");
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/orders/orders.cancel?seq=1"));
        assert!(second.bytes.is_empty());
        let mut session = far_end.accept_one(&listener).expect("third");
        assert_eq!(
            session.next_publish().expect("third").expect("one").bytes,
            b"third"
        );
        assert!(session.next_publish().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener).expect("fourth");
        assert!(session.next_publish().expect("gave up").is_none());
        assert_eq!(session.stored(), 0, "other.subject is not under orders.*");
        let (seq, refused) = sender.join().expect("thread").expect("sending");
        assert_eq!(seq, 1);
        let error = refused.expect_err("no stream took it");
        assert!(error.retryable);
        assert!(error.message.contains("no stream acknowledged"));
    }

    #[test]
    fn a_receive_ensures_the_stream_and_consumer_then_pulls_and_acks() {
        let far_end = far_end();
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            JetStreamTransport::new(address, "orders", "orders.*")
                .as_consumer("probe")
                .in_batches_of(2)
                .timing_out_after(secs(2))
                .receive()
        });
        let mut session = Session::accept(&listener, Some(secs(2)))
            .expect("accepting")
            .with_stream("orders", &["orders.*"])
            .with_messages("orders.new", &[b"first", b"second", b"third"]);
        let mut events = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            events.push(event);
        }
        assert_eq!(
            events,
            [
                Event::ConsumerCreated("probe".into()),
                Event::Fetched {
                    consumer: "probe".into(),
                    delivered: 2
                },
                Event::Acked(1),
                Event::Acked(2),
            ]
        );
        assert_eq!(session.acked(), [1, 2]);
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"first");
        assert!(arrived[0].origin_uri.ends_with("/orders/orders.new?seq=1"));
        assert!(arrived[1].origin_uri.ends_with("/orders/orders.new?seq=2"));

        // A bare session has no stream: the receive creates it and the
        // consumer, finds nothing, and nothing there is not an error.
        let address = listener.local_addr().expect("address").to_string();
        let receiver = std::thread::spawn(move || {
            JetStreamTransport::new(address, "orders", "orders.*")
                .timing_out_after(secs(1))
                .receive()
        });
        let mut session = Session::accept(&listener, Some(secs(2))).expect("accepting");
        let mut events = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            events.push(event);
        }
        assert_eq!(events[0], Event::StreamCreated("orders".into()));
        assert_eq!(events[1], Event::ConsumerCreated("xmip".into()));
        assert!(receiver.join().expect("thread").expect("empty").is_empty());
    }

    #[test]
    fn a_server_without_jetstream_or_speaking_nonsense_is_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            for info in [
                &b"INFO {\"server_id\":\"core\",\"version\":\"2.10.0\"}\r\n"[..],
                &b"INFO {\"jetstream\":true}\r\nCONNECT {}\r\n"[..],
                &b"220 mail.example ESMTP\r\n"[..],
            ] {
                let (mut stream, _) = listener.accept().expect("accept");
                std::io::Write::write_all(&mut stream, info).expect("write");
                // Take everything the client says up to its PING, so closing
                // is a FIN rather than a reset over unread bytes.
                let mut heard = Vec::new();
                let mut sink = [0u8; 1024];
                while !heard.ends_with(b"PING\r\n") {
                    match std::io::Read::read(&mut stream, &mut sink) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => heard.extend_from_slice(&sink[..n]),
                    }
                }
            }
        });
        let near = JetStreamTransport::new(address, "orders", "orders.*").timing_out_after(secs(2));
        let error = near.connect().err().expect("no jetstream");
        assert!(!error.retryable);
        assert!(error.message.contains("does not run JetStream"));
        let mut client = near.connect().expect("connected");
        let error = client.flush().expect_err("a CONNECT from a server");
        assert!(!error.retryable);
        assert!(!near.connect().err().expect("not nats").retryable);
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "nats-jetstream");
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = JetStreamTransport::loopback();
        let arrived = loopback.round(b"pub\r\nlished").expect("round");
        assert_eq!(arrived.bytes, b"pub\r\nlished");
        assert!(
            arrived
                .origin_uri
                .starts_with("nats-jetstream://127.0.0.1:")
        );
        assert!(arrived.origin_uri.ends_with("/probe/probe?seq=1"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = JetStreamTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }
}
