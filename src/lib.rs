#![forbid(unsafe_code)]

//! Streams that arrive as `JetStream` messages. One message is one Stream,
//! the stream, subject and sequence kept beside it.
//!
//! `JetStream` is NATS made durable: a stream on the server stores what is
//! published under its subjects, a consumer keeps its place in that stream,
//! and both outlive the connection that made them. A Receive Location makes
//! sure its stream and durable pull consumer exist, pulls a batch and hands
//! each message up unacknowledged: after the runtime's receive cycle it is
//! `+ACK`ed when accepted, `+TERM`ed when refused, never to be delivered
//! again, and `-NAK`ed when the cycle failed, for the server to deliver
//! again — a restart resumes from the last acknowledgement. A Send
//! Location publishes and waits for the stream to answer with the sequence
//! it stored the message at; no answer means no stream took it, and that is
//! retryable.
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

pub use client::{JetStream, Pulled};
use net::Target;
pub use session::{Event, Session};
use transport::error::TransportError;
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{
    Acknowledgement, Arrived, Configured, Directions, Pool, Taken, Transport, Verdict,
};
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

/// The durable consumer a Location pulls as unless told otherwise.
const DEFAULT_CONSUMER: &str = "xmip";

/// How many messages one receive pulls at most unless told otherwise.
const DEFAULT_BATCH: usize = 10;

#[derive(Clone)]
pub struct JetStreamTransport {
    server: String,
    stream: String,
    subject: String,
    consumer: String,
    name: String,
    batch: usize,
    timeout: Option<Duration>,
    /// The connections a send publishes on, connected once per server and
    /// kept.
    publishers: Pool<JetStream>,
    /// The connection a receive pulls on, its stream and consumer made sure
    /// of on the first receive and kept.
    consumers: Pool<JetStream>,
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
            consumer: DEFAULT_CONSUMER.to_string(),
            name: nats::DEFAULT_NAME.to_string(),
            batch: DEFAULT_BATCH,
            timeout: None,
            publishers: Pool::new(),
            consumers: Pool::new(),
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
    const fn in_batches_of(mut self, batch: usize) -> Self {
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
        match Target::naming_server(&["nats-jetstream"], target) {
            Some(named) if named.path().is_empty() => (named.authority(), &self.subject),
            Some(named) => (named.authority(), named.path()),
            None => (&self.server, target),
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

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered(
            "the acknowledgement goes on the session the receive reads from",
        )
    }

    /// Pull one batch, on the connection the first receive opened and made
    /// sure of the stream and consumer on, and kept. Nothing is
    /// acknowledged here: each message's [`answering`] `+ACK`s it after the
    /// receive cycle accepted it, `+TERM`s it when it refused it and
    /// `-NAK`s it when the cycle failed.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let pulled = self.consumers.exchange(
            self.server.as_str(),
            || {
                let mut client = self.connect()?;
                client.ensure_stream(&self.stream, &[&self.subject])?;
                client.ensure_consumer(&self.stream, &self.consumer)?;
                Ok(client)
            },
            |client| client.fetch(&self.stream, &self.consumer, self.batch),
        )?;
        Ok(pulled
            .into_iter()
            .map(|message| {
                let acknowledgement = answering(&self.consumers, &self.server, message.ack_subject);
                Arrived::whole(message.origin_uri, message.payload, acknowledgement)
            })
            .collect())
    }

    /// Publish on the connection kept for the server, connected on the
    /// first send to it, and wait for the stream's acknowledgement.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        self.publish(target, bytes, None)
    }

    /// The key is the message's `Nats-Msg-Id` header: a stream that holds a
    /// message under that id within its duplicate window stores nothing,
    /// and acknowledges the first one's sequence.
    fn send_keyed(&self, target: &str, bytes: &[u8], key: &str) -> Result<()> {
        self.publish(target, bytes, Some(key))
    }
}

impl JetStreamTransport {
    /// The one send, on the publisher kept for the target's server.
    fn publish(&self, target: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        let (server, subject) = self.resolve(target);
        self.publishers.exchange(
            server,
            || JetStream::publishing(server, &self.name, self.timeout),
            |client| client.publish(subject, bytes, key).map(|_| ()),
        )
    }
}

/// The acknowledgement of a pulled message, published to `ack_subject` on
/// the connection `consumers` keeps for `server`: `+ACK` on
/// [`Verdict::Accepted`], `+TERM` on [`Verdict::Refused`] — the server
/// stops delivering it without counting it processed — and `-NAK` on
/// [`Verdict::Failed`] for the server to deliver it again at once, each
/// flushed — one PING and PONG. Where the
/// server closed that connection meanwhile none is opened: the server
/// delivers again, after the consumer's ack wait, what was not answered.
fn answering(consumers: &Pool<JetStream>, server: &str, ack_subject: String) -> Acknowledgement {
    let consumers = consumers.clone();
    let server = server.to_string();
    Acknowledgement::deferred(move |verdict| {
        consumers.kept(
            server.as_str(),
            "the connection that pulled the message is closed; \
             the server delivers it again after the ack wait",
            |client| match verdict {
                Verdict::Accepted => client.ack(&ack_subject),
                Verdict::Refused(_) => client.term(&ack_subject),
                Verdict::Failed => client.nak(&ack_subject),
            },
        )
    })
}

impl Configured for JetStreamTransport {
    /// The address is the server's host and port: where a Location connects.
    // DEFAULT_BATCH is ten, which no pointer width wraps.
    #[allow(clippy::cast_possible_wrap)]
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "stream",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The stream a Receive Location makes sure of and pulls from.",
                applies: Applies::Receive,
            },
            Setting {
                name: "subject",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The subject the stream covers, and the one a Send Location \
                          publishes on when its target names none.",
                applies: Applies::Both,
            },
            Setting {
                name: "consumer",
                kind: Kind::Text,
                presence: Presence::Default(Fixed::Text(DEFAULT_CONSUMER)),
                meaning: "The durable consumer a Receive Location pulls as; two Locations \
                          with one name share one place in the stream.",
                applies: Applies::Receive,
            },
            Setting {
                name: "batch",
                kind: Kind::Integer {
                    minimum: 1,
                    maximum: u32::MAX as i64,
                },
                presence: Presence::Default(Fixed::Integer(DEFAULT_BATCH as i64)),
                meaning: "How many messages one receive pulls at most.",
                applies: Applies::Receive,
            },
            Setting {
                name: "name",
                kind: Kind::Text,
                presence: Presence::Default(Fixed::Text(nats::DEFAULT_NAME)),
                meaning: "The name a Location presents to the server in CONNECT.",
                applies: Applies::Both,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a server that stops mid-line is waited on, and how long \
                          a quiet server ends a batch; unbounded when left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let stream = settings.optional_text("stream").unwrap_or_default();
        let mut transport =
            Self::new(address, stream, settings.text("subject")).named(settings.text("name"));
        if let Some(consumer) = settings.optional_text("consumer") {
            transport = transport.as_consumer(consumer);
        }
        if let Some(batch) = settings.optional_integer("batch") {
            let batch = usize::try_from(batch).map_err(|_| {
                TransportError::permanent(format!(
                    "a batch of {batch} is more than this system holds"
                ))
            })?;
            transport = transport.in_batches_of(batch);
        }
        if let Some(timeout) = settings.optional_duration("timeout") {
            transport = transport.timing_out_after(timeout);
        }
        Ok(transport)
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
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
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
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
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
    fn nats_jetstream_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(
            JetStreamTransport::SETTINGS.problems(),
            Vec::<String>::new()
        );
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let given = [
            text("stream", "orders"),
            text("subject", "orders.*"),
            ("batch".to_string(), Given::Integer(25)),
            text("timeout", "2s"),
        ];
        let built = JetStreamTransport::open("bus:4222", Applies::Receive, &given).expect("built");
        assert_eq!(built.stream, "orders");
        assert_eq!(built.subject, "orders.*");
        assert_eq!(built.consumer, DEFAULT_CONSUMER);
        assert_eq!(built.batch, 25);
        assert_eq!(built.timeout, Some(secs(2)));
        let sent = JetStreamTransport::open("bus:4222", Applies::Send, &given[1..2]);
        assert_eq!(sent.expect("built").batch, DEFAULT_BATCH);
        let Err(refused) = JetStreamTransport::open("bus:4222", Applies::Send, &given) else {
            panic!("stream is a receive setting");
        };
        assert!(
            refused.message.contains("\"stream\""),
            "{}",
            refused.message
        );
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
            let seq = client.publish("orders.new", b"third", None)?;
            drop(client);
            let mut impatient = JetStream::connect(&address, "probe", Some(secs(1)))?;
            let refused = impatient.publish("other.subject", b"nobody", None);
            Ok::<_, transport::TransportError>((seq, refused))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        let first = session.next_publish().expect("first").expect("one");
        assert_eq!(first.bytes, b"order 1\r\nline 2");
        assert!(first.origin_uri.ends_with("/orders/orders.new?seq=1"));
        // The same server, so the same connection: connected once.
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/orders/orders.cancel?seq=2"));
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
    fn a_thousand_publishes_connect_once_and_a_connection_the_server_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = far_end().timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near =
            JetStreamTransport::new(address, "orders", "orders.new").timing_out_after(secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("orders.new", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a publish.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("orders.new", b"after the close")
        });
        // One CONNECT for every publish: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for n in 0..SENDS {
            let arrived = session.next_publish().expect("publish").expect("one");
            assert_eq!(arrived.bytes, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new connection");
        let last = again.next_publish().expect("publish").expect("one");
        assert_eq!(last.bytes, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.publishers.opened(), 2);
    }

    #[test]
    fn a_receive_ensures_the_stream_and_consumer_then_pulls_and_acks_naks_or_terms() {
        let far_end = far_end();
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            let near = JetStreamTransport::new(address, "orders", "orders.*")
                .as_consumer("probe")
                .in_batches_of(3)
                .timing_out_after(secs(2));
            let mut arrived = near.receive()?.into_iter();
            let first = arrived.next().expect("first");
            assert!(first.defers());
            let first = first.taken()?;
            let second = arrived.next().expect("second");
            let origin = second.origin_uri.clone();
            second.failed()?;
            arrived
                .next()
                .expect("third")
                .refused(transport::Refusal::Forbidden)?;
            Ok::<_, transport::TransportError>((first, origin))
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
                    delivered: 3
                },
                Event::Acked(1),
                Event::Naked(2),
                Event::Termed(3),
            ]
        );
        assert_eq!(session.acked(), [1]);
        let (first, second) = receiver.join().expect("thread").expect("receiving");
        assert_eq!(first.bytes, b"first");
        assert!(first.origin_uri.ends_with("/orders/orders.new?seq=1"));
        assert!(second.ends_with("/orders/orders.new?seq=2"));

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
    fn three_receives_make_sure_once_and_a_connection_the_server_closed_is_replaced() {
        let (listener, address) = far_end().bind().expect("binding");
        let near = JetStreamTransport::new(address, "orders", "orders.*")
            .in_batches_of(1)
            .timing_out_after(secs(2));
        let (go, going) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut arrived = Vec::new();
            for _ in 0..3 {
                for one in near.receive()? {
                    arrived.push(one.taken()?.bytes);
                }
            }
            going.recv().expect("go");
            for one in near.receive()? {
                arrived.push(one.taken()?.bytes);
            }
            Ok::<_, transport::TransportError>((arrived, near.consumers.opened()))
        });
        // A second's quiet ends the serving of the first session: the
        // client waits for the word to go on, its connection still open.
        let accept = || {
            Session::accept(&listener, Some(secs(1)))
                .expect("accepting")
                .with_stream("orders", &["orders.*"])
                .with_messages("orders.new", &[b"first", b"second", b"third"])
        };
        let served = |session: &mut Session| {
            let mut events = Vec::new();
            while let Ok(Some(event)) = session.next_event() {
                events.push(event);
            }
            events
        };
        let count =
            |events: &[Event], what: fn(&Event) -> bool| events.iter().filter(|e| what(e)).count();
        let acked = |e: &Event| matches!(e, Event::Acked(_));
        let made = |e: &Event| matches!(e, Event::ConsumerCreated(_));
        // One connection and one consumer made sure of, for three pulls.
        let mut session = accept();
        let events = served(&mut session);
        assert_eq!(count(&events, acked), 3, "{events:?}");
        assert_eq!(count(&events, made), 1, "{events:?}");
        drop(session);
        go.send(()).expect("went");
        let mut again = accept();
        let events = served(&mut again);
        assert_eq!(count(&events, acked), 1, "{events:?}");
        let (arrived, opened) = receiver.join().expect("thread").expect("receiving");
        let bytes: Vec<&[u8]> = arrived.iter().map(Vec::as_slice).collect();
        assert_eq!(bytes, [&b"first"[..], b"second", b"third", b"first"]);
        assert_eq!(opened, 2);
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
