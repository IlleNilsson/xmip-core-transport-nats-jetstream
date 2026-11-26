# xmip-core-transport-nats-jetstream

NATS JetStream transport: durable streams and pull consumers over NATS — publish
acknowledged by sequence, fetch a batch, acknowledge each — a subject is a
Location that survives a restart. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The connection is the nats technology's `Client` — its connect, INFO and CONNECT, its lines and its pings — and `JetStream` is the request and reply spoken over it; until 2026-09-27 this crate carried a second NATS client of its own. A Send Location publishes on a connection kept per server (`transport::Pool`), and every publish waits for its acknowledgement.

A Receive Location pulls on a connection kept the same way, the stream and consumer made sure of once, on its first receive; each receive pulls one batch. Until 2026-09-28 every receive connected and made sure of both again.

A pulled message is acknowledged after the runtime's whole receive cycle, never as it is pulled: `+ACK` to its acknowledgement subject when the cycle accepted it, `+TERM` when it refused it, so the server stops delivering it without counting it processed, and `-NAK` when the cycle failed, so the server delivers it again at once; each is flushed, one PING and PONG, as the acknowledgement always was. Where the server closed the pulling connection meanwhile none is opened for the answer: the server delivers again, after the consumer's ack wait, what was not answered. `Session` reports a `-NAK` as `Event::Naked` and a `+TERM` as `Event::Termed`. Until 2026-10-02 a batch was acknowledged as it was pulled.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

A Send Location publishes to the subject its `subject` setting names, on the server its address names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which `transport::destination::on_server` reads as that subject. A target naming another still wins — `orders.cancel`, or `nats-jetstream://host:4222/orders.cancel` (ADR-0064, amendment 2026-11-10).

## The deduplication key

A keyed send (the key `Transport::send_from` is handed, `Outgoing::key`; built 2026-10-04) publishes with headers — an HPUB, through the nats technology's `wire::Line::HPub` — carrying the Journey's identifier as `Nats-Msg-Id` (`api::MSG_ID`), the same on every attempt on one Journey. A stream that holds a message under that id within its duplicate window stores nothing and acknowledges the first one's sequence, `"duplicate": true`; the in-process `Session` does the same. The publishing connection says `headers` in its CONNECT (`JetStream::publishing`); a consumer's does not, and is delivered messages without them. An unkeyed `send` is a plain PUB, as before. The answers the `Session` writes for a consumer and a missing stream or consumer, and the reading of a request's JSON, moved into `api.rs` beside the rest of the API the same day.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `stream`, `subject`, `consumer`, `batch`, `name` and `timeout`, and `SCHEMES`, `nats-jetstream`; `name` is the nats technology's (`nats::declaration::NAME`), which this crate's declaration takes by that crate's own `declaration` feature, and the consumer and the batch a Location takes unless told otherwise are declared there, where the code takes them from. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the nats technology's code and `serde_json` among them — are behind the default feature. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check a `JetStream` Location's settings though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
