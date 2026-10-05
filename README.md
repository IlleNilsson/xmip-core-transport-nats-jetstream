# xmip-core-transport-nats-jetstream

NATS JetStream transport: durable streams and pull consumers over NATS — publish
acknowledged by sequence, fetch a batch, acknowledge each — a subject is a
Location that survives a restart. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The connection is the nats technology's `Client` — its connect, INFO and CONNECT, its lines and its pings — and `JetStream` is the request and reply spoken over it; until 2026-09-27 this crate carried a second NATS client of its own. A Send Location publishes on a connection kept per server (`transport::Pool`), and every publish waits for its acknowledgement.

A Receive Location pulls on a connection kept the same way, the stream and consumer made sure of once, on its first receive; each receive pulls one batch. Until 2026-09-28 every receive connected and made sure of both again.

A pulled message is acknowledged after the runtime's whole receive cycle, never as it is pulled: `+ACK` to its acknowledgement subject when the cycle accepted it, `+TERM` when it refused it, so the server stops delivering it without counting it processed, and `-NAK` when the cycle failed, so the server delivers it again at once; each is flushed, one PING and PONG, as the acknowledgement always was. Where the server closed the pulling connection meanwhile none is opened for the answer: the server delivers again, after the consumer's ack wait, what was not answered. `Session` reports a `-NAK` as `Event::Naked` and a `+TERM` as `Event::Termed`. Until 2026-10-02 a batch was acknowledged as it was pulled.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## The deduplication key

A keyed send (`Transport::send_keyed`, built 2026-10-04) publishes with headers — an HPUB, through the nats technology's `wire::Line::HPub` — carrying the Journey's identifier as `Nats-Msg-Id` (`api::MSG_ID`), the same on every attempt of one Journey. A stream that holds a message under that id within its duplicate window stores nothing and acknowledges the first one's sequence, `"duplicate": true`; the in-process `Session` does the same. The publishing connection says `headers` in its CONNECT (`JetStream::publishing`); a consumer's does not, and is delivered messages without them. An unkeyed `send` is a plain PUB, as before. The answers the `Session` writes for a consumer and a missing stream or consumer, and the reading of a request's JSON, moved into `api.rs` beside the rest of the API the same day.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
