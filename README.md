# xmip-core-transport-nats-jetstream

NATS JetStream transport: durable streams and pull consumers over NATS — publish
acknowledged by sequence, fetch a batch, acknowledge each — a subject is a
Location that survives a restart. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The connection is the nats technology's `Client` — its connect, INFO and CONNECT, its lines and its pings — and `JetStream` is the request and reply spoken over it; until 2026-09-27 this crate carried a second NATS client of its own. A Send Location publishes on a connection kept per server (`transport::Pool`), and every publish waits for its acknowledgement.

A Receive Location pulls on a connection kept the same way, the stream and consumer made sure of once, on its first receive; each receive pulls one batch and acknowledges it. Until 2026-09-28 every receive connected and made sure of both again.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
