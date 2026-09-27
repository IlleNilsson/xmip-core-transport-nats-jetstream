# xmip-core-transport-nats-jetstream

NATS JetStream transport: durable streams and pull consumers over NATS — publish
acknowledged by sequence, fetch a batch, acknowledge each — a subject is a
Location that survives a restart. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The connection is the nats technology's `Client` — its connect, INFO and CONNECT, its lines and its pings — and `JetStream` is the request and reply spoken over it; until 2026-09-27 this crate carried a second NATS client of its own. A Send Location publishes on a connection kept per server (`transport::Pool`), and every publish waits for its acknowledgement.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
