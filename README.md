# xmip-core-transport-nats-jetstream

NATS JetStream transport: durable streams and pull consumers over NATS — publish
acknowledged by sequence, fetch a batch, acknowledge each — a subject is a
Location that survives a restart. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
