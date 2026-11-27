//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`. `name` is
//! the nats technology's, taken by that crate's own `declaration` feature;
//! the consumer and the batch a Location takes unless told otherwise are
//! declared here, and the code takes them from here.

use xcore::settings::{Applies, Fixed, Kind, Presence, Setting, Settings};

/// The durable consumer a Location pulls as unless told otherwise.
pub(crate) const DEFAULT_CONSUMER: &str = "xmip";

/// How many messages one receive pulls at most unless told otherwise.
pub(crate) const DEFAULT_BATCH: usize = 10;

/// Every setting a Location takes beyond its address, the server's host
/// and port: where a Location connects.
// DEFAULT_BATCH is ten, which no pointer width wraps.
#[allow(clippy::cast_possible_wrap)]
pub const SETTINGS: &Settings = &Settings {
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
        nats::declaration::NAME,
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a server that stops mid-line is waited on, and how long \
             a quiet server ends a batch; unbounded when left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["nats-jetstream"];
