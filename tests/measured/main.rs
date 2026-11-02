//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand publishes on one connection take a millisecond each at
//!   most.

use std::time::{Duration, Instant};

use transport::Transport;
use xmip_core_transport_nats_jetstream::JetStreamTransport;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_thousand_publishes_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let far_end =
        JetStreamTransport::new("127.0.0.1:0", "orders", "orders.*").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = JetStreamTransport::new(address, "orders", "orders.new").timing_out_after(secs(5));
    let sender = std::thread::spawn(move || {
        let began = Instant::now();
        for n in 0..SENDS {
            near.send("orders.new", n.to_string().as_bytes())?;
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond a publish.
        assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
        Ok::<_, transport::TransportError>(())
    });
    let mut session = far_end.accept_one(&listener).expect("accepting");
    for n in 0..SENDS {
        let arrived = session.next_publish().expect("publish").expect("one");
        assert_eq!(arrived.bytes, n.to_string().as_bytes());
    }
    sender.join().expect("thread").expect("sending");
}
