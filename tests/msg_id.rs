//! A keyed send carries its deduplication key in the `Nats-Msg-Id` header,
//! the same on every attempt of one Journey: the stream stores it once and
//! acknowledges the second publish at the first one's sequence. An unkeyed
//! send carries none and is stored again.

use std::thread;

use transport::Transport;
use xmip_core_transport_nats_jetstream::JetStreamTransport;

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_publish_carries_the_journey_id_as_its_msg_id_and_is_stored_once() {
    let far_end = JetStreamTransport::loopback();
    let (listener, address) = far_end.bind().expect("bound");
    let sender = thread::spawn(move || {
        let near = JetStreamTransport::new(address, "probe", "probe");
        near.send_keyed("probe", b"order", KEY)?;
        near.send_keyed("probe", b"order", KEY)?;
        near.send("probe", b"order")
    });
    let mut session = far_end.accept_one(&listener).expect("accepted");
    let origins: Vec<String> = (0..3)
        .map(|_| {
            let published = session.next_publish().expect("read").expect("published");
            assert_eq!(published.bytes, b"order");
            published.origin_uri
        })
        .collect();
    sender.join().expect("sender").expect("sent");
    let sequences: Vec<&str> = origins
        .iter()
        .map(|origin| origin.rsplit_once('?').expect("a sequence").1)
        .collect();
    assert_eq!(sequences, ["seq=1", "seq=1", "seq=2"]);
    assert_eq!(session.stored(), 2, "the repeat is not stored again");
}
