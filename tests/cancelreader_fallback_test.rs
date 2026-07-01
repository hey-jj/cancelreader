//! Fallback backend semantics, constructed directly so the router cannot
//! upgrade it to a real backend.

use std::io::{Cursor, Read};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

use cancelreader::new_reader_plain;

mod common;
use common::{is_canceled, BlockingReader};

/// A cancel racing a blocked read still surfaces as a cancellation, and the
/// underlying read runs to completion first.
#[test]
fn fallback_reader_concurrent_cancel() {
    let (started_tx, started_rx) = channel();
    let (unblock_tx, unblock_rx) = channel();
    let read_flag = Arc::new(Mutex::new(false));

    let reader = BlockingReader::new(started_tx, unblock_rx, Arc::clone(&read_flag));
    let mut cr = new_reader_plain(reader);
    let canceler = cr.canceler();

    let handle = thread::spawn(move || {
        let mut sink = Vec::new();
        let err = cr
            .read_to_end(&mut sink)
            .expect_err("expected a cancellation");
        assert!(is_canceled(&err), "expected a cancellation, got {err}");
    });

    // Wait for the read to start before canceling.
    started_rx.recv().unwrap();
    canceler.cancel();
    unblock_tx.send(()).unwrap();

    handle.join().unwrap();

    // The fallback waited for the blocked read rather than abandoning it.
    assert!(
        *read_flag.lock().unwrap(),
        "the reader was canceled before the read finished, which should not happen"
    );
}

/// Pass-through before cancel, empty cancellation after.
#[test]
fn fallback_reader() {
    let mut cr = new_reader_plain(Cursor::new(b"first".to_vec()));

    let mut first = Vec::new();
    cr.read_to_end(&mut first).expect("expected no error");
    assert_eq!(first, b"first", "expected the buffered text");

    cr.cancel();

    let mut second = Vec::new();
    let err = cr
        .read_to_end(&mut second)
        .expect_err("expected a cancellation");
    assert!(is_canceled(&err), "expected a cancellation, got {err}");
    assert!(second.is_empty(), "expected an empty read after cancel");
}
