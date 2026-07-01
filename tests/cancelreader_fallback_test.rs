//! Fallback backend semantics, constructed directly so the router cannot
//! upgrade it to a real backend.

use std::io::{self, Cursor, ErrorKind, Read};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

use cancelreader::new_reader_plain;

mod common;
use common::{is_canceled, BlockingReader};

/// A reader whose one `read` returns a fixed error.
struct ErrReader {
    kind: ErrorKind,
    message: &'static str,
}

impl Read for ErrReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(self.kind, self.message))
    }
}

/// A reader that hands back one byte per `read`, then end of file.
struct DripReader {
    data: Vec<u8>,
    pos: usize,
}

impl Read for DripReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.data.len() {
            return Ok(0);
        }
        buf[0] = self.data[self.pos];
        self.pos += 1;
        Ok(1)
    }
}

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

/// Without a cancel, the inner reader's error passes through unchanged.
#[test]
fn fallback_passes_inner_error_through() {
    let mut cr = new_reader_plain(ErrReader {
        kind: ErrorKind::BrokenPipe,
        message: "boom",
    });

    let mut buf = [0u8; 4];
    let err = cr.read(&mut buf).expect_err("expected the inner error");
    assert!(
        !is_canceled(&err),
        "an uncanceled read must not report a cancel"
    );
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    assert_eq!(err.to_string(), "boom");
}

/// Without a cancel, partial read counts pass through byte for byte.
#[test]
fn fallback_passes_partial_reads_through() {
    let mut cr = new_reader_plain(DripReader {
        data: b"abc".to_vec(),
        pos: 0,
    });

    let mut buf = [0u8; 4];
    for expected in b"abc" {
        let n = cr.read(&mut buf).expect("expected one byte");
        assert_eq!(n, 1, "expected a single-byte read");
        assert_eq!(buf[0], *expected, "expected the next byte");
    }
    assert_eq!(cr.read(&mut buf).expect("expected end of file"), 0);
}

/// A read into an empty buffer returns zero and consumes nothing.
#[test]
fn fallback_zero_length_read() {
    let mut cr = new_reader_plain(DripReader {
        data: b"abc".to_vec(),
        pos: 0,
    });

    let mut empty: [u8; 0] = [];
    assert_eq!(cr.read(&mut empty).expect("expected zero"), 0);

    // The zero-length read consumed nothing, so the first byte is still there.
    let mut buf = [0u8; 1];
    assert_eq!(cr.read(&mut buf).expect("expected one byte"), 1);
    assert_eq!(buf[0], b'a');
}

/// Close on the fallback returns Ok and stays Ok when called again.
#[test]
fn fallback_close_is_idempotent() {
    let mut cr = new_reader_plain(io::empty());
    cr.close().expect("first close should succeed");
    cr.close().expect("second close should succeed");
}
