//! Cross-platform contract: a reader without a file descriptor cannot cancel.

use std::io;

use cancelreader::{is_canceled, new_reader_plain, ErrCanceled};

mod common;

/// A non-file reader routes to the fallback, and its cancel reports failure.
#[test]
fn reader_non_file() {
    let cr = new_reader_plain(std::io::empty());
    assert!(
        !cr.cancel(),
        "expected cancellation to fail for a non-file reader"
    );
}

/// Calling cancel more than once stays stable and never panics.
#[test]
fn cancel_idempotent() {
    let cr = new_reader_plain(std::io::empty());
    for _ in 0..8 {
        assert!(!cr.cancel(), "fallback cancel should always report failure");
    }
}

/// A cancellation is detectable even when wrapped one level deep.
#[test]
fn err_canceled_matchable() {
    let direct: io::Error = ErrCanceled.into();
    assert!(is_canceled(&direct), "a direct cancellation should match");

    let wrapped = io::Error::other(direct);
    assert!(is_canceled(&wrapped), "a wrapped cancellation should match");

    let unrelated = io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
    assert!(
        !is_canceled(&unrelated),
        "an unrelated error should not match"
    );
}
