//! The `Canceled` error value and the `is_canceled` source-chain walk.

use std::error::Error;
use std::fmt;
use std::io;

use cancelreader::{is_canceled, Canceled};

/// A wrapper that exposes its cause only through `source`, never `get_ref`.
///
/// This is the shape `is_canceled` must still see through: a custom error, a
/// boxed error, or an `anyhow`-style chain that holds `Canceled` as a source.
#[derive(Debug)]
struct SourceWrapper(io::Error);

impl fmt::Display for SourceWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("source wrapper")
    }
}

impl Error for SourceWrapper {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[test]
fn display_text_is_pinned() {
    assert_eq!(Canceled.to_string(), "read canceled");
}

#[test]
fn display_survives_io_error_wrapping() {
    let err: io::Error = Canceled.into();
    assert_eq!(err.to_string(), "read canceled");
}

#[test]
fn equality_holds() {
    assert_eq!(Canceled, Canceled);
}

#[test]
fn direct_cancellation_matches() {
    let direct: io::Error = Canceled.into();
    assert!(is_canceled(&direct));
}

#[test]
fn os_error_with_no_inner_does_not_match() {
    // A raw OS error has get_ref() == None, so the walk must handle a missing
    // inner value and return false.
    let err = io::Error::from_raw_os_error(5);
    assert!(err.get_ref().is_none(), "expected no inner value");
    assert!(!is_canceled(&err));
}

#[test]
fn plain_error_does_not_match() {
    let err = io::Error::other("plain");
    assert!(!is_canceled(&err));
}

#[test]
fn multi_level_io_wrapping_matches() {
    // Three nested io::Error layers around a cancellation.
    let deep = io::Error::other(io::Error::other(io::Error::other(Canceled)));
    assert!(is_canceled(&deep));
}

#[test]
fn cancellation_reachable_only_through_source_matches() {
    // The cancellation is not in any get_ref chain. It sits behind a custom
    // error's source(). The walk must follow source() to find it.
    let wrapper = SourceWrapper(Canceled.into());
    let err = io::Error::other(wrapper);
    assert!(is_canceled(&err));
}
