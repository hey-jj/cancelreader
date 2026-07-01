//! A cancelable reader.
//!
//! Wrap a blocking input source, typically `stdin` or a raw terminal, so a
//! second thread can abort a blocked read during shutdown of a terminal
//! application. The abort consumes no input.
//!
//! The mechanism is the self-pipe trick paired with an operating system
//! readiness wait. A read blocks until either the input file descriptor becomes
//! readable or a cancel signal arrives. [`CancelReader::cancel`] writes to the
//! cancel pipe (or sets an event on Windows), wakes the wait, and makes the read
//! return [`ErrCanceled`] instead of data.
//!
//! Backends are selected at compile time:
//!
//! - Linux uses epoll.
//! - macOS and the BSDs use kqueue.
//! - Solaris uses the POSIX select syscall.
//! - Windows uses `WaitForMultipleObjects` with overlapped reads from `CONIN$`.
//! - Every other target uses a fallback that never interrupts an in-flight read.
//!
//! Readers that do not expose a raw file descriptor also use the fallback. The
//! fallback flips a flag so future reads return [`ErrCanceled`], but it cannot
//! unblock a read that is already running.
//!
//! # Example
//!
//! ```no_run
//! use std::io::Read;
//! use cancelreader::{new_reader, is_canceled};
//!
//! let file = std::fs::File::open("/dev/tty").unwrap();
//! let mut reader = new_reader(file).unwrap();
//!
//! let mut buf = [0u8; 1024];
//! loop {
//!     match reader.read(&mut buf) {
//!         Ok(0) => break,
//!         Ok(n) => { /* handle n bytes */ }
//!         Err(err) if is_canceled(&err) => {
//!             println!("canceled");
//!             break;
//!         }
//!         Err(err) => panic!("read failed: {err}"),
//!     }
//! }
//! ```

#![warn(missing_docs)]

use std::error::Error;
use std::fmt;
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod fallback;

#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
    target_os = "solaris",
))]
mod unix;

#[cfg(windows)]
mod windows;

/// The error returned when reading from a canceled reader.
///
/// The [`fmt::Display`] text is exactly `read canceled`. Every backend returns
/// this same value, so callers can identify a cancellation with [`is_canceled`]
/// regardless of platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrCanceled;

impl fmt::Display for ErrCanceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("read canceled")
    }
}

impl Error for ErrCanceled {}

impl From<ErrCanceled> for io::Error {
    fn from(err: ErrCanceled) -> Self {
        io::Error::other(err)
    }
}

/// Report whether an error is a cancellation.
///
/// This unwraps nested [`io::Error`] values and walks the source chain, so a
/// wrapped [`ErrCanceled`] still counts. It mirrors the way callers check the
/// sentinel value on the source language.
pub fn is_canceled(err: &io::Error) -> bool {
    let mut current: Option<&(dyn Error + 'static)> =
        err.get_ref().map(|e| e as &(dyn Error + 'static));
    while let Some(err) = current {
        if err.downcast_ref::<ErrCanceled>().is_some() {
            return true;
        }
        // A nested `io::Error` keeps its own inner value in `get_ref`, not in
        // `source`, so prefer that when present.
        current = match err.downcast_ref::<io::Error>() {
            Some(io_err) => io_err.get_ref().map(|e| e as &(dyn Error + 'static)),
            None => err.source(),
        };
    }
    false
}

/// A [`Read`] whose reads can be canceled without consuming data.
///
/// The reader owns operating system resources and cleans them up on drop. Call
/// [`close`](CancelReader::close) to surface any teardown error.
pub trait CancelReader: Read {
    /// Cancel ongoing and future reads. Return `true` if the cancel succeeded.
    ///
    /// The fallback backend and non-file readers always return `false`. A file
    /// backend returns `true` when the wake signal was delivered. Windows
    /// returns `false` when it cannot rendezvous with a wedged read within
    /// 100 milliseconds.
    ///
    /// This takes a shared reference so another thread can cancel a read that is
    /// in flight.
    fn cancel(&self) -> bool;

    /// Return a handle that can cancel this reader from another thread.
    ///
    /// The reader itself reads with `&mut self`, so it cannot be shared while a
    /// read runs. Take a [`Canceler`] first, move the reader into the read
    /// thread, then cancel through the handle.
    fn canceler(&self) -> Canceler;

    /// Release the reader's resources and return the first teardown error.
    ///
    /// Drop calls this too, so explicit calls are only needed to observe the
    /// error. Calling it twice is safe.
    fn close(&mut self) -> io::Result<()>;
}

/// A reader backed by a raw file descriptor, plus a name.
///
/// Only readers that implement this can use a real backend. On Windows the
/// reader must also share stdin's handle. Everything else routes to the
/// fallback. The name feeds the `/dev/tty` special case on the BSDs.
pub trait File: Read {
    /// The raw file descriptor on Unix, or the raw handle on Windows.
    fn raw(&self) -> RawDescriptor;

    /// The file's name, used by the BSD `/dev/tty` special case.
    fn name(&self) -> &str;
}

#[cfg(unix)]
type RawDescriptor = std::os::fd::RawFd;

#[cfg(windows)]
type RawDescriptor = std::os::windows::io::RawHandle;

#[cfg(not(any(unix, windows)))]
type RawDescriptor = i32;

#[cfg(unix)]
impl File for std::fs::File {
    fn raw(&self) -> RawDescriptor {
        std::os::fd::AsRawFd::as_raw_fd(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

#[cfg(unix)]
impl File for std::io::Stdin {
    fn raw(&self) -> RawDescriptor {
        std::os::fd::AsRawFd::as_raw_fd(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

#[cfg(windows)]
impl File for std::fs::File {
    fn raw(&self) -> RawDescriptor {
        std::os::windows::io::AsRawHandle::as_raw_handle(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

#[cfg(windows)]
impl File for std::io::Stdin {
    fn raw(&self) -> RawDescriptor {
        std::os::windows::io::AsRawHandle::as_raw_handle(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

/// A thread-safe cancellation flag.
///
/// Set once, read from any thread. An [`AtomicBool`] is enough because the flag
/// only moves from false to true.
#[derive(Debug, Default)]
struct CancelFlag {
    canceled: AtomicBool,
}

impl CancelFlag {
    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }

    fn set_canceled(&self) {
        self.canceled.store(true, Ordering::SeqCst);
    }
}

/// A handle that cancels a reader from another thread.
///
/// Obtain one with [`CancelReader::canceler`]. Calling [`cancel`](Canceler::cancel)
/// has the same effect and return value as [`CancelReader::cancel`].
pub struct Canceler {
    inner: CancelerInner,
}

enum CancelerInner {
    Fallback(Arc<CancelFlag>),
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "solaris",
    ))]
    Unix(Arc<unix::CancelState>),
    #[cfg(windows)]
    Windows(Arc<windows::CancelState>),
}

impl Canceler {
    fn fallback(flag: Arc<CancelFlag>) -> Self {
        Canceler {
            inner: CancelerInner::Fallback(flag),
        }
    }

    /// Cancel the reader. Return `true` if the cancel succeeded.
    ///
    /// The fallback backend always returns `false`.
    pub fn cancel(&self) -> bool {
        match &self.inner {
            CancelerInner::Fallback(flag) => {
                flag.set_canceled();
                false
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly",
                target_os = "solaris",
            ))]
            CancelerInner::Unix(state) => state.cancel(),
            #[cfg(windows)]
            CancelerInner::Windows(state) => state.cancel(),
        }
    }
}

/// Wrap a reader so its reads can be canceled.
///
/// The returned reader implements [`CancelReader`]. When the input exposes a raw
/// file descriptor, [`cancel`](CancelReader::cancel) can interrupt a blocked
/// read. Otherwise the reader falls back to a version that cannot interrupt an
/// in-flight read and whose `cancel` always returns `false`.
///
/// # Errors
///
/// Returns an error if a backend fails to set up its readiness primitive or
/// self-pipe.
pub fn new_reader<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
where
    R: File + Send + 'static,
{
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "solaris",
    ))]
    {
        unix::new_reader(reader)
    }

    #[cfg(windows)]
    {
        windows::new_reader(reader)
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "solaris",
        windows,
    )))]
    {
        Ok(fallback::new_fallback_cancel_reader(reader))
    }
}

/// Wrap a plain reader that does not expose a file descriptor.
///
/// This always uses the fallback backend. `cancel` returns `false` and cannot
/// interrupt an in-flight read, but future reads return [`ErrCanceled`].
pub fn new_reader_plain<R>(reader: R) -> Box<dyn CancelReader + Send>
where
    R: Read + Send + 'static,
{
    fallback::new_fallback_cancel_reader(reader)
}

pub use fallback::new_fallback_cancel_reader;
