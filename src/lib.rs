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
//! return [`Canceled`] instead of data.
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
//! fallback flips a flag so future reads return [`Canceled`], but it cannot
//! unblock a read that is already running.
//!
//! # Example
//!
//! ```no_run
//! use std::io::Read;
//! use cancelreader::{named, new_reader, is_canceled};
//!
//! let file = std::fs::File::open("/dev/tty").unwrap();
//! let mut reader = new_reader(named(file, "/dev/tty")).unwrap();
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
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("read canceled")
    }
}

impl Error for Canceled {}

impl From<Canceled> for io::Error {
    fn from(err: Canceled) -> Self {
        io::Error::other(err)
    }
}

/// Report whether an error is a cancellation.
///
/// Every backend returns [`Canceled`] wrapped in an [`io::Error`], so a caller
/// can also write `err.get_ref().and_then(|e| e.downcast_ref::<Canceled>())`.
/// This helper does that and keeps looking through further wrapping, so a
/// cancellation buried under extra layers still matches.
///
/// It handles two nesting shapes at every depth. An [`io::Error`] keeps its
/// inner value in [`get_ref`](io::Error::get_ref), which its own
/// [`Error::source`] does not expose, so this reads `get_ref` for `io::Error`
/// nodes. Any other error type exposes its cause through `source`, so this
/// follows `source` there. Both a `Canceled` wrapped in nested `io::Error`s and
/// one reachable only through a custom error's `source` are found.
pub fn is_canceled(err: &io::Error) -> bool {
    // Start at the top error's inner value. The outer node is always an
    // `io::Error`, which cannot itself be a `Canceled`, so descend one step.
    let mut current = inner_of(err);
    while let Some(node) = current {
        if node.downcast_ref::<Canceled>().is_some() {
            return true;
        }
        current = match node.downcast_ref::<io::Error>() {
            Some(io_err) => inner_of(io_err),
            None => node.source(),
        };
    }
    false
}

/// The inner value of an [`io::Error`], as a trait object.
fn inner_of(err: &io::Error) -> Option<&(dyn Error + 'static)> {
    err.get_ref().map(|e| e as &(dyn Error + 'static))
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
///
/// [`std::fs::File`] and [`std::io::Stdin`] implement this. Their [`name`] is
/// empty, so a terminal opened as a plain file does not hit the `/dev/tty`
/// branch. Use [`named`] to tag such a reader with its path.
///
/// [`name`]: RawInput::name
pub trait RawInput: Read {
    /// The raw file descriptor on Unix, or the raw handle on Windows.
    fn raw(&self) -> RawDescriptor;

    /// The file's name, used by the BSD `/dev/tty` special case.
    fn name(&self) -> &str;
}

/// The raw descriptor [`RawInput::raw`] returns: a file descriptor on Unix, a
/// handle on Windows.
#[cfg(unix)]
pub type RawDescriptor = std::os::fd::RawFd;

/// The raw descriptor [`RawInput::raw`] returns: a file descriptor on Unix, a
/// handle on Windows.
#[cfg(windows)]
pub type RawDescriptor = std::os::windows::io::RawHandle;

/// The raw descriptor [`RawInput::raw`] returns: a file descriptor on Unix, a
/// handle on Windows.
#[cfg(not(any(unix, windows)))]
pub type RawDescriptor = i32;

#[cfg(unix)]
impl RawInput for std::fs::File {
    fn raw(&self) -> RawDescriptor {
        std::os::fd::AsRawFd::as_raw_fd(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

#[cfg(unix)]
impl RawInput for std::io::Stdin {
    fn raw(&self) -> RawDescriptor {
        std::os::fd::AsRawFd::as_raw_fd(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

#[cfg(windows)]
impl RawInput for std::fs::File {
    fn raw(&self) -> RawDescriptor {
        std::os::windows::io::AsRawHandle::as_raw_handle(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

#[cfg(windows)]
impl RawInput for std::io::Stdin {
    fn raw(&self) -> RawDescriptor {
        std::os::windows::io::AsRawHandle::as_raw_handle(self)
    }

    fn name(&self) -> &str {
        ""
    }
}

/// A [`RawInput`] that reports a caller-supplied name.
///
/// [`std::fs::File`] does not carry the path it was opened with, so its
/// [`name`](RawInput::name) is empty and the BSD `/dev/tty` branch never fires.
/// Wrap the file with [`named`] to give the backend the path it needs.
pub struct Named<R> {
    inner: R,
    name: String,
}

impl<R: Read> Read for Named<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<R: RawInput> RawInput for Named<R> {
    fn raw(&self) -> RawDescriptor {
        self.inner.raw()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Tag a reader with a name so the `/dev/tty` fast path can find it.
///
/// On macOS and the BSDs, kqueue returns ready at once when it watches
/// `/dev/tty`, so the backend routes a reader named `/dev/tty` to select
/// instead. A terminal opened as a plain [`std::fs::File`] reports no name and
/// misses that route. Wrap it:
///
/// ```no_run
/// use cancelreader::{named, new_reader};
///
/// let tty = std::fs::File::open("/dev/tty")?;
/// let reader = new_reader(named(tty, "/dev/tty"))?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn named<R: RawInput>(reader: R, name: impl Into<String>) -> Named<R> {
    Named {
        inner: reader,
        name: name.into(),
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

    /// Set the flag and report whether this call was the one that set it.
    ///
    /// Returns `true` only on the false-to-true transition, so a caller can act
    /// once no matter how many times cancel runs.
    fn set_canceled(&self) -> bool {
        !self.canceled.swap(true, Ordering::SeqCst)
    }
}

/// A handle that cancels a reader from another thread.
///
/// Obtain one with [`CancelReader::canceler`]. Calling [`cancel`](Canceler::cancel)
/// has the same effect and return value as [`CancelReader::cancel`].
pub struct Canceler {
    inner: CancelerInner,
}

impl fmt::Debug for Canceler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Canceler").finish_non_exhaustive()
    }
}

/// Clone shares the same cancellation state. Every clone cancels one reader.
impl Clone for Canceler {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            CancelerInner::Fallback(flag) => CancelerInner::Fallback(Arc::clone(flag)),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly",
                target_os = "solaris",
            ))]
            CancelerInner::Unix(state) => CancelerInner::Unix(Arc::clone(state)),
            #[cfg(windows)]
            CancelerInner::Windows(state) => CancelerInner::Windows(Arc::clone(state)),
        };
        Canceler { inner }
    }
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
/// The returned reader implements [`CancelReader`]. Platform backends use the
/// reader's raw descriptor so [`cancel`](CancelReader::cancel) can interrupt a
/// blocked read.
///
/// # Errors
///
/// Returns an error if a backend fails to set up its readiness primitive or
/// self-pipe.
pub fn new_reader<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
where
    R: RawInput + Send + 'static,
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
/// This builds the no-op fallback backend. `cancel` returns `false` and cannot
/// interrupt an in-flight read. After a `cancel`, future reads return
/// [`Canceled`] and consume no data.
pub fn new_reader_plain<R>(reader: R) -> Box<dyn CancelReader + Send>
where
    R: Read + Send + 'static,
{
    fallback::new_fallback_cancel_reader(reader)
}
