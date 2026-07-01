//! Shared helpers for the cancelreader test suite.

#![allow(dead_code, unused_imports)]

use std::io::{self, Read};
use std::time::Duration;

/// The cancel deadline the reader tests wait on, matching the source suite.
pub const CANCEL_TIMEOUT: Duration = Duration::from_millis(100);

/// The payload written through the pipe in the reader test.
pub const MSG: &[u8] = b"hello";

/// Report whether an error came from a cancellation.
pub fn is_canceled(err: &io::Error) -> bool {
    cancelreader::is_canceled(err)
}

use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

/// A reader that blocks until told to unblock.
///
/// It signals when a read starts, blocks until unblocked, records that the read
/// ran, then returns an error that the fallback is expected to discard in favor
/// of the cancellation.
pub struct BlockingReader {
    started: Sender<()>,
    unblock: Receiver<()>,
    read: Arc<Mutex<bool>>,
}

impl BlockingReader {
    /// Build a blocking reader with the channels a test drives it through.
    pub fn new(started: Sender<()>, unblock: Receiver<()>, read: Arc<Mutex<bool>>) -> Self {
        BlockingReader {
            started,
            unblock,
            read,
        }
    }
}

impl Read for BlockingReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        self.started.send(()).unwrap();
        let _ = self.unblock.recv();
        *self.read.lock().unwrap() = true;
        Err(io::Error::other("this error should be ignored"))
    }
}

#[cfg(unix)]
mod unix_pipe {
    use super::*;
    use cancelreader::RawInput;
    use rustix::pipe::pipe;
    use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

    /// The read end of an OS pipe, exposed as a [`RawInput`].
    pub struct PipeReader {
        fd: OwnedFd,
        name: String,
    }

    /// The write end of an OS pipe.
    pub struct PipeWriter {
        fd: OwnedFd,
    }

    impl PipeReader {
        /// Override the reported name, which drives the BSD `/dev/tty` branch.
        pub fn with_name(mut self, name: &str) -> Self {
            self.name = name.to_string();
            self
        }

        pub fn borrowed(&self) -> BorrowedFd<'_> {
            self.fd.as_fd()
        }

        /// Duplicate the read end so the same pipe can back several readers.
        pub fn try_clone(&self) -> io::Result<PipeReader> {
            let fd = self.fd.try_clone()?;
            Ok(PipeReader {
                fd,
                name: self.name.clone(),
            })
        }

        /// Duplicate this read end onto a descriptor at or above `min`.
        ///
        /// Used to exercise the select backend's file descriptor size guard.
        pub fn dup_min_fd(&self, min: RawFd) -> io::Result<PipeReader> {
            let fd =
                rustix::io::fcntl_dupfd_cloexec(self.fd.as_fd(), min).map_err(io::Error::from)?;
            Ok(PipeReader {
                fd,
                name: self.name.clone(),
            })
        }
    }

    impl Read for PipeReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            rustix::io::read(self.fd.as_fd(), buf).map_err(io::Error::from)
        }
    }

    impl RawInput for PipeReader {
        fn raw(&self) -> RawFd {
            self.fd.as_raw_fd()
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    impl PipeWriter {
        /// Write all bytes to the pipe.
        pub fn write(&self, data: &[u8]) -> io::Result<usize> {
            rustix::io::write(self.fd.as_fd(), data).map_err(io::Error::from)
        }
    }

    /// Create a pipe and return its two ends as file-like wrappers.
    pub fn make_pipe() -> io::Result<(PipeReader, PipeWriter)> {
        let (reader, writer) = pipe().map_err(io::Error::from)?;
        Ok((
            PipeReader {
                fd: reader,
                name: "pipe".to_string(),
            },
            PipeWriter { fd: writer },
        ))
    }
}

#[cfg(unix)]
pub use unix_pipe::{make_pipe, PipeReader, PipeWriter};
