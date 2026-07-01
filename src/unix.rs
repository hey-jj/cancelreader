//! Unix backends: epoll on Linux, kqueue on the BSDs and macOS, select on
//! Solaris.
//!
//! Each backend registers the input file descriptor and the read end of a
//! self-pipe with a readiness primitive. A read blocks until one is ready. When
//! the cancel pipe fires, the read drains one byte and returns [`ErrCanceled`].
//! When the input fires, the read performs the real syscall, which no longer
//! blocks because data is ready.
//!
//! `cancel` writes one byte to the pipe writer to wake the wait. The cancel
//! state lives behind an [`Arc`] so another thread can cancel a blocked read.

use std::io::{self, Read};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::Arc;

use rustix::pipe::pipe;

use crate::{CancelFlag, CancelReader, Canceler, CancelerInner, ErrCanceled, File};

/// The largest file descriptor the select backend can watch.
///
/// `select` works on a fixed-size bitset. Descriptors at or above this bound do
/// not fit, so the select backend refuses them and falls back.
const FD_SETSIZE: RawFd = libc::FD_SETSIZE as RawFd;

/// Shared cancellation state.
///
/// Holds the cancel flag and the pipe writer. `cancel` touches only this, so it
/// can run on any thread while a read blocks. Wrapped in an [`Arc`] and cloned
/// into the reader.
pub(crate) struct CancelState {
    flag: CancelFlag,
    writer: OwnedFd,
}

impl CancelState {
    pub(crate) fn cancel(&self) -> bool {
        self.flag.set_canceled();
        rustix::io::write(&self.writer, b"c").is_ok()
    }
}

/// The input, holding its file descriptor open.
///
/// `_owner` keeps the wrapped [`File`] alive so `fd` stays valid. Reads go
/// straight to the descriptor with `read`, which matches the source performing
/// `file.Read` on a real file after the wait.
struct Input {
    _owner: Box<dyn File + Send>,
    fd: RawFd,
}

impl Input {
    fn borrowed(&self) -> BorrowedFd<'_> {
        // Safety: `_owner` keeps the descriptor open for `self`'s lifetime.
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

/// Drain one byte from the cancel pipe after a cancel wins the wait.
///
/// Leaving the byte would make the next wait return at once.
fn drain_cancel(reader: BorrowedFd<'_>) -> io::Result<()> {
    let mut b = [0u8; 1];
    rustix::io::read(reader, &mut b).map_err(io::Error::from)?;
    Ok(())
}

/// Build the Unix backend for a reader.
///
/// Picks epoll, kqueue, or select by target. Non-file readers and, for select,
/// descriptors at or above [`FD_SETSIZE`] route to the fallback in the caller,
/// but here the input is already a [`File`], so only the select size guard can
/// still fall back.
pub fn new_reader<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
where
    R: File + Send + 'static,
{
    #[cfg(target_os = "linux")]
    {
        Backend::new_epoll(reader)
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))]
    {
        if reader.name() == "/dev/tty" {
            return Backend::new_select(reader);
        }
        Backend::new_kqueue(reader)
    }

    #[cfg(target_os = "solaris")]
    {
        Backend::new_select(reader)
    }
}

/// The wait primitive plus the input and the self-pipe.
struct Backend {
    input: Input,
    reader: OwnedFd,
    state: Arc<CancelState>,
    kind: Kind,
}

/// Which readiness primitive backs this reader.
enum Kind {
    #[cfg(target_os = "linux")]
    Epoll(OwnedFd),
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))]
    Kqueue(OwnedFd),
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "solaris",
    ))]
    Select,
}

impl Backend {
    fn make_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
        pipe().map_err(io::Error::from)
    }

    fn assemble(reader: OwnedFd, writer: OwnedFd, input: Input, kind: Kind) -> Self {
        Backend {
            input,
            reader,
            state: Arc::new(CancelState {
                flag: CancelFlag::default(),
                writer,
            }),
            kind,
        }
    }

    #[cfg(target_os = "linux")]
    fn new_epoll<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
    where
        R: File + Send + 'static,
    {
        use rustix::event::epoll;

        let fd = reader.raw();
        let input = Input {
            _owner: Box::new(reader),
            fd,
        };

        let epoll = epoll::create(epoll::CreateFlags::empty()).map_err(io::Error::from)?;
        let (pipe_reader, pipe_writer) = Self::make_pipe()?;

        epoll::add(
            &epoll,
            input.borrowed(),
            epoll::EventData::new_u64(fd as u64),
            epoll::EventFlags::IN,
        )
        .map_err(|_| io::Error::other("add reader to epoll interest list"))?;

        epoll::add(
            &epoll,
            pipe_reader.as_fd(),
            epoll::EventData::new_u64(rustix::fd::AsRawFd::as_raw_fd(&pipe_reader) as u64),
            epoll::EventFlags::IN,
        )
        .map_err(|_| io::Error::other("add reader to epoll interest list"))?;

        Ok(Box::new(Self::assemble(
            pipe_reader,
            pipe_writer,
            input,
            Kind::Epoll(epoll),
        )))
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))]
    fn new_kqueue<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
    where
        R: File + Send + 'static,
    {
        use rustix::event::kqueue::kqueue;

        let fd = reader.raw();
        let input = Input {
            _owner: Box::new(reader),
            fd,
        };

        let kq = kqueue().map_err(io::Error::from)?;
        let (pipe_reader, pipe_writer) = Self::make_pipe()?;

        Ok(Box::new(Self::assemble(
            pipe_reader,
            pipe_writer,
            input,
            Kind::Kqueue(kq),
        )))
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "solaris",
    ))]
    fn new_select<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
    where
        R: File + Send + 'static,
    {
        let fd = reader.raw();
        if fd >= FD_SETSIZE {
            return Ok(crate::fallback::new_fallback_cancel_reader(reader));
        }

        let input = Input {
            _owner: Box::new(reader),
            fd,
        };
        let (pipe_reader, pipe_writer) = Self::make_pipe()?;

        Ok(Box::new(Self::assemble(
            pipe_reader,
            pipe_writer,
            input,
            Kind::Select,
        )))
    }

    /// Block until the input or the cancel pipe is ready.
    ///
    /// Returns `Ok(())` when the input is ready, `Err(ErrCanceled)` when the
    /// cancel pipe fired, and other errors on wait failure.
    fn wait(&self) -> Result<(), WaitError> {
        match &self.kind {
            #[cfg(target_os = "linux")]
            Kind::Epoll(epoll) => self.wait_epoll(epoll),
            #[cfg(any(
                target_os = "macos",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly",
            ))]
            Kind::Kqueue(kq) => self.wait_kqueue(kq),
            #[cfg(any(
                target_os = "macos",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly",
                target_os = "solaris",
            ))]
            Kind::Select => self.wait_select(),
        }
    }

    #[cfg(target_os = "linux")]
    fn wait_epoll(&self, epoll: &OwnedFd) -> Result<(), WaitError> {
        use rustix::buffer::spare_capacity;
        use rustix::event::epoll;
        use rustix::fd::AsRawFd;

        let mut events: Vec<epoll::Event> = Vec::with_capacity(1);
        loop {
            match epoll::wait(epoll, spare_capacity(&mut events), None) {
                Ok(_) => break,
                Err(rustix::io::Errno::INTR) => {
                    events.clear();
                    continue;
                }
                Err(err) => return Err(WaitError::Io(io::Error::from(err))),
            }
        }

        let cancel_fd = self.reader.as_raw_fd() as u64;
        match events.first() {
            Some(event) if event.data.u64() == cancel_fd => Err(WaitError::Canceled),
            Some(_) => Ok(()),
            None => Err(WaitError::Unknown),
        }
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))]
    fn wait_kqueue(&self, kq: &OwnedFd) -> Result<(), WaitError> {
        use rustix::buffer::spare_capacity;
        use rustix::event::kqueue::{kevent, Event, EventFilter, EventFlags};
        use rustix::fd::AsRawFd;

        let changes = [
            Event::new(
                EventFilter::Read(self.input.fd),
                EventFlags::ADD,
                std::ptr::null_mut(),
            ),
            Event::new(
                EventFilter::Read(self.reader.as_raw_fd()),
                EventFlags::ADD,
                std::ptr::null_mut(),
            ),
        ];

        let mut events: Vec<Event> = Vec::with_capacity(1);
        loop {
            // Safety: the changelist and event buffer outlive the call, and the
            // descriptors stay open for the reader's lifetime.
            match unsafe { kevent(kq, &changes, spare_capacity(&mut events), None) } {
                Ok(_) => break,
                Err(rustix::io::Errno::INTR) => {
                    events.clear();
                    continue;
                }
                Err(err) => return Err(WaitError::Io(io::Error::from(err))),
            }
        }

        let cancel_fd = self.reader.as_raw_fd();
        match events.first().map(|e| e.filter()) {
            Some(EventFilter::Read(fd)) if fd == cancel_fd => Err(WaitError::Canceled),
            Some(_) => Ok(()),
            None => Err(WaitError::Unknown),
        }
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "solaris",
    ))]
    fn wait_select(&self) -> Result<(), WaitError> {
        use rustix::event::{fd_set_insert, fd_set_num_elements, select, FdSetElement, FdSetIter};
        use rustix::fd::AsRawFd;

        let reader_fd = self.input.fd;
        let abort_fd = self.reader.as_raw_fd();
        let max_fd = reader_fd.max(abort_fd);

        if max_fd >= FD_SETSIZE {
            return Err(WaitError::Io(io::Error::other(format!(
                "cannot select on file descriptor {max_fd} which is larger than 1024"
            ))));
        }

        let nfds = max_fd + 1;
        let num = fd_set_num_elements(2, nfds);
        let mut readfds = vec![FdSetElement::default(); num];

        loop {
            fd_set_insert(&mut readfds, reader_fd);
            fd_set_insert(&mut readfds, abort_fd);

            // Safety: `readfds` is sized for `nfds` descriptors and outlives the
            // call. No write or except sets are used.
            match unsafe { select(nfds, Some(&mut readfds), None, None, None) } {
                Ok(_) => break,
                Err(rustix::io::Errno::INTR) => {
                    readfds
                        .iter_mut()
                        .for_each(|e| *e = FdSetElement::default());
                    continue;
                }
                Err(err) => return Err(WaitError::Io(io::Error::from(err))),
            }
        }

        let ready: Vec<RawFd> = FdSetIter::new(&readfds).collect();
        if ready.contains(&abort_fd) {
            return Err(WaitError::Canceled);
        }
        if ready.contains(&reader_fd) {
            return Ok(());
        }
        Err(WaitError::Unknown)
    }

    /// Perform one cancelable read.
    ///
    /// Shared over `&self` so the read thread and a canceling thread can hold
    /// the same reader. `Read::read` delegates here.
    fn read_shared(&self, data: &mut [u8]) -> io::Result<usize> {
        if self.state.flag.is_canceled() {
            return Err(ErrCanceled.into());
        }

        match self.wait() {
            Ok(()) => rustix::io::read(self.input.borrowed(), data).map_err(io::Error::from),
            Err(WaitError::Canceled) => {
                drain_cancel(self.reader.as_fd())
                    .map_err(|e| io::Error::other(format!("reading cancel signal: {e}")))?;
                Err(ErrCanceled.into())
            }
            Err(WaitError::Io(err)) => Err(err),
            Err(WaitError::Unknown) => Err(io::Error::other("unknown error")),
        }
    }
}

/// Errors from a readiness wait.
enum WaitError {
    /// The cancel pipe fired.
    Canceled,
    /// The wait reported no descriptor.
    Unknown,
    /// A wait syscall failed.
    Io(io::Error),
}

impl Read for Backend {
    fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
        self.read_shared(data)
    }
}

impl CancelReader for Backend {
    fn cancel(&self) -> bool {
        self.state.cancel()
    }

    fn canceler(&self) -> Canceler {
        Canceler {
            inner: CancelerInner::Unix(Arc::clone(&self.state)),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        // Dropping the owned descriptors closes them. Nothing to surface here.
        Ok(())
    }
}
