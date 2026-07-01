//! Windows backend.
//!
//! Opens `CONIN$` in overlapped mode, sets a raw console mode, and waits on the
//! console handle plus a cancel event with `WaitForMultipleObjects`. A read
//! that wins the wait runs an overlapped `ReadFile` and blocks in
//! `GetOverlappedResult`. `cancel` sets the event to wake the wait.
//!
//! Only a reader that shares stdin's handle is cancelable. Everything else uses
//! the fallback.
//!
//! `cancel` and the async read rendezvous through a capacity-one channel. The
//! read fills the slot right before it blocks. `cancel` competes for the slot,
//! so it proceeds once the read is blocked or after 100 milliseconds. A read
//! wedged in `GetOverlappedResult` never frees the slot, so `cancel` times out
//! and returns `false`.

use std::io::{self, Read};
use std::os::windows::io::AsRawHandle;
use std::ptr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_IO_PENDING, GENERIC_READ, GENERIC_WRITE, HANDLE,
    WAIT_ABANDONED, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, ReadFile, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
};
use windows_sys::Win32::System::Console::{
    FlushConsoleInputBuffer, GetConsoleMode, SetConsoleMode, ENABLE_EXTENDED_FLAGS,
    ENABLE_INSERT_MODE, ENABLE_QUICK_EDIT_MODE, ENABLE_VIRTUAL_TERMINAL_INPUT,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, SetEvent, WaitForMultipleObjects, INFINITE,
};
use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};

use crate::{CancelFlag, CancelReader, Canceled, Canceler, CancelerInner, RawInput};

/// FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE.
const FILE_SHARE_VALID_FLAGS: u32 = 0x0000_0007;

/// A capacity-one rendezvous channel.
///
/// Models the buffered channel the source uses to coordinate the async read and
/// `cancel`. `send` blocks while the slot is full. `recv` blocks while it is
/// empty. `send_timeout` gives `cancel` its 100 millisecond bound.
struct Chan1 {
    state: Mutex<bool>,
    not_full: Condvar,
    not_empty: Condvar,
}

impl Chan1 {
    fn new() -> Self {
        Chan1 {
            state: Mutex::new(false),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        }
    }

    fn send(&self) {
        let mut full = self.state.lock().unwrap();
        while *full {
            full = self.not_full.wait(full).unwrap();
        }
        *full = true;
        self.not_empty.notify_one();
    }

    fn send_timeout(&self, timeout: Duration) -> bool {
        let mut full = self.state.lock().unwrap();
        while *full {
            let (guard, result) = self.not_full.wait_timeout(full, timeout).unwrap();
            full = guard;
            if result.timed_out() && *full {
                return false;
            }
        }
        *full = true;
        self.not_empty.notify_one();
        true
    }

    fn recv(&self) {
        let mut full = self.state.lock().unwrap();
        while !*full {
            full = self.not_empty.wait(full).unwrap();
        }
        *full = false;
        self.not_full.notify_one();
    }
}

/// A Windows handle wrapper that is safe to move and share.
struct Handle(HANDLE);

// Safety: the handles are console and event objects owned by this reader. They
// are only used through synchronized methods.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

/// Shared cancellation state.
///
/// Holds the cancel flag, the cancel event handle, and the rendezvous channel.
/// `cancel` uses only this, so it can run on another thread.
pub(crate) struct CancelState {
    flag: CancelFlag,
    cancel_event: Handle,
    signal: Chan1,
}

impl CancelState {
    pub(crate) fn cancel(&self) -> bool {
        self.flag.set_canceled();

        if !self.signal.send_timeout(Duration::from_millis(100)) {
            // The read is wedged in GetOverlappedResult after a spurious wake.
            // It cannot be canceled.
            return false;
        }

        // Safety: `cancel_event` is a valid event handle for the reader's life.
        let ok = unsafe { SetEvent(self.cancel_event.0) };
        if ok == 0 {
            return false;
        }
        self.signal.recv();
        true
    }
}

/// The Windows reader.
struct WinCancelReader {
    conin: Handle,
    reset_console: Option<ConsoleReset>,
    state: Arc<CancelState>,
    closed: bool,
}

/// Restores the original console mode on close or drop.
struct ConsoleReset {
    input: Handle,
    original_mode: u32,
}

impl ConsoleReset {
    fn reset(&self) -> io::Result<()> {
        // Safety: `input` is the console handle opened by this reader.
        let ok = unsafe { SetConsoleMode(self.input.0, self.original_mode) };
        if ok == 0 {
            return Err(io::Error::other(format!(
                "reset console mode: {}",
                last_error()
            )));
        }
        Ok(())
    }
}

fn last_error() -> io::Error {
    // Safety: GetLastError has no preconditions.
    let code = unsafe { GetLastError() };
    io::Error::from_raw_os_error(code as i32)
}

/// Build the Windows backend for a reader.
///
/// Falls back unless the reader shares stdin's handle.
pub fn new_reader<R>(reader: R) -> io::Result<Box<dyn CancelReader + Send>>
where
    R: RawInput + Send + 'static,
{
    let stdin = io::stdin().as_raw_handle();
    if reader.raw() != stdin {
        return Ok(crate::fallback::new_fallback_cancel_reader(reader));
    }

    let name: Vec<u16> = "CONIN$\0".encode_utf16().collect();

    // Safety: `name` is a valid null-terminated wide string. The other
    // arguments are constants or null.
    let conin = unsafe {
        CreateFileW(
            name.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_VALID_FLAGS,
            ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_OVERLAPPED,
            ptr::null_mut(),
        )
    };
    if conin.is_null() || conin == -1isize as HANDLE {
        return Err(io::Error::other(format!(
            "open CONIN$ in overlapping mode: {}",
            last_error()
        )));
    }
    let conin = Handle(conin);

    let reset_console =
        prepare_console(conin.0).map_err(|e| io::Error::other(format!("prepare console: {e}")))?;

    // Safety: `conin.0` is the console handle just opened.
    let flushed = unsafe { FlushConsoleInputBuffer(conin.0) };
    if flushed == 0 {
        return Err(io::Error::other(format!(
            "flush console input buffer: {}",
            last_error()
        )));
    }

    // Safety: all arguments are null or zero, forming an auto-reset,
    // non-signaled, unnamed event.
    let cancel_event = unsafe { CreateEventW(ptr::null(), 0, 0, ptr::null()) };
    if cancel_event.is_null() {
        return Err(io::Error::other(format!(
            "create stop event: {}",
            last_error()
        )));
    }

    Ok(Box::new(WinCancelReader {
        conin,
        reset_console: Some(reset_console),
        state: Arc::new(CancelState {
            flag: CancelFlag::default(),
            cancel_event: Handle(cancel_event),
            signal: Chan1::new(),
        }),
        closed: false,
    }))
}

/// Set the console to raw mode and return a handle that restores it.
fn prepare_console(input: HANDLE) -> io::Result<ConsoleReset> {
    let mut original_mode: u32 = 0;
    // Safety: `input` is a valid console handle and `original_mode` is a valid
    // out pointer.
    let ok = unsafe { GetConsoleMode(input, &mut original_mode) };
    if ok == 0 {
        return Err(io::Error::other(format!(
            "get console mode: {}",
            last_error()
        )));
    }

    // The source builds this mode from a zero value, so only the set flags
    // matter.
    let new_mode = ENABLE_EXTENDED_FLAGS
        | ENABLE_INSERT_MODE
        | ENABLE_QUICK_EDIT_MODE
        | ENABLE_VIRTUAL_TERMINAL_INPUT;

    // Safety: `input` is a valid console handle.
    let ok = unsafe { SetConsoleMode(input, new_mode) };
    if ok == 0 {
        return Err(io::Error::other(format!(
            "set console mode: {}",
            last_error()
        )));
    }

    Ok(ConsoleReset {
        input: Handle(input),
        original_mode,
    })
}

impl WinCancelReader {
    /// Block until the console or the cancel event is ready.
    fn wait(&self) -> io::Result<WaitOutcome> {
        let handles = [self.conin.0, self.state.cancel_event.0];
        // Safety: both handles are valid for the reader's life.
        let event = unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) };

        if (WAIT_OBJECT_0..WAIT_OBJECT_0 + 2).contains(&event) {
            if event == WAIT_OBJECT_0 + 1 {
                return Ok(WaitOutcome::Canceled);
            }
            if event == WAIT_OBJECT_0 {
                return Ok(WaitOutcome::Ready);
            }
            return Err(io::Error::other(format!(
                "unexpected wait object is ready: {}",
                event - WAIT_OBJECT_0
            )));
        }
        if (WAIT_ABANDONED..WAIT_ABANDONED + 2).contains(&event) {
            return Err(io::Error::other("abandoned"));
        }
        if event == WAIT_TIMEOUT {
            return Err(io::Error::other("timeout"));
        }
        if event == WAIT_FAILED {
            return Err(io::Error::other("failed"));
        }
        Err(io::Error::other(format!(
            "unexpected error: {}",
            last_error()
        )))
    }

    /// Perform one overlapped read from the console.
    ///
    /// Fills the rendezvous slot right before blocking in `GetOverlappedResult`.
    /// On a `GetOverlappedResult` error the slot stays filled, which is what
    /// makes a competing `cancel` time out.
    fn read_async(&self, data: &mut [u8]) -> io::Result<usize> {
        // Safety: null security attributes, auto-reset, non-signaled, unnamed.
        let hevent = unsafe { CreateEventW(ptr::null(), 0, 0, ptr::null()) };
        if hevent.is_null() {
            return Err(io::Error::other(format!("create event: {}", last_error())));
        }

        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.hEvent = hevent;

        let mut n: u32 = 0;
        // Safety: `data` is a valid buffer and `overlapped` outlives the call.
        let ok = unsafe {
            ReadFile(
                self.conin.0,
                data.as_mut_ptr(),
                data.len() as u32,
                &mut n,
                &mut overlapped,
            )
        };
        if ok == 0 {
            // Safety: no preconditions.
            let err = unsafe { GetLastError() };
            if err != ERROR_IO_PENDING {
                unsafe { CloseHandle(hevent) };
                return Err(io::Error::from_raw_os_error(err as i32));
            }
        }

        self.state.signal.send();
        // Safety: `overlapped` was used by the ReadFile above and outlives this.
        let done = unsafe { GetOverlappedResult(self.conin.0, &overlapped, &mut n, 1) };
        // Safety: `hevent` was created above and is no longer used.
        unsafe { CloseHandle(hevent) };
        if done == 0 {
            // The source swallows this error and leaves the slot filled.
            return Ok(n as usize);
        }
        self.state.signal.recv();

        Ok(n as usize)
    }
}

/// The outcome of a readiness wait.
enum WaitOutcome {
    Ready,
    Canceled,
}

impl Read for WinCancelReader {
    fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
        if self.state.flag.is_canceled() {
            return Err(Canceled.into());
        }

        match self.wait()? {
            WaitOutcome::Canceled => return Err(Canceled.into()),
            WaitOutcome::Ready => {}
        }

        if self.state.flag.is_canceled() {
            return Err(Canceled.into());
        }

        self.read_async(data)
    }
}

impl CancelReader for WinCancelReader {
    fn cancel(&self) -> bool {
        self.state.cancel()
    }

    fn canceler(&self) -> Canceler {
        Canceler {
            inner: CancelerInner::Windows(Arc::clone(&self.state)),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        // Safety: `cancel_event` is a valid handle owned by this reader.
        let ok = unsafe { CloseHandle(self.state.cancel_event.0) };
        if ok == 0 {
            return Err(io::Error::other(format!(
                "closing cancel event handle: {}",
                last_error()
            )));
        }

        if let Some(reset) = self.reset_console.take() {
            reset.reset()?;
        }

        // Safety: `conin` is a valid handle owned by this reader.
        let ok = unsafe { CloseHandle(self.conin.0) };
        if ok == 0 {
            return Err(io::Error::other("closing CONIN$"));
        }
        Ok(())
    }
}

impl Drop for WinCancelReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
