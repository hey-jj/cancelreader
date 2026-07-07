//! The real Unix backend end to end: epoll, kqueue, or select over a pipe.
//!
//! Drives the self-pipe and readiness wait, not the fallback. A cancel must
//! unblock a blocked read, consume no data, and leave the bytes readable.

#![cfg(unix)]

use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

use cancelreader::new_reader;

mod common;
use common::{is_canceled, make_pipe, CANCEL_TIMEOUT, MSG};

#[test]
fn reader() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr.try_clone().unwrap()).expect("expected no error");

    let n = pw.write(MSG).expect("expected no error");
    assert_eq!(n, 5, "expected 5 bytes written");

    // Cancel while a read blocks on the one byte buffer.
    let canceler = cr.canceler();
    let (tx, rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [0u8; 1];
        let result = std::io::Read::read(&mut cr, &mut buf);
        tx.send(result).unwrap();
    });

    assert!(
        canceler.cancel(),
        "expected cancellation to succeed on a file reader"
    );

    let result = rx
        .recv_timeout(CANCEL_TIMEOUT)
        .expect("expected cancellation to unblock the reader");
    handle.join().unwrap();

    match result {
        Ok(n) => panic!("expected a cancellation, got {n} bytes"),
        Err(err) => {
            assert!(is_canceled(&err), "expected a cancellation, got {err}");
        }
    }

    // The canceled read consumed nothing, so a fresh reader on the same pipe
    // still sees the buffered bytes.
    let mut cr = new_reader(pr.try_clone().unwrap()).expect("expected no error");
    let mut buf = [0u8; 5];
    let n = std::io::Read::read(&mut cr, &mut buf).expect("expected no error");
    assert_eq!(n, 5, "expected 5 bytes read");
    assert_eq!(&buf[..n], &MSG[..n], "expected to read the written bytes");

    drop(pw);
    drop(pr);
}

/// A second read on the same canceled reader returns a cancellation at once.
#[test]
fn read_after_cancel_is_empty() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let mut cr = new_reader(pr).expect("expected no error");
    pw.write(MSG).expect("expected no error");

    let canceler = cr.canceler();
    assert!(canceler.cancel(), "expected cancellation to succeed");

    let mut buf = [0u8; 1];
    let err = std::io::Read::read(&mut cr, &mut buf).expect_err("expected a cancellation");
    assert!(
        is_canceled(&err),
        "first read after cancel should be a cancellation"
    );

    let err = std::io::Read::read(&mut cr, &mut buf).expect_err("expected a cancellation");
    assert!(
        is_canceled(&err),
        "second read after cancel should be a cancellation"
    );

    drop(pw);
}

/// Calling cancel twice on a file reader stays stable.
#[test]
fn cancel_idempotent_file() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr).expect("expected no error");

    assert!(cr.cancel(), "first cancel should succeed");
    // A second cancel writes another wake byte. It must not panic and should
    // still report success on a healthy pipe.
    let _ = cr.cancel();

    drop(cr);
    drop(pw);
}

/// Repeated create, cancel, and drop cycles do not leak descriptors.
#[test]
fn no_fd_leak() {
    for _ in 0..2000 {
        let (pr, pw) = make_pipe().expect("expected no error");
        let cr = new_reader(pr).expect("expected no error");
        cr.cancel();
        drop(cr);
        drop(pw);
    }
}

/// One reader thread and several cancel threads end in a cancellation with no
/// data race.
#[test]
fn thread_safety_stress() {
    use std::sync::mpsc::sync_channel;
    use std::sync::{Arc, Barrier};

    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr).expect("expected no error");
    let canceler = cr.canceler();

    let barrier = Arc::new(Barrier::new(5));
    let mut cancelers = Vec::new();
    for _ in 0..4 {
        let c = canceler.clone();
        let b = Arc::clone(&barrier);
        cancelers.push(thread::spawn(move || {
            b.wait();
            c.cancel();
        }));
    }

    let (tx, rx) = sync_channel(1);
    let reader = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [0u8; 1];
        tx.send(std::io::Read::read(&mut cr, &mut buf)).unwrap();
    });

    barrier.wait();
    let result = rx.recv().expect("read thread finished");
    reader.join().unwrap();
    for c in cancelers {
        c.join().unwrap();
    }

    match result {
        Ok(n) => panic!("expected a cancellation, got {n} bytes"),
        Err(err) => assert!(is_canceled(&err), "expected a cancellation, got {err}"),
    }

    drop(pw);
}

/// The select backend refuses a descriptor at or above the select size limit
/// and falls back, so its cancel reports failure.
///
/// The `/dev/tty` name routes to select. macOS and the BSDs cap the fd there;
/// on Linux the epoll backend has no such limit, so this is gated to the
/// select platforms.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
    target_os = "solaris",
))]
#[test]
fn select_fd_setsize_guard() {
    let (pr, pw) = make_pipe().expect("expected no error");

    // Move the read end above the select limit and name it so it routes to the
    // select backend.
    let high = pr
        .dup_min_fd(1100)
        .expect("expected a high descriptor")
        .with_name("/dev/tty");

    let cr = new_reader(high).expect("expected no error");
    assert!(
        !cr.cancel(),
        "a descriptor above the select limit should fall back to the no-op reader"
    );

    drop(cr);
    drop(pw);
    drop(pr);
}

/// A read that is already blocked in the readiness wait unblocks on cancel.
///
/// The `reader` test cancels right after spawning, so the read can satisfy its
/// top-of-read short circuit without ever blocking in `wait`. This test writes
/// no data and sleeps before canceling, so the read is genuinely parked in the
/// wait. The cancel must wake it, drain the one signal byte, and return a
/// cancellation. Draining matters: a fresh reader on the same pipe must not see
/// a stale ready state.
#[test]
fn wait_then_drain_cancel() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr.try_clone().unwrap()).expect("expected no error");
    let canceler = cr.canceler();

    let (started_tx, started_rx) = sync_channel(1);
    let (tx, rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [0u8; 1];
        started_tx.send(()).unwrap();
        tx.send(std::io::Read::read(&mut cr, &mut buf)).unwrap();
    });

    // Wait for the thread to enter read, then give it time to park in wait.
    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));

    assert!(canceler.cancel(), "cancel on a blocked read should succeed");

    let result = rx
        .recv_timeout(CANCEL_TIMEOUT)
        .expect("cancel should unblock the blocked read");
    handle.join().unwrap();

    match result {
        Ok(n) => panic!("expected a cancellation, got {n} bytes"),
        Err(err) => assert!(is_canceled(&err), "expected a cancellation, got {err}"),
    }

    // The signal byte was drained, so a new reader on the same pipe blocks on a
    // real read rather than returning at once. Write, then read it back.
    let mut fresh = new_reader(pr.try_clone().unwrap()).expect("expected no error");
    pw.write(MSG).expect("expected no error");
    let mut buf = [0u8; 5];
    let n = std::io::Read::read(&mut fresh, &mut buf).expect("expected no error");
    assert_eq!(n, 5, "expected the written bytes after a clean drain");
    assert_eq!(&buf[..n], MSG);

    drop(pw);
    drop(pr);
}

/// A `/dev/tty`-named reader on the select platforms cancels end to end.
///
/// The name routes the reader to the select backend instead of kqueue. A pipe
/// renamed to `/dev/tty` reaches that path without a real terminal. The read
/// blocks in select, stays blocked while there is no data, and unblocks only on
/// cancel. If the routing were wrong or select spun, the read would return
/// before the cancel.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
))]
#[test]
fn select_cancel_end_to_end() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let tty = pr.try_clone().unwrap().with_name("/dev/tty");
    let cr = new_reader(tty).expect("expected no error");
    let canceler = cr.canceler();

    let (started_tx, started_rx) = sync_channel(1);
    let (tx, rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [0u8; 1];
        started_tx.send(()).unwrap();
        tx.send(std::io::Read::read(&mut cr, &mut buf)).unwrap();
    });

    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));

    // The read has no data and must still be blocked, not spinning or returned.
    assert!(
        rx.try_recv().is_err(),
        "the select read returned before cancel, which means it spun"
    );

    assert!(canceler.cancel(), "select cancel should succeed");

    let result = rx
        .recv_timeout(CANCEL_TIMEOUT)
        .expect("select cancel should unblock the read");
    handle.join().unwrap();

    match result {
        Ok(n) => panic!("expected a cancellation, got {n} bytes"),
        Err(err) => assert!(is_canceled(&err), "expected a cancellation, got {err}"),
    }

    drop(pw);
    drop(pr);
}

/// The public `named` wrapper drives the `/dev/tty` select route.
///
/// A `std::fs::File` reports an empty name, so wrapping is the supported way to
/// reach the select fast path. This wraps a pipe with `named(reader, "/dev/tty")`
/// and confirms the read blocks in select and cancels, the same path a real
/// terminal file would take once named.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
))]
#[test]
fn named_dev_tty_routes_to_select() {
    use cancelreader::named;

    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(named(pr.try_clone().unwrap(), "/dev/tty")).expect("expected no error");
    let canceler = cr.canceler();

    let (started_tx, started_rx) = sync_channel(1);
    let (tx, rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [0u8; 1];
        started_tx.send(()).unwrap();
        tx.send(std::io::Read::read(&mut cr, &mut buf)).unwrap();
    });

    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(
        rx.try_recv().is_err(),
        "the named /dev/tty read returned before cancel, which means it spun"
    );

    assert!(canceler.cancel(), "named /dev/tty cancel should succeed");
    let result = rx
        .recv_timeout(CANCEL_TIMEOUT)
        .expect("named /dev/tty cancel should unblock the read");
    handle.join().unwrap();

    match result {
        Ok(n) => panic!("expected a cancellation, got {n} bytes"),
        Err(err) => assert!(is_canceled(&err), "expected a cancellation, got {err}"),
    }

    drop(pw);
    drop(pr);
}

/// Select falls back when the cancel pipe read end is above the select limit.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
    target_os = "solaris",
))]
#[test]
fn select_cancel_pipe_fd_setsize_guard() {
    use cancelreader::named;
    use std::fs::File;
    use std::io::Read;
    use std::os::fd::AsRawFd;

    let (pr, pw) = make_pipe().expect("expected no error");
    let input = named(pr.try_clone().unwrap(), "/dev/tty");
    pw.write(b"x").expect("expected no error");

    let mut fillers = Vec::new();
    loop {
        let file = File::open("/dev/null").expect("expected to open /dev/null");
        let fd = file.as_raw_fd();
        fillers.push(file);
        if fd > 1100 {
            break;
        }
        assert!(
            fillers.len() < 2048,
            "expected to raise the next descriptor above the select limit"
        );
    }

    let mut cr = new_reader(input).expect("expected no error");
    let mut buf = [0u8; 1];
    let n = cr
        .read(&mut buf)
        .expect("ready input should read through the fallback");
    assert_eq!(n, 1, "expected one byte read");
    assert_eq!(buf[0], b'x', "expected the written byte");

    drop(fillers);
    drop(pw);
    drop(pr);
}

/// A zero-length read on an active Unix reader returns without waiting.
#[test]
fn active_reader_zero_length_read_returns_zero() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr).expect("expected no error");

    let (tx, rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [];
        let _ = tx.send(std::io::Read::read(&mut cr, &mut buf));
    });

    let result = rx
        .recv_timeout(Duration::from_millis(200))
        .expect("zero-length read should not block");
    handle.join().unwrap();

    assert_eq!(result.expect("expected no error"), 0);
    drop(pw);
}

/// Close on a file backend reports success and stays safe on a second call.
#[test]
fn close_reports_and_is_idempotent() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let mut cr = new_reader(pr).expect("expected no error");

    cr.close().expect("first close should report success");
    // A second close must not double close the descriptors or panic.
    cr.close().expect("second close should stay Ok");

    drop(cr);
    drop(pw);
}

/// Many cancels never block, even past the pipe's byte capacity.
///
/// Each cancel used to write one wake byte to a blocking pipe. Enough calls
/// filled the pipe and wedged cancel inside the write, in the very path meant to
/// unblock a read. Cancel must now write only on the first call and return at
/// once thereafter. A default pipe holds 65536 bytes, so this loops past that.
#[test]
fn cancel_many_times_never_blocks() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr).expect("expected no error");
    let canceler = cr.canceler();

    let (done_tx, done_rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        for _ in 0..100_000 {
            canceler.cancel();
        }
        done_tx.send(()).unwrap();
    });

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("repeated cancel should never block");
    handle.join().unwrap();

    drop(cr);
    drop(pw);
}

/// Read and cancel after close return errors instead of panicking.
///
/// Both `read` and `cancel` live on the same trait object as `close`, so this
/// call order is valid public API. It used to abort through an internal
/// `expect` once close took the shared state.
#[test]
fn read_and_cancel_after_close_do_not_panic() {
    use std::io::Read;

    let (pr, pw) = make_pipe().expect("expected no error");
    let mut cr = new_reader(pr).expect("expected no error");
    cr.close().expect("close should report success");

    let mut buf = [0u8; 1];
    cr.read(&mut buf)
        .expect_err("read after close should error");
    assert!(!cr.cancel(), "cancel after close should report failure");

    drop(cr);
    drop(pw);
}

/// A cancel wins even when input arrives at the same time.
///
/// The read is parked with no data. Input and cancel then land together, so the
/// read must return a cancellation and consume none of the input, leaving the
/// bytes for a fresh reader.
#[test]
fn cancel_wins_when_input_also_ready() {
    let (pr, pw) = make_pipe().expect("expected no error");
    let cr = new_reader(pr.try_clone().unwrap()).expect("expected no error");
    let canceler = cr.canceler();

    let (started_tx, started_rx) = sync_channel(1);
    let (tx, rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let mut cr = cr;
        let mut buf = [0u8; 1];
        started_tx.send(()).unwrap();
        tx.send(std::io::Read::read(&mut cr, &mut buf)).unwrap();
    });

    // Let the read park with no data, then deliver input and cancel together.
    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    pw.write(MSG).expect("expected no error");
    assert!(canceler.cancel(), "cancel should succeed");

    let result = rx
        .recv_timeout(CANCEL_TIMEOUT)
        .expect("cancel should unblock the read");
    handle.join().unwrap();

    match result {
        Ok(n) => panic!("expected a cancellation, got {n} bytes"),
        Err(err) => assert!(is_canceled(&err), "expected a cancellation, got {err}"),
    }

    // The read consumed nothing, so the buffered bytes are still readable.
    let mut fresh = new_reader(pr.try_clone().unwrap()).expect("expected no error");
    let mut buf = [0u8; 5];
    let n = std::io::Read::read(&mut fresh, &mut buf).expect("expected no error");
    assert_eq!(n, 5, "the cancel must not have consumed the buffered bytes");
    assert_eq!(&buf[..n], MSG);

    drop(pw);
    drop(pr);
}
