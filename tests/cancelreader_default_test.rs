//! The real Unix backend end to end: epoll, kqueue, or select over a pipe.
//!
//! Drives the self-pipe and readiness wait, not the fallback. A cancel must
//! unblock a blocked read, consume no data, and leave the bytes readable.

#![cfg(unix)]

use std::sync::mpsc::sync_channel;
use std::thread;

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
