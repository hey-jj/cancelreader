# cancelreader

A cancelable reader. Wrap a blocking input source like stdin or a raw terminal
so another thread can abort a blocked read during shutdown. The abort consumes
no input.

The mechanism is the self-pipe trick paired with an operating system readiness
wait. A read blocks until either the input becomes readable or a cancel signal
arrives. `cancel` wakes the wait and makes the read return `ErrCanceled` instead
of data.

## Backends

Selected at compile time:

- Linux uses epoll.
- macOS and the BSDs use kqueue.
- Solaris uses the POSIX select syscall.
- Windows uses `WaitForMultipleObjects` with overlapped reads from `CONIN$`.
- Every other target uses a fallback that cannot interrupt an in-flight read.

A reader that does not expose a raw file descriptor uses the fallback. On
Windows only a reader sharing stdin's handle is cancelable.

## Usage

```rust,no_run
use std::io::Read;
use cancelreader::{new_reader, is_canceled};

let file = std::fs::File::open("/dev/tty")?;
let mut reader = new_reader(file)?;

let mut buf = [0u8; 1024];
loop {
    match reader.read(&mut buf) {
        Ok(0) => break,
        Ok(n) => { /* handle n bytes */ }
        Err(err) if is_canceled(&err) => {
            println!("canceled");
            break;
        }
        Err(err) => return Err(err),
    }
}
# Ok::<(), std::io::Error>(())
```

To cancel from another thread, take a `Canceler` before moving the reader into
the read thread:

```rust,no_run
use cancelreader::new_reader;

let file = std::fs::File::open("/dev/tty")?;
let reader = new_reader(file)?;
let canceler = reader.canceler();

std::thread::spawn(move || {
    let mut reader = reader;
    let mut buf = [0u8; 1024];
    let _ = std::io::Read::read(&mut reader, &mut buf);
});

// later, from any thread
canceler.cancel();
# Ok::<(), std::io::Error>(())
```

## Installation

```toml
[dependencies]
cancelreader = "0.1"
```

## License

Licensed under the [MIT license](LICENSE).
