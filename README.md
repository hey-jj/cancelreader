# cancelreader

A cancelable reader. Wrap a blocking input source like stdin or a raw terminal
so another thread can abort a blocked read during shutdown. The abort consumes
no input.

The mechanism is the self-pipe trick paired with an operating system readiness
wait. A read blocks until either the input becomes readable or a cancel signal
arrives. `cancel` wakes the wait and makes the read return `Canceled` instead
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

On macOS and the BSDs, kqueue returns ready at once when it watches `/dev/tty`,
so a reader named `/dev/tty` routes to select instead. A `std::fs::File` does
not carry its path, so wrap a terminal file with `named(file, "/dev/tty")` to
hit that route.

## Usage

```rust,no_run
use std::io::Read;
use cancelreader::{named, new_reader, is_canceled};

let file = std::fs::File::open("/dev/tty")?;
let mut reader = new_reader(named(file, "/dev/tty"))?;

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
use cancelreader::{named, new_reader};

let file = std::fs::File::open("/dev/tty")?;
let reader = new_reader(named(file, "/dev/tty"))?;
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
