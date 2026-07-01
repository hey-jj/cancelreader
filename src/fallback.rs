//! The fallback backend.
//!
//! Used for readers without a raw file descriptor and for targets without a
//! readiness backend. It cannot interrupt a read that is already running.
//! `cancel` flips a flag and returns `false`. Future reads return
//! [`Canceled`] at once and consume no data.

use std::io::{self, Read};
use std::sync::Arc;

use crate::{CancelFlag, CancelReader, Canceled, Canceler};

/// A [`CancelReader`] that cannot cancel an ongoing read.
///
/// `cancel` always returns `false`. After a `cancel`, new reads return
/// [`Canceled`] and consume no data.
struct FallbackCancelReader<R> {
    reader: R,
    flag: Arc<CancelFlag>,
}

/// Wrap any reader in the fallback backend.
///
/// This never fails. The returned reader flips a flag on `cancel` and reports
/// the cancellation on the next read.
pub fn new_fallback_cancel_reader<R>(reader: R) -> Box<dyn CancelReader + Send>
where
    R: Read + Send + 'static,
{
    Box::new(FallbackCancelReader {
        reader,
        flag: Arc::new(CancelFlag::default()),
    })
}

impl<R: Read> Read for FallbackCancelReader<R> {
    fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
        if self.flag.is_canceled() {
            return Err(Canceled.into());
        }

        let result = self.reader.read(data);
        // A blocking inner reader may sit in this call while another thread
        // cancels. Surface the cancellation even though the read already ran.
        if self.flag.is_canceled() {
            return Err(Canceled.into());
        }
        result
    }
}

impl<R: Read + Send> CancelReader for FallbackCancelReader<R> {
    fn cancel(&self) -> bool {
        self.flag.set_canceled();
        false
    }

    fn canceler(&self) -> Canceler {
        Canceler::fallback(Arc::clone(&self.flag))
    }

    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
