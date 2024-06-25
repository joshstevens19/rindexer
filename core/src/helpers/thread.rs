use std::cell::RefCell;
use std::io::{self, Write};

pub struct NullWriter;

impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

thread_local! {
    static THREAD_STDOUT: RefCell<Option<Box<dyn Write + Send>>> = RefCell::new(None);
    static THREAD_STDERR: RefCell<Option<Box<dyn Write + Send>>> = RefCell::new(None);
}

pub fn set_thread_no_logging() {
    THREAD_STDOUT.with(|thread_stdout| {
        *thread_stdout.borrow_mut() = Some(Box::new(NullWriter));
    });

    THREAD_STDERR.with(|thread_stderr| {
        *thread_stderr.borrow_mut() = Some(Box::new(NullWriter));
    });
}
