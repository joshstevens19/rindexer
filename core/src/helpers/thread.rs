use std::{
    cell::RefCell,
    io::{self, Write},
};

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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_null_writer_write() {
        let mut writer = NullWriter;
        let data = b"hello";
        let result = writer.write(data);
        assert!(result.is_ok());
        assert_eq!(result.expect("Failed to write"), data.len());
    }

    #[test]
    fn test_null_writer_flush() {
        let mut writer = NullWriter;
        let result = writer.flush();
        assert!(result.is_ok());
    }

    #[test]
    fn test_thread_no_logging_stdout() {
        set_thread_no_logging();
        THREAD_STDOUT.with(|thread_stdout| {
            let mut thread_stdout = thread_stdout.borrow_mut();
            let writer = thread_stdout.as_mut().expect("Failed to get thread stdout");
            let data = b"hello";
            let result = writer.write(data);
            assert!(result.is_ok());
            assert_eq!(result.expect("Failed to write"), data.len());
        });
    }

    #[test]
    fn test_thread_no_logging_stderr() {
        set_thread_no_logging();
        THREAD_STDERR.with(|thread_stderr| {
            let mut thread_stderr = thread_stderr.borrow_mut();
            let writer = thread_stderr.as_mut().expect("Failed to get thread stderr");
            let data = b"error";
            let result = writer.write(data);
            assert!(result.is_ok());
            assert_eq!(result.expect("Failed to write"), data.len());
        });
    }
}
