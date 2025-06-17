use std::{
    io::Write,
    sync::atomic::{AtomicBool, Ordering},
};

use once_cell::sync::Lazy;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    fmt::{
        format::{Format, Writer},
        MakeWriter,
    },
    EnvFilter,
};

static SHUTDOWN_IN_PROGRESS: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

struct ShutdownAwareWriter {
    buffer: std::io::BufWriter<std::io::Stdout>,
}

impl ShutdownAwareWriter {
    fn new() -> Self {
        Self { buffer: std::io::BufWriter::new(std::io::stdout()) }
    }
}

impl Write for ShutdownAwareWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if SHUTDOWN_IN_PROGRESS.load(Ordering::Relaxed) {
            // During shutdown, write directly to stdout
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            handle.write(buf)
        } else {
            self.buffer.write(buf)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if SHUTDOWN_IN_PROGRESS.load(Ordering::Relaxed) {
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            handle.flush()
        } else {
            self.buffer.flush()
        }
    }
}

struct ShutdownAwareWriterMaker;

impl<'a> MakeWriter<'a> for ShutdownAwareWriterMaker {
    type Writer = ShutdownAwareWriter;

    fn make_writer(&'a self) -> Self::Writer {
        ShutdownAwareWriter::new()
    }
}

struct CustomTimer;

impl tracing_subscriber::fmt::time::FormatTime for CustomTimer {
    fn format_time(&self, writer: &mut Writer<'_>) -> std::fmt::Result {
        // Use a simpler time format during shutdown
        if SHUTDOWN_IN_PROGRESS.load(Ordering::Relaxed) {
            let now = chrono::Local::now();
            write!(writer, "{}", now.format("%H:%M:%S"))
        } else {
            let now = chrono::Local::now();
            write!(writer, "{} - {}", now.format("%d %B"), now.format("%H:%M:%S%.6f"))
        }
    }
}

pub fn setup_logger(log_level: LevelFilter) {
    let filter = EnvFilter::from_default_env().add_directive(log_level.into());

    let format = Format::default().with_timer(CustomTimer).with_level(true).with_target(false);

    let subscriber = tracing_subscriber::fmt()
        .with_writer(ShutdownAwareWriterMaker)
        .with_env_filter(filter)
        .event_format(format)
        .finish();

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        // Use println! here since logging might not be set up yet
        println!("Logger has already been set up, continuing...");
    }
}

pub fn setup_info_logger() {
    setup_logger(LevelFilter::INFO);
}

// Call this when starting shutdown
pub fn mark_shutdown_started() {
    SHUTDOWN_IN_PROGRESS.store(true, Ordering::Relaxed);
}

// Optional guard for temporary logger suppression
pub struct LoggerGuard;

impl Drop for LoggerGuard {
    fn drop(&mut self) {
        SHUTDOWN_IN_PROGRESS.store(false, Ordering::Relaxed);
    }
}
