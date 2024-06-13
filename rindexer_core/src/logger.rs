use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::{Format, Writer};
use tracing_subscriber::EnvFilter;

struct CustomTimer;

impl tracing_subscriber::fmt::time::FormatTime for CustomTimer {
    fn format_time(&self, writer: &mut Writer<'_>) -> std::fmt::Result {
        let now = chrono::Local::now();
        write!(
            writer,
            "{} - {}",
            now.format("%d %B"),
            now.format("%H:%M:%S%.6f")
        )
    }
}

pub fn setup_logger(log_level: LevelFilter) {
    let filter = EnvFilter::from_default_env().add_directive(log_level.into());

    let format = Format::default()
        .with_timer(CustomTimer) // As time is included in the custom timer.
        .with_level(true)
        .with_target(false); // Disable logging the target (module path).

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .event_format(format)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set the default subscriber");
}
