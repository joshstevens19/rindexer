use tracing::{debug, level_filters::LevelFilter};
use tracing_subscriber::{
    fmt::format::{Format, Writer},
    EnvFilter,
};

struct CustomTimer;

impl tracing_subscriber::fmt::time::FormatTime for CustomTimer {
    fn format_time(&self, writer: &mut Writer<'_>) -> std::fmt::Result {
        let now = chrono::Local::now();
        write!(writer, "{} - {}", now.format("%d %B"), now.format("%H:%M:%S%.6f"))
    }
}

pub fn setup_logger(log_level: LevelFilter) {
    let filter = EnvFilter::from_default_env().add_directive(log_level.into());

    let format = Format::default().with_timer(CustomTimer).with_level(true).with_target(false);

    let subscriber =
        tracing_subscriber::fmt().with_env_filter(filter).event_format(format).finish();

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        debug!("Logger has already been set up, continuing...");
    }
}

pub fn setup_info_logger() {
    setup_logger(LevelFilter::INFO);
}

// pub fn set_no_op_logger() -> DefaultGuard {
//     let no_op_subscriber = FmtSubscriber::builder().with_writer(|| NullWriter).finish();
//
//     let no_op_dispatch = Dispatch::new(no_op_subscriber);
//
//     tracing::dispatcher::set_default(&no_op_dispatch)
// }
