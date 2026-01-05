use std::time::Duration;

pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    match (hours, minutes) {
        (h, m) if h > 0 => format!("{}h {}m {}s", h, m, seconds),
        (0, m) if m > 0 => format!("{}m {}s", m, seconds),
        _ => format!("{}s", seconds),
    }
}
