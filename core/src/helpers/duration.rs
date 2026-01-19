use std::time::Duration;

pub fn format_duration(duration: Duration) -> String {
    let total_ms = duration.as_millis();
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    let ms = total_ms % 1000;

    match (hours, minutes, secs) {
        (h, m, _) if h > 0 => format!("{}h {}m {}s", h, m, seconds),
        (0, m, _) if m > 0 => format!("{}m {}s", m, seconds),
        (0, 0, s) if s >= 10 => format!("{}s", s),
        (0, 0, s) if s > 0 => format!("{}s {}ms", s, ms),
        _ => format!("{}ms", total_ms),
    }
}
