use std::time::{Duration, Instant};

use alloy::primitives::U64;

#[derive(Debug, PartialEq, Eq)]
pub enum HeartbeatAction {
    Silent,
    Alive,
    Stalled,
}

/// Tracks whether the observed chain tip has advanced between polls so we can
/// distinguish an expected provider cache hit from a stalled RPC.
pub struct HeartbeatTracker {
    last_observed_tip: Option<U64>,
    tip_last_advanced_at: Instant,
    last_heartbeat_at: Instant,
    interval: Duration,
    stall_timeout: Duration,
}

impl HeartbeatTracker {
    pub fn new(poll_interval: Duration) -> Self {
        Self::new_at(poll_interval, Instant::now())
    }

    fn new_at(poll_interval: Duration, now: Instant) -> Self {
        let interval = Duration::from_secs(300);
        Self {
            last_observed_tip: None,
            tip_last_advanced_at: now,
            last_heartbeat_at: now,
            interval,
            // Allow a fresh poll before treating an unchanged cached tip as stalled.
            stall_timeout: interval.max(poll_interval.saturating_mul(2)),
        }
    }

    pub fn stall_timeout(&self) -> Duration {
        self.stall_timeout
    }

    pub fn tick(&mut self, tip: U64) -> HeartbeatAction {
        self.tick_at(tip, Instant::now())
    }

    fn tick_at(&mut self, tip: U64, now: Instant) -> HeartbeatAction {
        // A tip only counts as "advancing" when it strictly increases. A flapping
        // load-balanced RPC that oscillates between two heights must not reset
        // the stall clock, otherwise Stalled would never fire.
        let advanced = self.last_observed_tip.is_none_or(|prev| tip > prev);
        if advanced {
            self.last_observed_tip = Some(tip);
            self.tip_last_advanced_at = now;
        }
        if now.duration_since(self.last_heartbeat_at) < self.interval {
            return HeartbeatAction::Silent;
        }
        self.last_heartbeat_at = now;
        if now.duration_since(self.tip_last_advanced_at) >= self.stall_timeout {
            HeartbeatAction::Stalled
        } else {
            HeartbeatAction::Alive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_poll_is_silent() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        assert_eq!(tracker.tick_at(U64::from(100), start), HeartbeatAction::Silent);
    }

    #[test]
    fn silent_before_interval_elapses() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        let later = start + Duration::from_secs(299);
        assert_eq!(tracker.tick_at(U64::from(100), later), HeartbeatAction::Silent);
    }

    #[test]
    fn alive_when_tip_advanced_within_interval() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        let mid = start + Duration::from_secs(200);
        tracker.tick_at(U64::from(101), mid);
        let after = start + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(101), after), HeartbeatAction::Alive);
    }

    #[test]
    fn stalled_when_tip_unchanged_for_interval() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        let after = start + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(100), after), HeartbeatAction::Stalled);
    }

    #[test]
    fn cache_hit_repeated_same_tip_does_not_reset_stall_clock() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        for secs in [60, 120, 180, 240, 299] {
            assert_eq!(
                tracker.tick_at(U64::from(100), start + Duration::from_secs(secs)),
                HeartbeatAction::Silent
            );
        }
        let after = start + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(100), after), HeartbeatAction::Stalled);
    }

    #[test]
    fn heartbeat_resets_after_emission() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        let first = start + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(100), first), HeartbeatAction::Stalled);
        let too_soon = first + Duration::from_secs(299);
        assert_eq!(tracker.tick_at(U64::from(100), too_soon), HeartbeatAction::Silent);
        let next = first + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(100), next), HeartbeatAction::Stalled);
    }

    #[test]
    fn flapping_tip_does_not_reset_stall_clock() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        for (secs, tip) in [(60u64, 99u64), (120, 100), (180, 99), (240, 100), (299, 99)] {
            tracker.tick_at(U64::from(tip), start + Duration::from_secs(secs));
        }
        let after = start + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(100), after), HeartbeatAction::Stalled);
    }

    #[test]
    fn tip_advance_after_stall_restores_alive_on_next_emission() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_millis(50), start);
        tracker.tick_at(U64::from(100), start);
        let first = start + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(100), first), HeartbeatAction::Stalled);
        tracker.tick_at(U64::from(101), first + Duration::from_secs(10));
        let next = first + Duration::from_secs(300);
        assert_eq!(tracker.tick_at(U64::from(101), next), HeartbeatAction::Alive);
    }

    #[test]
    fn slow_polling_keeps_heartbeats_alive_between_refreshes() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_secs(600), start);
        tracker.tick_at(U64::from(100), start);
        for (secs, tip) in [(300, 100), (600, 101), (900, 101), (1200, 102)] {
            assert_eq!(
                tracker.tick_at(U64::from(tip), start + Duration::from_secs(secs)),
                HeartbeatAction::Alive
            );
        }
    }

    #[test]
    fn slow_polling_still_detects_stalls_and_recovers() {
        let start = Instant::now();
        let mut tracker = HeartbeatTracker::new_at(Duration::from_secs(600), start);
        assert_eq!(tracker.stall_timeout(), Duration::from_secs(1200));
        tracker.tick_at(U64::from(100), start);
        for secs in [300, 600, 900] {
            assert_eq!(
                tracker.tick_at(U64::from(100), start + Duration::from_secs(secs)),
                HeartbeatAction::Alive
            );
        }
        assert_eq!(
            tracker.tick_at(U64::from(100), start + Duration::from_secs(1200)),
            HeartbeatAction::Stalled
        );
        assert_eq!(
            tracker.tick_at(U64::from(101), start + Duration::from_secs(1500)),
            HeartbeatAction::Alive
        );
    }
}
