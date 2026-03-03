use std::time::Duration;

/// Runtime configurations for connector retry/timeout behavior.
#[derive(Debug, Clone, Copy)]
pub struct ConnectorRuntimeConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub healthy_threshold: Duration,
    pub max_consecutive_permanent_errors: u32,
}

impl Default for ConnectorRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            healthy_threshold: Duration::from_secs(30),
            max_consecutive_permanent_errors: 3,
        }
    }
}
