use std::time::Duration;

/// Runtime knobs for connector retry/timeout behavior.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectorRuntimeConfig {
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    /// A session that lasted this long is considered healthy; backoff resets.
    pub(crate) healthy_threshold: Duration,
    pub(crate) max_consecutive_permanent_errors: u32,
    pub(crate) max_consecutive_parse_errors: u32,
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
            max_consecutive_parse_errors: 20,
        }
    }
}
