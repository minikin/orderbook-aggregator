use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExchangeError {
    // Boxed to keep the enum size small (tungstenite::Error is 136 bytes).
    #[error("websocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("unexpected message format: {0}")]
    Format(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for ExchangeError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        ExchangeError::WebSocket(Box::new(e))
    }
}
