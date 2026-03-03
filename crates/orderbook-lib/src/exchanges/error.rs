use serde_json::Error as SerdeJsonError;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;

#[derive(Error, Debug)]
pub enum ExchangeError {
    // Boxed to keep the enum size small (tungstenite::Error is 136 bytes).
    #[error("websocket error: {0}")]
    WebSocket(Box<TungsteniteError>),

    #[error("json parse error: {0}")]
    Json(#[from] SerdeJsonError),

    #[error("unexpected message format: {0}")]
    Format(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

impl From<TungsteniteError> for ExchangeError {
    fn from(err: TungsteniteError) -> Self {
        ExchangeError::WebSocket(Box::new(err))
    }
}
