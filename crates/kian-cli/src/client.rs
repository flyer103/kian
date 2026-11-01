//! Thin JSON-RPC 2.0 HTTP client (card C14) — the transport the
//! session commands use to talk to a running `kian serve`.
//!
//! Two endpoints:
//! - `POST /rpc` — the C13 control plane (`projection`, …);
//! - `POST /ops/rpc` — the C14 operational plane (`wake_now`,
//!   `recovery_budget_reset`).
//!
//! Errors are honest and structured: transport failures stay
//! [`ClientError::Http`]; a JSON-RPC error object becomes
//! [`ClientError::Rpc`] with the server's code and message (unknown
//! session = -32001, draining = -32002, terminal = -32003, internal =
//! -32000) so callers can match instead of parsing strings.

use serde_json::{Value, json};

/// What a client call can fail with.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Http(String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("malformed response: {0}")]
    Malformed(String),
}

/// A JSON-RPC 2.0 client bound to one server base URL.
#[derive(Debug, Clone)]
pub struct KianClient {
    base: String,
    http: reqwest::Client,
}

impl KianClient {
    /// Client for `base` (e.g. `http://127.0.0.1:7331`).
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Calls a control-plane method on `POST /rpc`.
    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value, ClientError> {
        self.call("/rpc", method, params).await
    }

    /// Calls an operational method on `POST /ops/rpc`.
    pub async fn ops(&self, method: &str, params: Value) -> Result<Value, ClientError> {
        self.call("/ops/rpc", method, params).await
    }

    async fn call(&self, path: &str, method: &str, params: Value) -> Result<Value, ClientError> {
        let url = format!("{}{path}", self.base);
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ClientError::Http(format!("{url}: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| ClientError::Http(format!("{url}: body read: {e}")))?;
        if !status.is_success() {
            return Err(ClientError::Http(format!("{url}: status {status}: {text}")));
        }
        let value: Value = serde_json::from_str(&text)
            .map_err(|e| ClientError::Malformed(format!("{url}: not JSON: {e}")))?;
        if let Some(err) = value.get("error").filter(|e| !e.is_null()) {
            return Err(ClientError::Rpc {
                code: err.get("code").and_then(Value::as_i64).unwrap_or(0),
                message: err
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("(no message)")
                    .to_string(),
            });
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| ClientError::Malformed(format!("{url}: no `result` field")))
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientError, KianClient};
    use serde_json::json;

    /// A client against an unreachable port fails as transport, not panic.
    #[tokio::test]
    async fn unreachable_server_is_a_transport_error() {
        let client = KianClient::new("http://127.0.0.1:9");
        match client.rpc("projection", json!({})).await {
            Err(ClientError::Http(_)) => {}
            other => panic!("expected Http error, got {other:?}"),
        }
    }
}
