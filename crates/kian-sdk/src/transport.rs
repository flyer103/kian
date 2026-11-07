//! Thin JSON-RPC 2.0 HTTP transport (extracted from
//! `kian-cli/src/client.rs` unchanged ) — the transport the
//! typed surface rides on.
//!
//! Two endpoints:
//! - `POST /rpc` — the control plane (`projection`, …);
//! - `POST /ops/rpc` — the operational plane (`wake_now`,
//!   `recovery_budget_reset`, `mock_channel_send`).
//!
//! The request envelope (`jsonrpc`/`id`/`method`/`params`) is byte-stable
//! by the serialization-freeze discipline; golden wire fixtures pin it.

use serde_json::{Value, json};

/// A JSON-RPC 2.0 client bound to one server base URL.
#[derive(Debug, Clone)]
pub struct KianClient {
    pub(crate) base: String,
    pub(crate) http: reqwest::Client,
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
    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value, super::ClientError> {
        self.call("/rpc", method, params).await
    }

    /// Calls an operational method on `POST /ops/rpc`.
    pub async fn ops(&self, method: &str, params: Value) -> Result<Value, super::ClientError> {
        self.call("/ops/rpc", method, params).await
    }

    async fn call(
        &self,
        path: &str,
        method: &str,
        params: Value,
    ) -> Result<Value, super::ClientError> {
        let url = format!("{}{path}", self.base);
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| super::ClientError::Http(format!("{url}: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| super::ClientError::Http(format!("{url}: body read: {e}")))?;
        if !status.is_success() {
            return Err(super::ClientError::Http(format!(
                "{url}: status {status}: {text}"
            )));
        }
        let value: Value = serde_json::from_str(&text)
            .map_err(|e| super::ClientError::Malformed(format!("{url}: not JSON: {e}")))?;
        if let Some(err) = value.get("error").filter(|e| !e.is_null()) {
            return Err(super::ClientError::Rpc {
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
            .ok_or_else(|| super::ClientError::Malformed(format!("{url}: no `result` field")))
    }
}

#[cfg(test)]
mod tests {
    use super::super::ClientError;
    use super::KianClient;
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
