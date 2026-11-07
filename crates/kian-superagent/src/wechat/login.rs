//! The QR login state machine (CHANNEL-WECHAT §2.1/§8).
//!
//! Credential states only — no CLI I/O lives here. `kian-cli` (R10-04)
//! renders the QR image content, prints ids and prompts; this module
//! owns the transitions: `wait`/`scaned` (keep polling),
//! `scaned_but_redirect` (switch polling host), `need_verifycode` /
//! `verify_code_blocked` (operator acts on the phone; no verify-code
//! submit endpoint exists in the frozen facts, so the flow keeps
//! polling), `binded_redirect` (success iff a token is present),
//! `confirmed` (success), `expired` (bounded QR re-fetch, then an
//! honest error). Every loop is bounded.

use std::time::Duration;

use serde_json::Value;

use super::protocol::IlinkClient;

/// Default bound on QR re-fetches after `expired` (§2.1 "bounded
/// refresh count").
pub const DEFAULT_QR_REFRESH_BUDGET: u32 = 3;

/// Default bound on status polls inside [`run_login`] — a login may
/// idle in `wait` for a long time, but never unboundedly (§8 global
/// bounded-loop rule).
pub const DEFAULT_STATUS_POLL_BUDGET: usize = 240;

/// Long-poll deadline for one status request (server holds ~35 s).
pub const QR_STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(45);

/// The material `confirmed` (or a token-bearing `binded_redirect`)
/// yields. The `bot_token` is a credential: print account/user ids,
/// never the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginSuccess {
    pub bot_token: String,
    pub account_id: String,
    pub user_id: String,
    /// The `baseurl` the login response carries (the adapter still uses
    /// the configured `api_base`; this is recorded for diagnostics).
    pub base_url: String,
}

/// One step of the QR flow, surfaced to the caller (the CLI layer
/// prints these; nothing here performs I/O).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QrStep {
    /// `wait`: nobody scanned yet.
    Wait,
    /// `scaned`: scanned, not yet confirmed.
    Scanned,
    /// `scaned_but_redirect`: polling switched to the redirect host.
    Redirected { redirect_host: String },
    /// `need_verifycode`: the operator must enter the code (on the
    /// phone side — no submit endpoint exists in the frozen facts).
    NeedVerifyCode,
    /// `verify_code_blocked`: too many wrong codes; the operator must
    /// wait and retry on the phone.
    VerifyCodeBlocked,
    /// `expired`, and the bounded QR re-fetch still had budget: a fresh
    /// QR was issued (carry `qrcode_img_content` to the operator).
    QrRefreshed { qrcode_img_content: String },
    /// `confirmed` (or token-bearing `binded_redirect`).
    Success(LoginSuccess),
}

/// Login failures, all bounded and honest. None carries the token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoginError {
    #[error("login protocol failure: {0}")]
    Protocol(#[from] super::protocol::IlinkError),
    #[error("login cannot poll a status before a QR code was fetched")]
    NoQrCode,
    #[error("login status response malformed: {0}")]
    Malformed(String),
    #[error("login status carried unknown status `{0}` (protocol break; not guessed)")]
    UnknownStatus(String),
    #[error("login reported binded_redirect without a bot token")]
    BindedWithoutToken,
    #[error("login QR expired {budget}x and the refresh budget is spent")]
    QrExpiredOut { budget: u32 },
    #[error("login did not complete within {polls} status polls")]
    PollBudgetExhausted { polls: usize },
}

/// One parsed `get_qrcode_status` body (before flow decisions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QrStatus {
    Wait,
    Scanned,
    ScannedButRedirect { redirect_host: String },
    NeedVerifyCode,
    VerifyCodeBlocked,
    BindedRedirect,
    Confirmed(LoginSuccess),
    Expired,
}

/// Parses a status body. Tolerates a `data` envelope; an unknown status
/// is an error, never a guess (protocol-break discipline).
pub fn parse_qr_status(value: &Value) -> Result<QrStatus, LoginError> {
    let body = value.get("data").unwrap_or(value);
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| LoginError::Malformed("no status field".into()))?;
    match status {
        "wait" => Ok(QrStatus::Wait),
        "scaned" => Ok(QrStatus::Scanned),
        "scaned_but_redirect" => Ok(QrStatus::ScannedButRedirect {
            redirect_host: body
                .get("redirect_host")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "need_verifycode" => Ok(QrStatus::NeedVerifyCode),
        "verify_code_blocked" => Ok(QrStatus::VerifyCodeBlocked),
        "binded_redirect" => match login_material(body)? {
            Some(material) => Ok(QrStatus::Confirmed(material)),
            None => Ok(QrStatus::BindedRedirect),
        },
        "confirmed" => login_material(body)?
            .map(QrStatus::Confirmed)
            .ok_or(LoginError::Malformed(
                "confirmed without a bot token".into(),
            )),
        "expired" => Ok(QrStatus::Expired),
        other => Err(LoginError::UnknownStatus(other.to_string())),
    }
}

/// Extracts `bot_token`/`ilink_bot_id`/`ilink_user_id`/`baseurl` from a
/// status body; `None` when no token is present (the
/// `binded_redirect`-without-token case).
fn login_material(body: &Value) -> Result<Option<LoginSuccess>, LoginError> {
    let Some(bot_token) = body
        .get("bot_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    else {
        return Ok(None);
    };
    Ok(Some(LoginSuccess {
        bot_token: bot_token.to_string(),
        account_id: body
            .get("ilink_bot_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        user_id: body
            .get("ilink_user_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        base_url: body
            .get("baseurl")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    }))
}

/// Resolves the post-redirect polling base: a full URL passes through;
/// a bare host joins the current base's scheme; anything else is
/// used verbatim (the official client sends a host).
pub fn resolve_redirect_base(current: &str, redirect_host: &str) -> String {
    let host = redirect_host.trim().trim_end_matches('/');
    if host.is_empty() {
        return current.to_string();
    }
    if host.starts_with("http://") || host.starts_with("https://") {
        return host.to_string();
    }
    if let Some((scheme, _)) = current.split_once("://") {
        return format!("{scheme}://{host}");
    }
    host.to_string()
}

/// The QR login flow: fetch a QR, poll its status, follow redirects,
/// re-fetch expired QRs within a budget. One instance per login
/// attempt; steps surface as [`QrStep`] for the CLI layer.
pub struct QrLoginFlow {
    client: IlinkClient,
    api_base: String,
    poll_base: String,
    refresh_budget: u32,
    refreshes_left: u32,
    /// Per-poll client deadline (default [`QR_STATUS_POLL_TIMEOUT`]).
    status_poll_timeout: Duration,
    qrcode: Option<String>,
}

impl QrLoginFlow {
    /// `refresh_budget` bounds QR re-fetches after `expired`.
    pub fn new(client: IlinkClient, api_base: impl Into<String>, refresh_budget: u32) -> Self {
        let api_base = api_base.into();
        Self {
            client,
            poll_base: api_base.clone(),
            api_base,
            refresh_budget,
            refreshes_left: refresh_budget,
            status_poll_timeout: QR_STATUS_POLL_TIMEOUT,
            qrcode: None,
        }
    }

    /// Test scaffolding: overrides the per-poll client deadline (the
    /// production default stays [`QR_STATUS_POLL_TIMEOUT`], above the
    /// server's ~35 s hold). Production code never calls this.
    pub fn with_status_poll_timeout(mut self, timeout: Duration) -> Self {
        self.status_poll_timeout = timeout;
        self
    }

    /// The base URL QR codes are fetched from (never redirected).
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// The base URL status polls currently target (moves on redirect).
    pub fn poll_base(&self) -> &str {
        &self.poll_base
    }

    /// The current QR's image content (the URL to open in WeChat).
    pub fn qrcode_img_content(&self) -> Option<&str> {
        self.qrcode.as_deref()
    }

    /// Login step 1: fetch a fresh QR. Returns the image content to
    /// show the operator.
    pub async fn fetch_qrcode(&mut self) -> Result<String, LoginError> {
        let code = self.client.get_bot_qrcode(&self.api_base).await?;
        self.qrcode = Some(code.qrcode);
        Ok(code.qrcode_img_content)
    }

    /// Login step 2: one status long-poll plus the automatic flow
    /// decisions (redirect switch, bounded expired re-fetch).
    pub async fn poll_status(&mut self) -> Result<QrStep, LoginError> {
        let Some(qrcode) = self.qrcode.clone() else {
            return Err(LoginError::NoQrCode);
        };
        let value = match self
            .client
            .get_qrcode_status(&self.poll_base, &qrcode, self.status_poll_timeout)
            .await
        {
            Ok(value) => value,
            // A client-side timeout on the ~35 s status long-poll is
            // normal and expected (CHANNEL-WECHAT §11 — the same rule
            // as the receive path): treat it as `wait` and re-poll.
            // Real transport errors stay fatal.
            Err(super::protocol::IlinkError::Timeout) => return Ok(QrStep::Wait),
            Err(other) => return Err(other.into()),
        };
        match parse_qr_status(&value)? {
            QrStatus::Wait => Ok(QrStep::Wait),
            QrStatus::Scanned => Ok(QrStep::Scanned),
            QrStatus::ScannedButRedirect { redirect_host } => {
                self.poll_base = resolve_redirect_base(&self.poll_base, &redirect_host);
                Ok(QrStep::Redirected { redirect_host })
            }
            QrStatus::NeedVerifyCode => Ok(QrStep::NeedVerifyCode),
            QrStatus::VerifyCodeBlocked => Ok(QrStep::VerifyCodeBlocked),
            QrStatus::BindedRedirect => Err(LoginError::BindedWithoutToken),
            QrStatus::Confirmed(material) => Ok(QrStep::Success(material)),
            QrStatus::Expired => {
                if self.refreshes_left == 0 {
                    return Err(LoginError::QrExpiredOut {
                        budget: self.refresh_budget,
                    });
                }
                self.refreshes_left -= 1;
                let img = self.fetch_qrcode().await?;
                Ok(QrStep::QrRefreshed {
                    qrcode_img_content: img,
                })
            }
        }
    }
}

/// Drives the flow to completion for the non-interactive happy path:
/// poll until success or a bounded, honest error. The interactive
/// states (`need_verifycode`, `verify_code_blocked`) surface as steps
/// here and keep polling — the operator resolves them on the phone
/// side; there is no code-submit endpoint in the frozen facts.
pub async fn run_login(flow: &mut QrLoginFlow, budget: usize) -> Result<LoginSuccess, LoginError> {
    if flow.qrcode.is_none() {
        flow.fetch_qrcode().await?;
    }
    for _poll in 0..budget {
        match flow.poll_status().await? {
            QrStep::Success(material) => return Ok(material),
            _ => continue,
        }
    }
    Err(LoginError::PollBudgetExhausted { polls: budget })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_parsing_covers_the_frozen_vocabulary() {
        assert_eq!(
            parse_qr_status(&serde_json::json!({"status": "wait"})),
            Ok(QrStatus::Wait)
        );
        assert_eq!(
            parse_qr_status(&serde_json::json!({"status": "scaned"})),
            Ok(QrStatus::Scanned)
        );
        assert_eq!(
            parse_qr_status(&serde_json::json!({
                "status": "scaned_but_redirect", "redirect_host": "h2.example"
            })),
            Ok(QrStatus::ScannedButRedirect {
                redirect_host: "h2.example".into()
            })
        );
        assert_eq!(
            parse_qr_status(&serde_json::json!({"status": "need_verifycode"})),
            Ok(QrStatus::NeedVerifyCode)
        );
        assert_eq!(
            parse_qr_status(&serde_json::json!({"status": "verify_code_blocked"})),
            Ok(QrStatus::VerifyCodeBlocked)
        );
        assert_eq!(
            parse_qr_status(&serde_json::json!({"status": "expired"})),
            Ok(QrStatus::Expired)
        );
        assert_eq!(
            parse_qr_status(&serde_json::json!({"data": {"status": "wait"}})),
            Ok(QrStatus::Wait),
            "a data envelope is tolerated"
        );
    }

    #[test]
    fn confirmed_yields_material_and_binded_redirect_needs_a_token() {
        let confirmed = serde_json::json!({
            "status": "confirmed",
            "bot_token": "fake-bot-token-login",
            "ilink_bot_id": "bot-9",
            "ilink_user_id": "user-9",
            "baseurl": "https://ilinkai.weixin.qq.com",
        });
        assert_eq!(
            parse_qr_status(&confirmed),
            Ok(QrStatus::Confirmed(LoginSuccess {
                bot_token: "fake-bot-token-login".into(),
                account_id: "bot-9".into(),
                user_id: "user-9".into(),
                base_url: "https://ilinkai.weixin.qq.com".into(),
            }))
        );

        // binded_redirect WITH a token is a success…
        let mut binded = confirmed.clone();
        binded["status"] = serde_json::json!("binded_redirect");
        assert!(matches!(
            parse_qr_status(&binded),
            Ok(QrStatus::Confirmed(_))
        ));
        // …without one it is the honest BindedRedirect state (the flow
        // turns that into an error).
        let bare = serde_json::json!({"status": "binded_redirect"});
        assert_eq!(parse_qr_status(&bare), Ok(QrStatus::BindedRedirect));
    }

    #[test]
    fn unknown_or_missing_statuses_are_errors_never_guesses() {
        assert!(matches!(
            parse_qr_status(&serde_json::json!({"status": "wat"})),
            Err(LoginError::UnknownStatus(_))
        ));
        assert!(matches!(
            parse_qr_status(&serde_json::json!({})),
            Err(LoginError::Malformed(_))
        ));
    }

    #[test]
    fn redirect_base_resolution_covers_hosts_and_full_urls() {
        assert_eq!(
            resolve_redirect_base("https://a.example", "b.example"),
            "https://b.example"
        );
        assert_eq!(
            resolve_redirect_base("https://a.example", "http://b.example/x/"),
            "http://b.example/x"
        );
        assert_eq!(
            resolve_redirect_base("https://a.example", ""),
            "https://a.example",
            "an empty redirect keeps the current host"
        );
        assert_eq!(resolve_redirect_base("not-a-url", "b.example"), "b.example");
    }
}
