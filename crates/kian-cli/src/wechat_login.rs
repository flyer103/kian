//! `kian wechat login` — the QR login CLI (CHANNEL-WECHAT §8).
//!
//! Runs the QR flow over the adapter's login state machine
//! ([`kian_superagent::wechat::login`]), prints the QR URL and
//! operator instructions, handles every frozen status (`wait`,
//! `scaned`, `scaned_but_redirect`, `need_verifycode`,
//! `verify_code_blocked`, `binded_redirect`, `expired` with a bounded
//! re-fetch), and on `confirmed` writes the credential file
//! atomically with mode 0600 via the adapter's credentials module.
//!
//! Output discipline (§6): the command prints `account_id`/`user_id`
//! and the resolved path — **never the token**, on stdout or stderr,
//! in text or `--json` mode. `--json` prints one machine-readable
//! object on stdout; progress lines go to stderr so the stdout stream
//! stays parseable. Any failure exits non-zero with a clear message.
//!
//! Serve never performs this flow: a configured channel without a
//! usable credential file refuses to start and names this command.

use std::path::PathBuf;

use kian_superagent::wechat::credentials::{self, WechatCredentials};
use kian_superagent::wechat::login::{
    DEFAULT_QR_REFRESH_BUDGET, DEFAULT_STATUS_POLL_BUDGET, LoginError, QrLoginFlow, QrStep,
};
use kian_superagent::wechat::protocol::IlinkClient;
use serde_json::json;

use crate::config::{ServeConfig, WechatChannelSection};

/// `kian wechat login [--config <path>] [--channel <name>] [--json]`.
pub struct WechatLoginArgs {
    pub config: PathBuf,
    pub channel: Option<String>,
    pub json: bool,
}

impl Default for WechatLoginArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("kian.yaml"),
            channel: None,
            json: false,
        }
    }
}

/// The `type: wechat` channel to log in: the one named by
/// `--channel`, or the config's single wechat channel (config
/// validation caps the type at one; `kian wechat login` also runs
/// against configs whose credential file does not exist yet).
fn resolve_channel<'a>(
    config: &'a ServeConfig,
    wanted: Option<&str>,
) -> Result<(String, &'a WechatChannelSection), String> {
    let superagent = config.superagent.as_ref().ok_or_else(|| {
        "the config has no `superagent:` section — a `type: wechat` channel is required for \
             `kian wechat login`"
            .to_string()
    })?;
    let wechat_channels: Vec<(&String, &WechatChannelSection)> = superagent
        .channels
        .iter()
        .filter_map(|(name, channel)| channel.wechat.as_ref().map(|section| (name, section)))
        .collect();
    match wanted {
        Some(wanted) => wechat_channels
            .into_iter()
            .find(|(name, _)| name.as_str() == wanted)
            .map(|(name, section)| (name.clone(), section))
            .ok_or_else(|| {
                format!(
                    "channel `{wanted}` is not a `type: wechat` channel in the config \
                     (wechat channels: {})",
                    superagent
                        .channels
                        .iter()
                        .filter(|(_, c)| c.wechat.is_some())
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }),
        None => match wechat_channels.len() {
            1 => Ok(wechat_channels
                .into_iter()
                .next()
                .map(|(name, section)| (name.clone(), section))
                .expect("exactly one wechat channel")),
            0 => Err(
                "the config declares no `type: wechat` channel — `kian wechat login` needs one"
                    .to_string(),
            ),
            _ => Err(
                "the config declares multiple wechat channels — pass `--channel <name>`"
                    .to_string(),
            ),
        },
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Renders a login failure for the operator (bounded, honest; never
/// carries the token — [`LoginError`] has no token-carrying variant).
fn render_error(error: LoginError) -> String {
    error.to_string()
}

/// Runs the login. Progress lines are printed as they happen (stdout
/// in text mode, stderr in `--json` mode); the returned string is the
/// final token-free summary (text) or the single JSON object (`--json`),
/// which the caller prints on stdout.
pub async fn run_login(args: &WechatLoginArgs) -> Result<String, String> {
    let config = ServeConfig::load(&args.config).map_err(|e| e.to_string())?;
    let (name, section) = resolve_channel(&config, args.channel.as_deref())?;
    let api_base = section.api_base.trim().to_string();
    let credentials_file = PathBuf::from(section.credentials_file.trim());

    let progress = |line: String| {
        if args.json {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    };

    let client =
        IlinkClient::new().map_err(|e| format!("cannot build the iLink http client: {e}"))?;
    let mut flow = QrLoginFlow::new(client, api_base.clone(), DEFAULT_QR_REFRESH_BUDGET);

    // Login step 1: fetch the QR and tell the operator what to do
    // with it (`qrcode_img_content` is the URL to open in WeChat).
    let img = flow.fetch_qrcode().await.map_err(|e| {
        format!(
            "cannot fetch a login QR code from `{api_base}`: {}",
            render_error(e)
        )
    })?;
    progress(format!(
        "WeChat login for channel `{name}` — open this URL in WeChat (or scan its QR code):\n  \
         {img}\nthen confirm on the phone. Waiting for the scan (Ctrl-C aborts)…"
    ));

    // Login step 2: poll the status to completion. Every poll is
    // bounded; the loop exits on success or a bounded, honest error.
    for _poll in 0..DEFAULT_STATUS_POLL_BUDGET {
        match flow.poll_status().await.map_err(render_error)? {
            QrStep::Wait => progress("… waiting for the scan".into()),
            QrStep::Scanned => progress("scanned — waiting for confirmation".into()),
            QrStep::Redirected { redirect_host } => progress(format!(
                "polling switched to the redirect host `{redirect_host}`"
            )),
            QrStep::NeedVerifyCode => progress(
                "the login needs a verification code: type the code shown in the WeChat app \
                 on your phone (kian keeps polling — the iLink API has no code-submit \
                 endpoint)"
                    .into(),
            ),
            QrStep::VerifyCodeBlocked => progress(
                "verification code entered wrong too many times — wait and retry on the \
                 phone; kian keeps polling"
                    .into(),
            ),
            QrStep::QrRefreshed { qrcode_img_content } => progress(format!(
                "QR code expired — a fresh one was fetched. Open this URL in WeChat:\n  \
                 {qrcode_img_content}"
            )),
            QrStep::Success(material) => {
                // The token's only home is the file; the file is
                // written atomically with mode 0600.
                let base_url = if material.base_url.trim().is_empty() {
                    api_base.clone()
                } else {
                    material.base_url.trim().to_string()
                };
                let credentials = WechatCredentials::new(
                    base_url,
                    material.bot_token,
                    material.account_id.clone(),
                    material.user_id.clone(),
                    now_unix_ms(),
                );
                credentials::save(&credentials_file, &credentials)
                    .map_err(|e| format!("cannot write the credential file: {e}"))?;
                let path = credentials_file.display().to_string();
                let summary = if args.json {
                    json!({
                        "status": "ok",
                        "channel": name,
                        "account_id": material.account_id,
                        "user_id": material.user_id,
                        "credentials_file": path,
                    })
                    .to_string()
                } else {
                    format!(
                        "wechat login confirmed — account_id {}, user_id {}; credentials \
                         written to {path} (mode 0600; the token lives only in that file)",
                        material.account_id, material.user_id
                    )
                };
                // `material` (token inside) drops here without ever
                // being printed.
                drop(credentials);
                return Ok(summary);
            }
        }
    }
    Err(format!(
        "login did not complete within {DEFAULT_STATUS_POLL_BUDGET} status polls — run \
             `kian wechat login` again"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_args_target_the_default_config() {
        let args = WechatLoginArgs::default();
        assert_eq!(args.config, PathBuf::from("kian.yaml"));
        assert!(args.channel.is_none() && !args.json);
    }
}
