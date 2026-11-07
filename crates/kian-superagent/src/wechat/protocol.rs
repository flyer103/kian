//! The typed iLink Bot API HTTP client (WeChat ClawBot).
//!
//! Every wire fact here is FROZEN in `design/CHANNEL-WECHAT.md` §2
//! (verified 2026-11-07; not re-researched): endpoints, headers, body
//! shapes, response fields. Changing a wire fact changes that file
//! first, then this module in the same change (§15 there).
//!
//! Redaction discipline: the bot token is a credential. It never
//! appears in an error variant, a log line, or a `Debug` render — the
//! error types below carry only paths, codes and server `errmsg`s, and
//! request headers are never logged by the adapter.

use std::time::Duration;

use serde_json::{Value, json};

/// `POST /ilink/bot/getupdates` (long-poll receive).
pub const GET_UPDATES_PATH: &str = "/ilink/bot/getupdates";

/// `POST /ilink/bot/sendmessage`.
pub const SEND_MESSAGE_PATH: &str = "/ilink/bot/sendmessage";

/// `GET /ilink/bot/get_bot_qrcode?bot_type=3` (login step 1).
pub const GET_BOT_QRCODE_PATH: &str = "/ilink/bot/get_bot_qrcode";

/// `GET /ilink/bot/get_qrcode_status?qrcode=<qrcode>` (login step 2,
/// long-poll ~35 s).
pub const GET_QRCODE_STATUS_PATH: &str = "/ilink/bot/get_qrcode_status";

/// `bot_type` for the QR login (the official client's value).
pub const QR_BOT_TYPE: u32 = 3;

/// `base_info.channel_version` sent with every POST body: kian's own
/// client version. Whether the server accepts an arbitrary value is
/// UNVERIFIED (CHANNEL-WECHAT §14.4 — settle in L3).
pub const CHANNEL_VERSION: &str = concat!("kian/", env!("CARGO_PKG_VERSION"));

/// `AuthorizationType` header value (the token kind the `Authorization`
/// header carries).
pub const AUTHORIZATION_TYPE: &str = "ilink_bot_token";

/// Observability `iLink-App-Id`. Whether the server requires the two
/// `iLink-*` headers from a third-party client is UNVERIFIED (§14.4) —
/// send them and verify in L3.
pub const ILINK_APP_ID: &str = "kian";

/// Observability `iLink-App-ClientVersion`, a uint32 `0x00MMNNPP`.
/// `0x00_00_01_00` spells 0.1.0 (major 0, minor 1, patch 0).
pub const ILINK_APP_CLIENT_VERSION: u32 = 0x0000_0100;

/// UA-style `bot_agent` header (≤256 bytes; also the HTTP client's
/// `User-Agent`).
pub const BOT_AGENT: &str = concat!("kian/", env!("CARGO_PKG_VERSION"), " (+wechat-ilink)");

/// `errcode` meaning "stale bot token": pause everything and wait for a
/// re-login (the official client pauses 1 h; kian mirrors via the
/// adapter's credential-mtime watch).
pub const STALE_TOKEN_ERRCODE: i64 = -14;

/// Typed iLink failures, pre-split the way the adapter needs them
/// (transport/timeout vs stale token vs API rejection vs unparseable).
/// No variant ever carries the bot token or a request header value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IlinkError {
    /// Client-side timeout. NORMAL for `getupdates` long-polls (the
    /// server holds the request): the caller treats it as an empty
    /// batch and retries.
    #[error("ilink request timed out")]
    Timeout,
    /// Any other transport-level failure (connect refused, TLS, …).
    #[error("ilink transport failure: {0}")]
    Transport(String),
    /// `errcode == -14`: the bot token is stale; a re-login is required.
    #[error("ilink rejected the bot token as stale (errcode -14)")]
    StaleToken,
    /// `ret != 0` or a non-`-14` `errcode` in an otherwise-parsed body.
    #[error("ilink api error: ret={ret} errcode={errcode} errmsg={errmsg}")]
    Api {
        ret: i64,
        errcode: i64,
        errmsg: String,
    },
    /// The body was not the expected JSON shape (a protocol break is an
    /// honest error, never a guess).
    #[error("ilink response malformed: {0}")]
    Malformed(String),
}

/// One `getupdates` batch, parsed tolerantly (reverse-documented
/// protocol: unknown fields are ignored, known fields are typed).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdatesBatch {
    /// Raw `WeixinMessage` objects, verbatim (the adapter maps them
    /// with a pure function).
    pub msgs: Vec<Value>,
    /// The opaque server cursor (`get_updates_buf`); carried within one
    /// process run only — never persisted (CHANNEL-WECHAT §10).
    pub cursor: Option<String>,
    /// Server-advertised long-poll hold, when present.
    pub longpolling_timeout_ms: Option<u64>,
}

/// One `sendmessage` response. `ret` is left to the adapter: only
/// `ret == 0` mints a receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendOutcome {
    /// The server's message id, parsed losslessly (uint64 on the wire →
    /// decimal string here; never through a float).
    pub message_id: Option<String>,
    pub ret: i64,
    pub errmsg: String,
}

/// The QR login ticket from `get_bot_qrcode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotQrCode {
    /// The opaque `qrcode` id the status endpoint is polled with.
    pub qrcode: String,
    /// The string to render/print (a URL the operator opens in
    /// WeChat).
    pub qrcode_img_content: String,
}

/// The minimal HTTP client for the iLink Bot API. Stateless beyond the
/// `reqwest::Client`; every call is given the base URL and the current
/// token (the token rotates by re-login, so it is per-call state).
#[derive(Debug, Clone)]
pub struct IlinkClient {
    http: reqwest::Client,
}

impl IlinkClient {
    /// Builds the client. The HTTP timeout is set per request (the
    /// long-poll receive and one-shot sends want different deadlines).
    pub fn new() -> Result<Self, IlinkError> {
        let http = reqwest::Client::builder()
            .user_agent(BOT_AGENT)
            .build()
            .map_err(|e| IlinkError::Transport(format!("http client build: {e}")))?;
        Ok(Self { http })
    }

    /// `POST /ilink/bot/getupdates`. A client-side timeout surfaces as
    /// [`IlinkError::Timeout`] — for the long-poll driver that outcome
    /// is normal and expected, not a failure.
    pub async fn get_updates(
        &self,
        base_url: &str,
        token: &str,
        cursor: &str,
        timeout: Duration,
    ) -> Result<UpdatesBatch, IlinkError> {
        let url = format!("{base_url}{GET_UPDATES_PATH}");
        let body = json!({
            "get_updates_buf": cursor,
            "base_info": base_info(),
        });
        let value = self
            .post_json(&url, token, &body, timeout, "getupdates")
            .await?;
        classify(&value)?;
        Ok(UpdatesBatch {
            msgs: value
                .get("msgs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            cursor: value
                .get("get_updates_buf")
                .and_then(Value::as_str)
                .map(str::to_string),
            longpolling_timeout_ms: value.get("longpolling_timeout_ms").and_then(Value::as_u64),
        })
    }

    /// `POST /ilink/bot/sendmessage` with the already-shaped `msg`
    /// object (the adapter builds the exact §2.4 shape). `ret != 0` is
    /// returned as data ([`SendOutcome`]), not an error — only the
    /// adapter knows the receipt rule; `-14` is classified here because
    /// it pauses the whole channel. A body WITHOUT an integer `ret` is
    /// [`IlinkError::Malformed`] (a shape break is never a success),
    /// and a non-`-14` non-zero `errcode` is [`IlinkError::Api`] —
    /// symmetric with the receive path's `classify`.
    pub async fn send_message(
        &self,
        base_url: &str,
        token: &str,
        msg: &Value,
        timeout: Duration,
    ) -> Result<SendOutcome, IlinkError> {
        let url = format!("{base_url}{SEND_MESSAGE_PATH}");
        let body = json!({"msg": msg, "base_info": base_info()});
        let value = self
            .post_json(&url, token, &body, timeout, "sendmessage")
            .await?;
        if value.get("errcode").and_then(Value::as_i64) == Some(STALE_TOKEN_ERRCODE) {
            return Err(IlinkError::StaleToken);
        }
        let ret = value.get("ret").and_then(Value::as_i64).ok_or_else(|| {
            IlinkError::Malformed(
                "sendmessage response carries no integer `ret` — nothing synthesized".into(),
            )
        })?;
        let errcode = value.get("errcode").and_then(Value::as_i64).unwrap_or(0);
        if errcode != 0 {
            return Err(IlinkError::Api {
                ret,
                errcode,
                errmsg: value
                    .get("errmsg")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string(),
            });
        }
        Ok(SendOutcome {
            message_id: lossless_id(&value, "message_id"),
            ret,
            errmsg: value
                .get("errmsg")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        })
    }

    /// `GET /ilink/bot/get_bot_qrcode?bot_type=3` (login step 1; no
    /// bearer token — none exists yet).
    pub async fn get_bot_qrcode(&self, base_url: &str) -> Result<BotQrCode, IlinkError> {
        let url = format!("{base_url}{GET_BOT_QRCODE_PATH}");
        let value = self
            .get_json(
                &url,
                &format!("bot_type={QR_BOT_TYPE}"),
                Duration::from_secs(15),
                "get_bot_qrcode",
            )
            .await?;
        let qrcode = value
            .get("qrcode")
            .and_then(Value::as_str)
            .filter(|q| !q.is_empty())
            .ok_or_else(|| IlinkError::Malformed("no qrcode in get_bot_qrcode response".into()))?
            .to_string();
        let qrcode_img_content = value
            .get("qrcode_img_content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Ok(BotQrCode {
            qrcode,
            qrcode_img_content,
        })
    }

    /// `GET /ilink/bot/get_qrcode_status?qrcode=<qrcode>` (login step
    /// 2; long-poll ~35 s server-side). Returns the verbatim body —
    /// status parsing lives in [`crate::wechat::login`] (the state
    /// machine, not the wire client).
    pub async fn get_qrcode_status(
        &self,
        base_url: &str,
        qrcode: &str,
        timeout: Duration,
    ) -> Result<Value, IlinkError> {
        let url = format!("{base_url}{GET_QRCODE_STATUS_PATH}");
        self.get_json(
            &url,
            &format!("qrcode={qrcode}"),
            timeout,
            "get_qrcode_status",
        )
        .await
    }

    /// One POST with the full iLink header set (fresh `X-WECHAT-UIN`
    /// per request per §2.2) and a per-request deadline.
    async fn post_json(
        &self,
        url: &str,
        token: &str,
        body: &Value,
        timeout: Duration,
        what: &str,
    ) -> Result<Value, IlinkError> {
        let mut request = self.http.post(url).json(body).timeout(timeout);
        request = ilink_headers(request, token);
        let response = request
            .send()
            .await
            .map_err(|e| transport_error(what, e))?
            .error_for_status()
            .map_err(|e| transport_error(what, e))?;
        response
            .json::<Value>()
            .await
            .map_err(|e| IlinkError::Malformed(format!("{what} body unreadable: {e}")))
    }

    /// One GET with the same header set (minus the bearer token when
    /// none exists yet — the login calls).
    async fn get_json(
        &self,
        url: &str,
        query: &str,
        timeout: Duration,
        what: &str,
    ) -> Result<Value, IlinkError> {
        // `query` is built from our own constants/ids, never operator
        // input, so plain string concatenation is safe here.
        let full = format!("{url}?{query}");
        let mut request = self.http.get(&full).timeout(timeout);
        // The login GETs carry no bearer token by construction (none
        // exists yet); an empty token adds no Authorization header.
        request = ilink_headers(request, "");
        let response = request
            .send()
            .await
            .map_err(|e| transport_error(what, e))?
            .error_for_status()
            .map_err(|e| transport_error(what, e))?;
        response
            .json::<Value>()
            .await
            .map_err(|e| IlinkError::Malformed(format!("{what} body unreadable: {e}")))
    }
}

impl Default for IlinkClient {
    fn default() -> Self {
        Self::new().expect("reqwest client builds without TLS configuration")
    }
}

/// `base_info` attached to every POST body (§2.2).
fn base_info() -> Value {
    json!({"channel_version": CHANNEL_VERSION})
}

/// Applies the §2.2 header set. `token` empty (the pre-login calls)
/// means no `Authorization` header.
fn ilink_headers(request: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
    let mut request = request
        .header("AuthorizationType", AUTHORIZATION_TYPE)
        .header("X-WECHAT-UIN", fresh_wechat_uin())
        .header("iLink-App-Id", ILINK_APP_ID)
        .header(
            "iLink-App-ClientVersion",
            ILINK_APP_CLIENT_VERSION.to_string(),
        )
        .header("bot_agent", BOT_AGENT);
    if !token.is_empty() {
        request = request.bearer_auth(token);
    }
    request
}

/// `X-WECHAT-UIN`: `base64(<random decimal uint32>)`, fresh per request
/// (§2.2). Randomness comes from a v4 UUID — no new dependency.
pub fn fresh_wechat_uin() -> String {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let n = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    base64_encode(n.to_string().as_bytes())
}

/// Standard-alphabet base64 with padding (the `X-WECHAT-UIN` encoding).
/// Hand-rolled to avoid a new dependency for one header.
pub fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = chunk.get(1).map(|b| u32::from(*b)).unwrap_or(0);
        let b2 = chunk.get(2).map(|b| u32::from(*b)).unwrap_or(0);
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(n >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Classifies a parsed `getupdates` body: `-14` → stale token, other
/// `ret != 0 || errcode != 0` → API error (§2 receive).
fn classify(value: &Value) -> Result<(), IlinkError> {
    let ret = value.get("ret").and_then(Value::as_i64).unwrap_or(0);
    let errcode = value.get("errcode").and_then(Value::as_i64).unwrap_or(0);
    if errcode == STALE_TOKEN_ERRCODE {
        return Err(IlinkError::StaleToken);
    }
    if ret != 0 || errcode != 0 {
        return Err(IlinkError::Api {
            ret,
            errcode,
            errmsg: value
                .get("errmsg")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string(),
        });
    }
    Ok(())
}

/// Parses an id field losslessly: a JSON string passes through, a JSON
/// integer (the uint64 wire form) becomes its exact decimal string.
/// Never through a float — `as_u64` on a serde integer is exact.
pub fn lossless_id(value: &Value, key: &str) -> Option<String> {
    match value.get(key) {
        Some(Value::String(s)) => (!s.is_empty()).then(|| s.clone()),
        Some(Value::Number(n)) => n.as_u64().map(|u| u.to_string()),
        _ => None,
    }
}

/// Transport failures → the typed pair the adapter maps (`Timeout` vs
/// everything else).
fn transport_error(what: &str, e: reqwest::Error) -> IlinkError {
    if e.is_timeout() {
        IlinkError::Timeout
    } else {
        IlinkError::Transport(format!("{what}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encode_matches_the_standard_alphabet() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn fresh_uin_is_standard_base64_of_a_decimal_u32() {
        // minimal standard decoder for the assertion
        fn decode(text: &str) -> Vec<u8> {
            const ALPHABET: &[u8; 64] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let bytes: Vec<u32> = text
                .bytes()
                .filter(|b| *b != b'=')
                .map(|b| {
                    ALPHABET
                        .iter()
                        .position(|a| *a == b)
                        .expect("base64 alphabet member") as u32
                })
                .collect();
            let mut out = Vec::new();
            for chunk in bytes.chunks(4) {
                let n = chunk.iter().fold(0u32, |acc, d| (acc << 6) | d)
                    << (6 * (4 - chunk.len()) as u32);
                out.push((n >> 16) as u8);
                if chunk.len() > 2 {
                    out.push((n >> 8) as u8);
                }
                if chunk.len() > 3 {
                    out.push(n as u8);
                }
            }
            out
        }
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let uin = fresh_wechat_uin();
            let decoded = decode(&uin);
            let text = String::from_utf8(decoded.clone()).expect("the uin encodes ASCII digits");
            let n: u32 = text.parse().expect("the uin encodes a decimal uint32");
            assert_eq!(n.to_string(), text, "round-trips exactly");
            seen.insert(uin);
        }
        assert!(
            seen.len() > 32,
            "the uin must be fresh (random) per request"
        );
    }

    #[test]
    fn lossless_id_passes_strings_and_exact_uint64s() {
        let max = json!({"message_id": u64::MAX});
        assert_eq!(
            lossless_id(&max, "message_id").as_deref(),
            Some("18446744073709551615"),
            "u64::MAX must survive without float rounding"
        );
        assert_eq!(
            lossless_id(&json!({"message_id": "16999999999999999999"}), "message_id").as_deref(),
            Some("16999999999999999999")
        );
        assert_eq!(lossless_id(&json!({"message_id": ""}), "message_id"), None);
        assert_eq!(lossless_id(&json!({}), "message_id"), None);
        assert_eq!(lossless_id(&json!({"message_id": 1.5}), "message_id"), None);
    }

    #[test]
    fn classify_routes_stale_tokens_api_errors_and_success() {
        assert_eq!(
            classify(&json!({"ret": 0, "errcode": -14, "errmsg": "stale"})),
            Err(IlinkError::StaleToken)
        );
        assert_eq!(
            classify(&json!({"ret": 5, "errcode": 0, "errmsg": "no"})),
            Err(IlinkError::Api {
                ret: 5,
                errcode: 0,
                errmsg: "no".into()
            })
        );
        assert_eq!(
            classify(&json!({"ret": 0, "errcode": 0, "msgs": []})),
            Ok(())
        );
    }
}
