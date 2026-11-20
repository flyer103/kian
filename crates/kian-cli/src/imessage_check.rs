//! `kian imessage check` — the iMessage bridge preflight
//! (CHANNEL-IMESSAGE §7.1).
//!
//! Read-only: it resolves the configured `type: imessage` channel,
//! proves the env-only bridge token is present, calls the bridge's
//! `GET /v1/health`, and reports the gates the operator must fix —
//! Full Disk Access (`database.ready`), `imsg rpc`, the protocol
//! version, and the `watch.participants` ↔ `topics` allowlist diff.
//!
//! It never sends a message, never writes the bridge state, and never
//! prints the token — on stdout or stderr, in text or `--json` mode.
//! Any failed gate exits non-zero with one clear message.

use std::path::PathBuf;
use std::time::Duration;

use kian_superagent::imessage::bridge::BridgeClient;
use kian_superagent::imessage::handle;
use serde_json::json;

use crate::config::{ImessageChannelSection, ServeConfig};

/// `kian imessage check [--config <path>] [--channel <name>] [--json]`.
pub struct ImessageCheckArgs {
    pub config: PathBuf,
    pub channel: Option<String>,
    pub json: bool,
}

impl Default for ImessageCheckArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("kian.yaml"),
            channel: None,
            json: false,
        }
    }
}

/// The `type: imessage` channel to check: the one named by `--channel`,
/// or the config's single imessage channel (config validation caps the
/// type at one).
fn resolve_channel<'a>(
    config: &'a ServeConfig,
    wanted: Option<&str>,
) -> Result<(String, &'a ImessageChannelSection), String> {
    let superagent = config.superagent.as_ref().ok_or_else(|| {
        "the config has no `superagent:` section — a `type: imessage` channel is required for \
         `kian imessage check`"
            .to_string()
    })?;
    let channels: Vec<(&String, &ImessageChannelSection)> = superagent
        .channels
        .iter()
        .filter_map(|(name, channel)| channel.imessage.as_ref().map(|section| (name, section)))
        .collect();
    match wanted {
        Some(wanted) => channels
            .iter()
            .find(|(name, _)| name.as_str() == wanted)
            .map(|(name, section)| (name.to_string(), *section))
            .ok_or_else(|| {
                format!(
                    "channel `{wanted}` is not a `type: imessage` channel in the config (found: \
                     {})",
                    channel_names(&channels)
                )
            }),
        None => match channels.as_slice() {
            [] => Err(
                "the config declares no `type: imessage` channel — `kian imessage check` needs \
                 one"
                .to_string(),
            ),
            [only] => Ok((only.0.to_string(), only.1)),
            many => Err(format!(
                "the config declares more than one `type: imessage` channel ({}) — name one with \
                 `--channel`",
                channel_names(many)
            )),
        },
    }
}

fn channel_names(channels: &[(&String, &ImessageChannelSection)]) -> String {
    if channels.is_empty() {
        "none".to_string()
    } else {
        channels
            .iter()
            .map(|(name, _)| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Runs the preflight and returns the rendered report (stdout text or
/// one JSON object). `Err` means at least one gate failed.
pub async fn run_check(args: &ImessageCheckArgs) -> Result<String, String> {
    let config = ServeConfig::load(&args.config).map_err(|e| e.to_string())?;
    let (name, section) = resolve_channel(&config, args.channel.as_deref())?;

    // The token is env-only. `to_runtime` is the assembly's single
    // conversion point; the check runs it so the operator sees the same
    // fail-closed errors serve would raise.
    let token = std::env::var(kian_superagent::IMESSAGE_BRIDGE_TOKEN_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    let runtime = section
        .to_runtime(&name, token)
        .map_err(|e| e.to_string())?;

    let client = BridgeClient::new(runtime.bridge_url.clone(), runtime.token.clone())
        .map_err(|e| e.to_string())?;
    let health = client
        .health(Duration::from_secs(10))
        .await
        .map_err(|e| format!("imessage bridge health failed: {e}"))?;

    let mut topics: Vec<String> = runtime.topics.keys().cloned().collect();
    topics.sort();
    let mut participants: Vec<String> = health
        .watch
        .participants
        .iter()
        .filter_map(|handle| handle::normalize_handle(handle))
        .collect();
    participants.sort();
    participants.dedup();
    let participants_only: Vec<String> = participants
        .iter()
        .filter(|handle| !topics.contains(handle))
        .cloned()
        .collect();
    let topics_only: Vec<String> = topics
        .iter()
        .filter(|handle| !participants.is_empty() && !participants.contains(handle))
        .cloned()
        .collect();

    let mut failures: Vec<String> = Vec::new();
    if !health.imsg.rpc {
        failures.push(
            "the bridge cannot speak `imsg rpc` — install/upgrade imsg (>= 0.11) on the \
             Messages Mac"
                .to_string(),
        );
    }
    if !health.database.ready {
        failures.push(format!(
            "the bridge cannot read the Messages database{} — grant Full Disk Access to the \
             bridge binary under System Settings → Privacy & Security → Full Disk Access",
            health
                .database
                .error
                .as_deref()
                .map(|error| format!(" ({error})"))
                .unwrap_or_default()
        ));
    }

    let warnings: Vec<String> = {
        let mut warnings = Vec::new();
        if health.automation == "denied" {
            warnings.push(
                "Automation → Messages is denied: sends will fail with -1743 until it is granted \
                 to the bridge binary (System Settings → Privacy & Security → Automation)"
                    .to_string(),
            );
        }
        if participants.is_empty() {
            warnings.push(
                "the bridge's watch.participants is empty: every conversation on the Mac is read \
                 (only allowlisted senders are admitted, but the others still transit the \
                 bridge) — set it to the kian topics handles"
                    .to_string(),
            );
        }
        if !topics_only.is_empty() {
            warnings.push(format!(
                "topics-only handles will never arrive: {} (the bridge's watch.participants does \
                 not include them)",
                topics_only.join(", ")
            ));
        }
        if !participants_only.is_empty() {
            warnings.push(format!(
                "bridge participants not in topics will be read then dropped: {}",
                participants_only.join(", ")
            ));
        }
        warnings
    };

    let report = json!({
        "ok": failures.is_empty(),
        "channel": name,
        "bridge_url": runtime.bridge_url,
        "protocol": health.protocol,
        "bridge_version": health.bridge_version,
        "imsg_version": health.imsg.version,
        "imsg_rpc": health.imsg.rpc,
        "database_ready": health.database.ready,
        "database_path": health.database.path,
        "automation": health.automation,
        "participants": participants,
        "topics": topics,
        "topics_only": topics_only,
        "participants_only": participants_only,
        "failures": failures,
        "warnings": warnings,
    });

    if args.json {
        let rendered =
            serde_json::to_string_pretty(&report).map_err(|e| format!("render report: {e}"))?;
        if failures.is_empty() {
            Ok(format!("{rendered}\n"))
        } else {
            Err(rendered)
        }
    } else {
        let mut out = String::new();
        out.push_str(&format!(
            "iMessage channel `{name}` → {} (protocol {}, bridge {})\n",
            runtime.bridge_url, health.protocol, health.bridge_version
        ));
        out.push_str(&format!(
            "  imsg: {} (rpc: {})\n",
            health.imsg.version, health.imsg.rpc
        ));
        out.push_str(&format!(
            "  database: {} ({})\n",
            health.database.ready, health.database.path
        ));
        out.push_str(&format!("  automation: {}\n", health.automation));
        out.push_str(&format!("  topics: {}\n", topics.join(", ")));
        out.push_str(&format!("  participants: {}\n", participants.join(", ")));
        for warning in &warnings {
            out.push_str(&format!("  WARNING: {warning}\n"));
        }
        if failures.is_empty() {
            out.push_str("  OK\n");
            Ok(out)
        } else {
            for failure in &failures {
                out.push_str(&format!("  FAILED: {failure}\n"));
            }
            Err(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServeConfig;

    /// A minimal valid super-agent config with the given extra channel
    /// entry spliced into `superagent.channels` (4-space indented).
    fn config_with(channel: &str) -> ServeConfig {
        let base = r#"listen: "127.0.0.1:7433"
storage:
  sqlite: ":memory:"
providers:
  p:
    base_url: "http://127.0.0.1:1/v1"
    api_key_env: P_KEY
agent:
  id: a
  model_slots:
    main: { provider: p, model: m }
  tools: [fs_read]
agents:
  router:
    id: router
    version: 1
    model_slots:
      router: { provider: p, model: m }
    tools: [spawn_worker, reply, deliver_structured]
  worker:
    id: worker
    version: 2
    model_slots:
      main: { provider: p, model: m }
    tools: [fs_list]
plugins: ["openai-compat", "local-tools", "superagent"]
superagent:
  router:
    session_id: "69d9e9b0-028c-4720-ae91-1f75fd8d472a"
    agent: router
    slot: router
  worker:
    agent: worker
  channels:
    mock-im:
      type: mock-im
recovery:
  rules:
    - class: unknown
      action: fail
"#;
        let yaml = base.replace(
            "    mock-im:\n      type: mock-im",
            &format!("{channel}    mock-im:\n      type: mock-im"),
        );
        ServeConfig::parse(&yaml).expect("config parses")
    }

    #[test]
    fn resolves_the_single_imessage_channel() {
        let config = config_with(
            "    imessage:\n      type: imessage\n      imessage:\n        bridge_url: \"http://127.0.0.1:7420\"\n        topics:\n          \"+14155551212\": imessage\n",
        );
        let (name, section) = resolve_channel(&config, None).expect("resolves");
        assert_eq!(name, "imessage");
        assert_eq!(section.bridge_url, "http://127.0.0.1:7420");
    }

    #[test]
    fn a_named_channel_must_be_imessage() {
        let config = config_with("");
        let err = resolve_channel(&config, Some("mock-im")).expect_err("refused");
        assert!(err.contains("not a `type: imessage` channel"), "{err}");
    }

    #[test]
    fn no_imessage_channel_is_an_honest_error() {
        let config = config_with("");
        let err = resolve_channel(&config, None).expect_err("refused");
        assert!(
            err.contains("declares no `type: imessage` channel"),
            "{err}"
        );
    }
}
