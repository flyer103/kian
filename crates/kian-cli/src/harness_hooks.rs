//! Assembly-side adapters between the bridge ports and the plugin bus (HARNESS-BRIDGE §4.4,
//! [ADR-045](design/decisions/ADR-045-harness-approvals-at-tool-pre.md)).
//!
//! `kian-harness` stays on the narrow waist (no `kian-plugin` type); the driver exposes a plain
//! [`ToolPrePort`], and this module turns a harness approval into the plugin contract's
//! `tool/pre` view (`surface:"harness"`) and reduces the bus verdict back to the driver's.

use futures::future::BoxFuture;
use kian_harness::{ToolPreCall, ToolPrePort, ToolPreVerdict};
use kian_plugin::hooks::{
    FrameInfo, HookBus, PermissionView, ToolCallView, ToolVerdict, bounded_value, serialized,
    sha256_hex,
};

/// The `tool/pre` port over the process's one hook bus.
pub struct BusToolPre {
    bus: HookBus,
}

impl BusToolPre {
    #[must_use]
    pub fn new(bus: HookBus) -> Self {
        Self { bus }
    }
}

impl ToolPrePort for BusToolPre {
    fn tool_pre(&self, call: ToolPreCall) -> BoxFuture<'static, ToolPreVerdict> {
        let bus = self.bus.clone();
        Box::pin(async move {
            let view = view_for(&call);
            match bus.tool_pre(&view).await {
                ToolVerdict::Allow => ToolPreVerdict::Allow,
                ToolVerdict::Deny { plugin, reason } => ToolPreVerdict::Deny { plugin, reason },
            }
        })
    }
}

/// The plugin contract's view for one harness-gated call: the same bounds, digest and
/// permission shape the ReAct site uses, with `surface:"harness"` and the honest floor frame
/// (the bridge holds no worker binding).
pub(crate) fn view_for(call: &ToolPreCall) -> ToolCallView {
    let bytes = serialized(&call.arguments);
    let (arguments, arguments_truncated) = bounded_value(&call.arguments);
    ToolCallView {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        iteration: call.iteration,
        arguments,
        arguments_bytes: bytes.len() as u64,
        arguments_sha256: sha256_hex(&bytes),
        arguments_truncated,
        permission: PermissionView {
            evaluated: if call.config_allow { "allow" } else { "deny" }.to_owned(),
            policy: "configured".to_owned(),
            matched: call.config_matched.clone(),
            reason: None,
        },
        surface: "harness".to_owned(),
        frame: Some(FrameInfo::new(call.session)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::model::SessionId;
    use serde_json::json;

    #[test]
    fn a_harness_call_becomes_a_surface_harness_view() {
        let session = SessionId::new();
        let call = ToolPreCall {
            session,
            call_id: "t1".into(),
            name: "Bash".into(),
            iteration: 3,
            arguments: json!({"command": "git status"}),
            config_allow: true,
            config_matched: "Bash(*git status*)".into(),
        };
        let view = view_for(&call);
        assert_eq!(view.surface, "harness");
        assert_eq!(view.call_id, "t1");
        assert_eq!(view.name, "Bash");
        assert_eq!(view.iteration, 3);
        assert_eq!(view.permission.evaluated, "allow");
        assert_eq!(view.permission.policy, "configured");
        assert_eq!(view.permission.matched, "Bash(*git status*)");
        assert!(!view.arguments_truncated);
        assert_eq!(view.arguments_sha256.len(), 64);
        assert_eq!(view.frame.as_ref().map(|f| f.session), Some(session));
    }
}
