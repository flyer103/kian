//! Worker fs tool-schema parity (ADR-023): the remote fs tools advertise
//! byte-identical schemas to the local built-ins, and the refusing side
//! mounts the same three names. A drift here would change a bound worker
//! session's prompt prefix and the model's local/remote view.

use kian_core::seams::ToolProvider;

#[test]
fn remote_fs_schemas_match_the_local_builtin_fs_tools() {
    let root = std::env::temp_dir();
    let local: Vec<_> = kian_loop::tools::LocalTools::new(&root)
        .schemas()
        .into_iter()
        .filter(|s| s.name.starts_with("fs_"))
        .collect();
    let remote = kian_exec::RemoteFsTool::fs_schemas();
    assert_eq!(local.len(), 3, "local fs surface is three tools");
    assert_eq!(
        local, remote,
        "remote fs tools must be schema-identical to the local built-ins"
    );
}

#[test]
fn refusing_fs_tool_mounts_the_same_three_schemas() {
    let refusal = kian_cli::superagent::Refusal::Malformed;
    let tool = kian_cli::superagent::RefusingFsTool::new(refusal);
    assert_eq!(tool.schemas(), kian_exec::RemoteFsTool::fs_schemas());
    assert!(tool.refusal().message_for("fs_read").contains("fs refused"));
}
