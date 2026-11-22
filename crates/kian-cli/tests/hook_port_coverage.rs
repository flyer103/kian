//! Port-coverage pin (PLUGIN-SPEC §4): the seven port methods at their
//! nine call sites, asserted against the source tree. Each row names
//! the call site's dispatch expression; if a call site disappears (a
//! refactor drops the consult, a port is unwired), the row's marker
//! no longer matches and this test fails naming the row. This is the
//! coverage half of the P2 acceptance — the behavioral half lives in
//! `kian-loop`'s integration tests (rows b–f), `kian-superagent`'s
//! worker/subscription tests (rows a, e) and `kian-dsh`'s bridge test
//! (row d″).

use std::path::{Path, PathBuf};

fn source(rel: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/ has a workspace parent");
    let path: PathBuf = root.join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

struct Site {
    row: &'static str,
    port: &'static str,
    file: &'static str,
    markers: &'static [&'static str],
}

const SITES: &[Site] = &[
    Site {
        row: "(a)",
        port: "admit/pre + admit/post, channel-borne",
        file: "kian-superagent/src/subscriptions.rs",
        markers: &[
            // One evaluation at the shaping boundary, before any inbox row.
            "bus.admit(&draft).await",
            // admit/post fires per admitted target with its receipt.
            "bus.admit_post(",
        ],
    },
    Site {
        row: "(a')",
        port: "admit/pre, API-borne (observe-only in v1)",
        file: "kian-cli/src/server.rs",
        markers: &[
            "self.hooks.admit(&draft).await",
            "kian_plugin::hooks::Origin::Api",
        ],
    },
    Site {
        row: "(b)",
        port: "turn/request, after the persisted primary body",
        file: "kian-loop/src/react.rs",
        markers: &["hooks.request(&meta).await"],
    },
    Site {
        row: "(c)",
        port: "turn/response, once per settled provider turn",
        file: "kian-loop/src/react.rs",
        markers: &["hooks.response(&view).await"],
    },
    Site {
        row: "(d)",
        port: "tool/pre, after evaluate_permission, before append/invoke",
        file: "kian-loop/src/react.rs",
        markers: &["hooks.tool_pre(&view).await"],
    },
    Site {
        row: "(d')",
        port: "tool/post, ReAct — executed and denied arms alike",
        file: "kian-loop/src/react.rs",
        markers: &["hooks.tool_post(&view).await"],
    },
    Site {
        row: "(d\")",
        port: "tool/post, dsh — crate-local port, post-append branch",
        file: "kian-dsh/src/driver.rs",
        markers: &["port.tool_post(", "crate::toolpost::ToolPostView"],
    },
    Site {
        row: "(e)",
        port: "settle/pre, WorkerLoopDriver before LoopOutcome::Settled",
        file: "kian-superagent/src/worker.rs",
        markers: &[
            // The gate consult in the Phase machine…
            "settle_gate(",
            // …and the bus dispatch with the caller-implemented SettleCtl.
            "bus.settle(&view, ctl.as_ref()).await",
        ],
    },
    Site {
        row: "(f)",
        port: "recover/decided, at the class → action resolution",
        file: "kian-loop/src/react.rs",
        markers: &["hooks.recover(&view).await"],
    },
];

#[test]
fn every_spec_4_call_site_is_wired() {
    assert_eq!(
        SITES.len(),
        9,
        "PLUGIN-SPEC §4 is seven ports at nine call sites"
    );
    let mut missing = Vec::new();
    for site in SITES {
        let text = source(site.file);
        for marker in site.markers {
            if !text.contains(marker) {
                missing.push(format!(
                    "row {} ({}) — `{}` no longer contains `{}`",
                    site.row, site.port, site.file, marker
                ));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "PLUGIN-SPEC §4 call-site coverage regression:\n{}",
        missing.join("\n")
    );
}

/// D4: the narrow waist gains nothing. `kian-core`, `kian-store` and
/// `kian-orchestrator` must not reference the plugin crate anywhere in
/// their sources (a dependency edge could not compile, but a doc or
/// stray reference would still be contract drift).
#[test]
fn core_crates_reference_no_plugin_hooks() {
    for file in [
        "kian-core/src/lib.rs",
        "kian-store/src/lib.rs",
        "kian-orchestrator/src/lib.rs",
    ] {
        let text = source(file);
        assert!(
            !text.contains("kian_plugin::hooks"),
            "D4: `{file}` must gain nothing from the hook-plugin round"
        );
    }
}

// ---------------------------------------------------------------------------
// §3.4.3 — the internal-producer allowlist (M4-a)
// ---------------------------------------------------------------------------

/// One file's expected count of internal inbox-admit producers — call
/// sites that write a durable input row **without** first consulting
/// `admit/pre` (§3.4.3's exclusions). Channel-borne input goes through
/// `subscriptions.rs`'s port consult (`bus.admit`) and API-borne input
/// through `server.rs`'s observe-only consult (`self.hooks.admit`);
/// both are excluded from the count by construction. A NEW internal
/// producer — a fresh `.admit(` on the inbox/engine in production
/// code, in a listed file or any other — fails this pin naming the
/// file, so the exclusion list can never grow silently.
struct ProducerFile {
    file: &'static str,
    /// Production-code admits (everything before the first
    /// `#[cfg(test)]`), excluding the two port consults.
    admits: usize,
}

const PRODUCER_FILES: &[ProducerFile] = &[
    ProducerFile {
        file: "kian-cli/src/async_bridge.rs",
        admits: 1,
    },
    ProducerFile {
        file: "kian-cli/src/memory.rs",
        admits: 0,
    },
    ProducerFile {
        // The API-borne producers (`submit`/`steer`/`queue`) plus the
        // memory-index admit. The cron resident dispatcher moved to
        // `cron_engine.rs` (row below); both are internal producers by the
        // §3.4.3 matrix and bypass `admit/pre` there and here.
        file: "kian-cli/src/server.rs",
        admits: 3,
    },
    ProducerFile {
        // The cron resident sink (`EngineTurn`): a scheduled firing is an
        // internal producer (operator config, same class as a watchdog timer),
        // so it bypasses `admit/pre` by the §3.4.3 matrix (CRON-MODEL §6.1).
        // ADR-059 (P4) adds the deferred delivery path through one shared
        // helper: the textual .admit( sites are the helper's engine call plus
        // its two callers (immediate deliver and deliver_at), one producer.
        file: "kian-cli/src/cron_engine.rs",
        admits: 3,
    },
    ProducerFile {
        file: "kian-cli/src/superagent.rs",
        admits: 2,
    },
    ProducerFile {
        file: "kian-superagent/src/policy_version.rs",
        admits: 1,
    },
    ProducerFile {
        file: "kian-superagent/src/router.rs",
        admits: 4,
    },
    ProducerFile {
        file: "kian-superagent/src/subscriptions.rs",
        admits: 1,
    },
    ProducerFile {
        file: "kian-superagent/src/watchdog.rs",
        admits: 2,
    },
    ProducerFile {
        file: "kian-superagent/src/worker.rs",
        admits: 5,
    },
];

/// The production text of one source file: everything before the first
/// `#[cfg(test)]` (this codebase's convention keeps test modules at
/// the tail or in dedicated test files).
fn production_source(rel: &str) -> String {
    let text = source(rel);
    match text.find("#[cfg(test)]") {
        Some(cut) => text[..cut].to_string(),
        None => text,
    }
}

fn count_internal_producers(text: &str) -> usize {
    text.lines()
        .filter(|l| l.contains(".admit("))
        // The two §4.1 port consults are the sanctioned surface, not
        // bypasses.
        .filter(|l| !l.contains("bus.admit(") && !l.contains("hooks.admit("))
        .count()
}

fn rs_files(dir: &Path, root: &Path, out: &mut Vec<String>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                rs_files(&path, root, out);
            } else if path.extension().is_some_and(|e| e == "rs")
                && let Ok(rel) = path.strip_prefix(root)
                && let Some(rel) = rel.to_str()
            {
                // Normalize to the forward-slash form the allowlist
                // uses, whatever the platform separator is.
                out.push(rel.replace(std::path::MAIN_SEPARATOR, "/"));
            }
        }
    }
}

/// §3.4.3/§17 (M4-a): every internal producer that bypasses the
/// admission port is on this test's allowlist. Fails naming the file
/// when a listed file's count drifts (a producer appeared or moved)
/// or when any other bundle source grows a production `.admit(`.
#[test]
fn internal_producers_bypass_the_admission_port() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/ has a workspace parent");
    let mut problems = Vec::new();

    let mut files = Vec::new();
    rs_files(&root.join("kian-cli/src"), root, &mut files);
    rs_files(&root.join("kian-superagent/src"), root, &mut files);
    let mut seen = std::collections::BTreeSet::new();
    for rel in &files {
        let text = production_source(rel);
        let count = count_internal_producers(&text);
        seen.insert(rel.clone());
        let expected = PRODUCER_FILES
            .iter()
            .find(|p| p.file == *rel)
            .map(|p| p.admits);
        match expected {
            Some(expected) if expected != count => problems.push(format!(
                "`{rel}`: {count} internal admit(s), allowlist says {expected}"
            )),
            None if count > 0 => problems.push(format!(
                "`{rel}`: {count} internal admit(s) and NO allowlist row — a new \
                 producer is bypassing the admission port"
            )),
            _ => {}
        }
    }
    for p in PRODUCER_FILES {
        if !seen.contains(p.file) {
            problems.push(format!(
                "allowlist row `{}` names a file that no longer exists",
                p.file
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "§3.4.3 internal-producer allowlist drift:\n{}",
        problems.join("\n")
    );
}

// ---------------------------------------------------------------------------
// §3.4.4 — the assembly rows (M4-d, static half)
// ---------------------------------------------------------------------------

/// §3.4.4 (M4-d): the rows that must NOT consult the bus never grow a
/// consultation. `RouterDriver` (worker.rs's sibling router.rs) and
/// `ServeLoopDriver` (superagent.rs's plain enum wrapper) hold no hook
/// bus, no settle gate and no port dispatch — the settle gate exists
/// exactly where `WorkerLoopDriver::with_hooks` is mounted. The
/// behavioral half: `worker.rs::non_worker_stream_never_reaches_the_gate`
/// (pass-through row) and `worker_scoped_narrowing_…` (worker rows).
#[test]
fn router_and_serve_loop_driver_never_consult_the_hook_bus() {
    let router = production_source("kian-superagent/src/router.rs");
    for needle in ["HookBus", "settle_gate", "SettleVerdict", "with_hooks"] {
        assert!(
            !router.contains(needle),
            "§3.4.4: RouterDriver sources must never reference `{needle}`"
        );
    }
    let superagent = production_source("kian-cli/src/superagent.rs");
    let serve = superagent
        .split("pub enum ServeLoopDriver")
        .nth(1)
        .expect("ServeLoopDriver is defined");
    // The enum's own body only (up to the closing brace of the item).
    let body = &serve[..serve.find('}').unwrap_or(serve.len())];
    for needle in ["HookBus", "hooks", "settle_gate", "SettleVerdict"] {
        assert!(
            !body.contains(needle),
            "§3.4.4: ServeLoopDriver's variants must never reference `{needle}`"
        );
    }
}

/// §3.4.4/§13.1 (B2's static half): the assembly mounts the ONE bus on
/// every worker-side driver it builds. Each construction site is
/// pinned by its **own region and its own exact count** (NEW-2): a
/// site that loses its mount — its `Some(bus)` arm reverting to the
/// bare constructor — drops that region's `with_hooks` count to 0 and
/// fails *this* row, instead of being absorbed by a sibling site's
/// match. The regions also exclude `ReActLoop::with_hooks_and_guard`
/// (the built-in assembly at the top of the file), so the
/// substring-collision that made the old `>= 3` pin unlosable cannot
/// recur. The behavioral half lives in
/// `worker.rs::worker_scoped_narrowing_…` and kian-loop's hook-port
/// integration tests.
#[test]
fn assembly_mounts_the_bus_on_every_worker_side() {
    let plugins = source("kian-cli/src/plugins.rs");
    // The hoisted, assembly-owned bus the worker sides read.
    assert!(
        plugins
            .contains("let hook_bus = ctx\n            .resolve::<kian_plugin::hooks::HookBus>()"),
        "the assembly resolves the bus once, before the worker template's loop"
    );
    // The two per-worker-id factories capture the assembly bus.
    for marker in [
        "side_hooks = hook_bus.clone()",
        "refuse_hooks = hook_bus.clone()",
    ] {
        assert!(
            plugins.contains(marker),
            "both per-worker-id factories must capture the assembly bus: `{marker}` missing"
        );
    }
    // The slice of `plugins.rs` between two anchors (panic names the
    // anchor if a refactor moves it — the pin asks for an update, not
    // a silent pass).
    let region = |start: &str, end: &str| -> String {
        let i = plugins
            .find(start)
            .unwrap_or_else(|| panic!("anchor `{start}` vanished from plugins.rs"));
        let j = plugins[i..]
            .find(end)
            .unwrap_or_else(|| panic!("anchor `{end}` vanished after `{start}`"));
        plugins[i..i + j].to_string()
    };
    // Site 1 — the worker template's inner loop: the exact site B2
    // reported. The bus-present arm must mount `with_hooks`.
    let template = region("let worker_loop = match &hook_bus", "let worker_inner");
    assert_eq!(
        template.matches("ReActLoop::with_hooks").count(),
        1,
        "site 1 (worker template inner loop): the bus-present arm mounts the ReAct ports"
    );
    assert_eq!(
        template.matches("ReActLoop::with_config").count(),
        1,
        "site 1: exactly one bare-embedding fallback arm"
    );
    // Site 2 — the static side's wrapper (the settle gate).
    let static_side = region("let worker_side = {", "let mut side_factory");
    assert_eq!(
        static_side.matches("WorkerLoopDriver::with_hooks").count(),
        1,
        "site 2 (static side wrapper): the bus-present arm mounts the settle gate"
    );
    assert_eq!(
        static_side.matches("WorkerLoopDriver::new").count(),
        1,
        "site 2: exactly one bare-embedding fallback arm"
    );
    // Site 3 — the lazy per-worker-id factory: inner loop AND wrapper.
    let lazy = region(
        "side_factory = Some(Arc::new(move |id:",
        "let refuse_registry",
    );
    assert_eq!(
        lazy.matches("ReActLoop::with_hooks").count(),
        1,
        "site 3a (lazy side inner loop): mounts the ReAct ports"
    );
    assert_eq!(
        lazy.matches("WorkerLoopDriver::with_hooks").count(),
        1,
        "site 3b (lazy side wrapper): mounts the settle gate"
    );
    // Site 4 — the refuse-side factory: inner loop AND wrapper.
    let refuse = region(
        "refuse_side = Some(Arc::new(move |refusal:",
        "The driver's known-ids source",
    );
    assert_eq!(
        refuse.matches("ReActLoop::with_hooks").count(),
        1,
        "site 4a (refuse side inner loop): mounts the ReAct ports"
    );
    assert_eq!(
        refuse.matches("WorkerLoopDriver::with_hooks").count(),
        1,
        "site 4b (refuse side wrapper): mounts the settle gate"
    );
}
