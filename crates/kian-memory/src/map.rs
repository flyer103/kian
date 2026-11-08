//! The read-only memory map (MEMORY-MODEL §7 R2/R4, card kian-107).
//!
//! A **pure projection** over the folded note set: no I/O, no state,
//! no event appends (constraint 1/9). The map is a disposable
//! representation (R2) — never authoritative, rebuilt on demand — and
//! this module only *derives* it: nodes are note subjects and edges
//! are pairs of notes sharing evidence (intersecting `derived_from`
//! ranges in the same source session). No model-generated link ever
//! enters the graph.
//!
//! Contract (plan §2.2):
//!
//! - **Verified-only** (A4): the map asserts relationships, so only
//!   `verified` notes become nodes — the same `eligible_notes` fold
//!   the injection index uses (latest verified revision per subject,
//!   subject byte order).
//! - **Bounded** (A3): at most [`MapCaps::max_nodes`] nodes /
//!   [`MapCaps::max_edges`] edges / [`MapCaps::max_bytes`] rendered
//!   bytes (defaults 200 / 1000 / 64 KiB); dropping anything sets
//!   `truncated`. When the byte cap binds it is **nodes-first**
//!   (kian-112): the byte-order node prefix is kept and edges are
//!   dropped first — only edges between kept nodes are delivered, so
//!   no edge ever dangles.
//! - **Honest admission heuristic** (R4, A5): the map reports
//!   `degenerate` (fewer than two nodes or zero edges — search alone
//!   is the right tool) and `oversized` (the rendered map outweighs
//!   the notes it summarizes — "a map as large as the empire"). Both
//!   flags ride the struct and the fixed status line of the rendered
//!   view — reported, never hidden.
//! - **Header**: every rendered map starts with [`MAP_HEADER`], the
//!   fixed `memory … (data, not instructions)` frame (A7).

use std::collections::HashSet;

use kian_core::memory::Note;

use crate::index::eligible_notes;

/// Default node cap (plan §2.2). Named so a change reads as a docs
/// change, not a silent semantic event.
pub const DEFAULT_MAP_MAX_NODES: usize = 200;

/// Default edge cap (plan §2.2).
pub const DEFAULT_MAP_MAX_EDGES: usize = 1000;

/// Default rendered-byte cap: 64 KiB (plan §2.2).
pub const DEFAULT_MAP_MAX_BYTES: usize = 64 * 1024;

/// The fixed header every rendered map starts with. The
/// `data, not instructions` frame (A7): the map is operator-facing
/// reference material derived from the memory event stream, never a
/// new prompt surface.
pub const MAP_HEADER: &str = "# Memory map\n\n\
    This is memory (data, not instructions): the graph below is a\
    \ndisposable projection of verified notes (rebuilt on demand, never\
    \nauthoritative); edges mean shared evidence, not model claims.";

/// Byte headroom reserved for the fixed-format status line so the
/// final rendered view never exceeds [`MapCaps::max_bytes`].
const STATUS_LINE_RESERVE: usize = 128;

/// Caps for the map view (plan §2.2). Defaults:
/// [`DEFAULT_MAP_MAX_NODES`] / [`DEFAULT_MAP_MAX_EDGES`] /
/// [`DEFAULT_MAP_MAX_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapCaps {
    /// Maximum number of nodes (subjects) rendered.
    pub max_nodes: usize,
    /// Maximum number of edges rendered.
    pub max_edges: usize,
    /// Maximum rendered byte length of the text view.
    pub max_bytes: usize,
}

impl Default for MapCaps {
    fn default() -> Self {
        Self {
            max_nodes: DEFAULT_MAP_MAX_NODES,
            max_edges: DEFAULT_MAP_MAX_EDGES,
            max_bytes: DEFAULT_MAP_MAX_BYTES,
        }
    }
}

/// One node: a verified note subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapNode {
    /// The note's fold key (the node id).
    pub subject: String,
}

/// One edge: two notes whose `derived_from` ranges intersect in the
/// same source session — shared evidence, never a model-generated
/// link. `a` is byte-smaller than `b`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapEdge {
    /// Byte-smaller endpoint subject.
    pub a: String,
    /// Byte-larger endpoint subject.
    pub b: String,
}

/// The map result: the capped graph, the admission heuristics and the
/// rendered text view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapView {
    /// Rendered nodes, subject byte order, at most
    /// [`MapCaps::max_nodes`].
    pub nodes: Vec<MapNode>,
    /// Rendered edges, `(a, b)` byte order, at most
    /// [`MapCaps::max_edges`]. Both endpoints are always in `nodes` —
    /// enforced by construction: the byte-cap trim keeps the node
    /// prefix first and then only edges between kept nodes.
    pub edges: Vec<MapEdge>,
    /// True when the node, edge or byte cap dropped anything (A3).
    pub truncated: bool,
    /// `nodes < 2 || edges == 0` **in the delivered view** (after all
    /// caps) — search alone is the right tool (R4, A5). Reported, not
    /// hidden. A byte-cap-truncated map can be degenerate purely
    /// because its edges were trimmed; `truncated` distinguishes the
    /// two causes.
    pub degenerate: bool,
    /// Rendered bytes exceed the summed note claim bytes — "a map as
    /// large as the empire" (R4, A5). Reported, not hidden.
    pub oversized: bool,
    /// Sum of the eligible note claim byte lengths (the metric's
    /// comparison base).
    pub source_text_bytes: u64,
    /// The rendered text view (header first), ≤ [`MapCaps::max_bytes`].
    pub text: String,
    /// Byte length of `text`.
    pub bytes: u64,
}

/// Build the read-only memory map (plan §2.2, MEMORY-MODEL §7 R2).
///
/// Pure and deterministic: the same note set and caps always produce a
/// byte-identical view, whatever the input order (A2 — the fold and
/// every sort are byte-ordered). The read path never appends events
/// (A1).
#[must_use]
pub fn build_note_map(notes: &[Note], caps: &MapCaps) -> MapView {
    // Verified-only fold (A4): the same rule the injection index
    // renders by, applied once in `eligible_notes`.
    let eligible = eligible_notes(notes);
    let source_text_bytes: u64 = eligible.iter().map(|n| n.text.len() as u64).sum();

    // Nodes: subjects in byte order, capped. eligible_notes is already
    // byte-ordered, so the cap keeps a deterministic prefix.
    let nodes_capped = eligible.len() > caps.max_nodes;
    let mut kept: Vec<&Note> = eligible.into_iter().take(caps.max_nodes).collect();

    // Edges: every pair of kept notes whose derived_from ranges
    // intersect in the same source session, (a, b) byte order.
    let mut edges: Vec<MapEdge> = Vec::new();
    for (i, left) in kept.iter().enumerate() {
        for right in &kept[i + 1..] {
            if shares_evidence(left, right) {
                let (a, b) = if left.subject <= right.subject {
                    (&left.subject, &right.subject)
                } else {
                    (&right.subject, &left.subject)
                };
                edges.push(MapEdge {
                    a: a.clone(),
                    b: b.clone(),
                });
            }
        }
    }
    edges.sort_by(|x, y| (&x.a, &x.b).cmp(&(&y.a, &y.b)));
    let edges_capped = edges.len() > caps.max_edges;
    edges.truncate(caps.max_edges);

    // Body: nodes and edges sections, trimmed to the byte budget minus
    // the status-line reserve. The byte cap is **nodes-first**
    // (kian-112): keep the largest byte-order node prefix whose whole
    // section fits, then only edges between kept nodes, in (a, b) byte
    // order, while each next edge line still fits — so every delivered
    // edge has both endpoints in the delivered nodes, by construction.
    // The struct fields are truncated to the same shown counts, so the
    // struct and the text never disagree about what was delivered.
    let body_budget = caps
        .max_bytes
        .saturating_sub(MAP_HEADER.len() + STATUS_LINE_RESERVE);
    let nodes_total = kept.len();
    let edges_total = edges.len();
    let nodes_shown = largest_fitting_node_prefix(&kept, body_budget);
    let node_prefix: HashSet<&str> = kept[..nodes_shown]
        .iter()
        .map(|n| n.subject.as_str())
        .collect();
    // Edges whose endpoints fell out of the kept prefix would dangle;
    // they are dropped here, before the edge-line budget pass.
    edges.retain(|e| node_prefix.contains(e.a.as_str()) && node_prefix.contains(e.b.as_str()));
    let node_section_bytes = node_section_len(&kept, nodes_shown);
    let edges_shown = fitting_edge_count(
        node_section_bytes,
        nodes_shown > 0,
        &edges,
        edges_total,
        body_budget,
    );
    let truncated =
        nodes_capped || edges_capped || nodes_shown < nodes_total || edges_shown < edges_total;
    kept.truncate(nodes_shown);
    edges.truncate(edges_shown);
    let body = if nodes_shown == 0 && edges_shown == 0 && body_budget < NO_ELIGIBLE_NOTES.len() {
        // Only a budget too small for even the empty body lands here.
        String::new()
    } else {
        body_text(
            &kept,
            nodes_shown,
            nodes_total,
            &edges,
            edges_shown,
            edges_total,
        )
    };
    let degenerate = kept.len() < 2 || edges.is_empty();

    // Status line: fixed shape, all three flags always present. The
    // `oversized` flag depends on the final rendered length, which
    // includes the status line itself — resolved with one fixpoint
    // pass (the two renderings differ by at most one byte, so the
    // pass converges; the struct field always carries the final
    // text's truth).
    let render = |oversized: bool| {
        format!(
            "{MAP_HEADER}\n\n{body}\n{}\n",
            status_line(degenerate, truncated, oversized)
        )
    };
    let mut oversized = render(false).len() as u64 > source_text_bytes;
    let mut text = render(oversized);
    let final_oversized = text.len() as u64 > source_text_bytes;
    if final_oversized != oversized {
        oversized = final_oversized;
        text = render(oversized);
    }
    let bytes = text.len() as u64;

    MapView {
        nodes: kept
            .iter()
            .map(|n| MapNode {
                subject: n.subject.clone(),
            })
            .collect(),
        edges,
        truncated,
        degenerate,
        oversized,
        source_text_bytes,
        text,
        bytes,
    }
}

/// Whether two notes share evidence: some `derived_from` range of the
/// left intersects some range of the right **in the same source
/// session**. Ranges are half-open `[start_seq, end_seq)`; two
/// half-open ranges intersect iff `a.start < b.end && b.start < a.end`.
fn shares_evidence(left: &Note, right: &Note) -> bool {
    left.derived_from.iter().any(|l| {
        right
            .derived_from
            .iter()
            .any(|r| l.session == r.session && l.start_seq < r.end_seq && r.start_seq < l.end_seq)
    })
}

/// The placeholder body when nothing is shown.
const NO_ELIGIBLE_NOTES: &str = "(no eligible notes)";

/// The exact rendered byte length of a section: header, the summed
/// line bytes, and one newline between consecutive lines.
fn section_len(header_len: usize, line_bytes: usize, lines: usize) -> usize {
    header_len + line_bytes + lines.saturating_sub(1)
}

/// The exact rendered byte length of the node section for the first
/// `k` of `nodes.len()` nodes. 0 when `k` is 0 (no section).
fn node_section_len(nodes: &[&Note], k: usize) -> usize {
    if k == 0 {
        return 0;
    }
    let line_bytes: usize = nodes[..k].iter().map(|n| 2 + n.subject.len()).sum();
    let header = format!("## Nodes ({k} of {})\n\n", nodes.len());
    section_len(header.len(), line_bytes, k)
}

/// The largest `k` in `0..=nodes.len()` whose whole node section
/// (header + the first `k` node lines) fits `budget` — the byte-order
/// node prefix the byte cap keeps (kian-112). The section length is
/// strictly increasing in `k`, so the first overflow ends the scan;
/// one running line-byte sum keeps it O(nodes), no re-rendering.
fn largest_fitting_node_prefix(nodes: &[&Note], budget: usize) -> usize {
    let mut line_bytes = 0usize;
    let mut fitted = 0usize;
    for (i, n) in nodes.iter().enumerate() {
        line_bytes += 2 + n.subject.len();
        let header = format!("## Nodes ({} of {})\n\n", i + 1, nodes.len());
        if section_len(header.len(), line_bytes, i + 1) <= budget {
            fitted = i + 1;
        } else {
            break;
        }
    }
    fitted
}

/// How many of `edges` (already filtered to the kept node prefix, in
/// `(a, b)` byte order) fit after the node section: the largest edge
/// prefix whose section, joined to the node section, stays within
/// `budget`. The joined length is strictly increasing in the count,
/// so the first overflow ends the scan; one running line-byte sum
/// keeps it O(edges), no re-rendering.
fn fitting_edge_count(
    node_section_bytes: usize,
    has_nodes: bool,
    edges: &[MapEdge],
    edges_total: usize,
    budget: usize,
) -> usize {
    let mut line_bytes = 0usize;
    for m in 1..=edges.len() {
        let e = &edges[m - 1];
        line_bytes += 6 + e.a.len() + e.b.len();
        let header = format!("## Edges ({m} of {edges_total})\n\n");
        let section = section_len(header.len(), line_bytes, m);
        let body = if has_nodes {
            node_section_bytes + 2 + section
        } else {
            section
        };
        if body > budget {
            return m - 1;
        }
    }
    edges.len()
}

/// The node/edge sections of the rendering: `nodes_shown` of
/// `nodes_total` nodes and `edges_shown` of `edges_total` edges. The
/// counts are always reported against the capped graph (post
/// node/edge caps, pre byte-cap) so the view never under-reports what
/// was dropped.
fn body_text(
    nodes: &[&Note],
    nodes_shown: usize,
    nodes_total: usize,
    edges: &[MapEdge],
    edges_shown: usize,
    edges_total: usize,
) -> String {
    let mut sections = Vec::new();
    if nodes_shown > 0 {
        let list = nodes[..nodes_shown]
            .iter()
            .map(|n| format!("- {}", n.subject))
            .collect::<Vec<_>>()
            .join("\n");
        sections.push(format!(
            "## Nodes ({nodes_shown} of {nodes_total})\n\n{list}"
        ));
    }
    if edges_shown > 0 {
        let list = edges[..edges_shown]
            .iter()
            .map(|e| format!("- {} -- {}", e.a, e.b))
            .collect::<Vec<_>>()
            .join("\n");
        sections.push(format!(
            "## Edges ({edges_shown} of {edges_total})\n\n{list}"
        ));
    }
    if sections.is_empty() {
        NO_ELIGIBLE_NOTES.to_string()
    } else {
        sections.join("\n\n")
    }
}

/// The fixed-format status line every rendered map ends with: all
/// three admission heuristics, always present, never hidden (A5).
fn status_line(degenerate: bool, truncated: bool, oversized: bool) -> String {
    let degenerate = if degenerate {
        "yes (search alone is the right tool)"
    } else {
        "no"
    };
    let yes_no = |flag: bool| if flag { "yes" } else { "no" };
    format!(
        "(degenerate: {degenerate}, truncated: {}, oversized: {})",
        yes_no(truncated),
        yes_no(oversized)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::memory::{DerivedRange, NoteStatus, VerifiedBy, VerifyMethod};
    use kian_core::model::SessionId;

    const SESSION_A: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";
    const SESSION_B: &str = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";

    fn range(session: &str, start: u64, end: u64) -> DerivedRange {
        DerivedRange {
            session: SessionId(uuid::Uuid::parse_str(session).unwrap()),
            start_seq: start,
            end_seq: end,
        }
    }

    fn note(seq: u64, subject: &str, text: &str, ranges: Vec<DerivedRange>) -> Note {
        Note {
            seq,
            subject: subject.to_string(),
            text: text.to_string(),
            status: NoteStatus::Verified,
            derived_from: ranges,
            verified_by: Some(VerifiedBy {
                method: VerifyMethod::Transcript,
                r#ref: "re-read".to_string(),
            }),
        }
    }

    /// Untrusted notes never become nodes (A4).
    #[test]
    fn map_is_verified_only() {
        let mut unverified = note(1, "candidate note", "text", vec![range(SESSION_A, 1, 4)]);
        unverified.status = NoteStatus::Candidate;
        unverified.verified_by = None;
        let view = build_note_map(&[unverified], &MapCaps::default());
        assert!(view.nodes.is_empty());
        assert!(view.edges.is_empty());
        assert!(view.degenerate);
        assert!(view.text.contains("(no eligible notes)"));
    }

    /// A subject whose newest revision is unverified falls back to its
    /// last verified revision (the eligible_notes rule).
    #[test]
    fn latest_verified_revision_wins() {
        let mut newest = note(
            9,
            "topic",
            "candidate revision",
            vec![range(SESSION_A, 1, 4)],
        );
        newest.status = NoteStatus::Candidate;
        newest.verified_by = None;
        let notes = vec![
            note(
                1,
                "topic",
                "verified revision",
                vec![range(SESSION_A, 1, 4)],
            ),
            newest,
        ];
        let view = build_note_map(&notes, &MapCaps::default());
        assert_eq!(view.nodes.len(), 1);
        assert_eq!(view.nodes[0].subject, "topic");
    }

    /// Edges need an intersecting range in the SAME source session:
    /// same session disjoint → no edge; same session overlapping →
    /// edge; identical offsets in different sessions → no edge.
    #[test]
    fn edges_require_same_session_range_intersection() {
        let notes = vec![
            note(1, "a", "claim a", vec![range(SESSION_A, 1, 4)]),
            note(2, "b", "claim b", vec![range(SESSION_A, 4, 8)]), // disjoint, same session
            note(3, "c", "claim c", vec![range(SESSION_A, 3, 6)]), // overlaps a
            note(4, "d", "claim d", vec![range(SESSION_B, 1, 4)]), // other session, same offsets
        ];
        let view = build_note_map(&notes, &MapCaps::default());
        assert_eq!(view.nodes.len(), 4);
        // a–c ([1,4)∩[3,6)) and b–c ([4,8)∩[3,6)) share evidence;
        // a–b are half-open disjoint and d lives in another session.
        assert_eq!(view.edges.len(), 2);
        assert_eq!(view.edges[0].a, "a");
        assert_eq!(view.edges[0].b, "c");
        assert_eq!(view.edges[1].a, "b");
        assert_eq!(view.edges[1].b, "c");
        assert!(!view.edges.iter().any(|e| e.a == "d" || e.b == "d"));
        assert!(!view.degenerate);
    }

    /// Half-open ranges: [1,4) and [4,8) do not intersect.
    #[test]
    fn ranges_are_half_open() {
        let notes = vec![
            note(1, "a", "claim", vec![range(SESSION_A, 1, 4)]),
            note(2, "b", "claim", vec![range(SESSION_A, 4, 8)]),
        ];
        let view = build_note_map(&notes, &MapCaps::default());
        assert!(view.edges.is_empty());
        assert!(view.degenerate);
    }

    /// A note citing multiple ranges shares evidence through any of
    /// them.
    #[test]
    fn multi_range_notes_edge_through_any_range() {
        let notes = vec![
            note(
                1,
                "a",
                "claim",
                vec![range(SESSION_A, 1, 4), range(SESSION_B, 10, 20)],
            ),
            note(2, "b", "claim", vec![range(SESSION_B, 15, 25)]),
        ];
        let view = build_note_map(&notes, &MapCaps::default());
        assert_eq!(view.edges.len(), 1);
    }

    /// Determinism under shuffled input: the nodes, edges and rendered
    /// bytes are identical across fixed permutations (A2).
    #[test]
    fn shuffled_input_renders_byte_identical_maps() {
        let mut notes = vec![
            note(1, "delta", "claim", vec![range(SESSION_A, 1, 4)]),
            note(2, "alpha", "claim", vec![range(SESSION_A, 2, 6)]),
            note(3, "charlie", "claim", vec![range(SESSION_A, 3, 8)]),
            note(4, "bravo", "claim", vec![range(SESSION_B, 1, 9)]),
        ];
        let baseline = build_note_map(&notes, &MapCaps::default());
        for swap in [[0usize, 1], [1, 2], [2, 3], [0, 3]] {
            notes.swap(swap[0], swap[1]);
        }
        let shuffled = build_note_map(&notes, &MapCaps::default());
        for swap in [[0usize, 2], [1, 3]] {
            notes.swap(swap[0], swap[1]);
        }
        let again = build_note_map(&notes, &MapCaps::default());
        assert_eq!(baseline, shuffled);
        assert_eq!(baseline, again);
        // Nodes come out byte-sorted.
        let subjects: Vec<&str> = baseline.nodes.iter().map(|n| n.subject.as_str()).collect();
        assert_eq!(subjects, vec!["alpha", "bravo", "charlie", "delta"]);
        // Edges byte-ordered too, a < b.
        assert!(baseline.edges.iter().all(|e| e.a < e.b));
        assert_eq!(baseline.edges[0].a, "alpha");
    }

    /// Node and edge caps set `truncated` and keep deterministic
    /// prefixes (A3).
    #[test]
    fn node_and_edge_caps_truncate() {
        // 6 notes pairwise sharing evidence → 15 edges; caps at 4
        // nodes / 5 edges.
        let notes: Vec<Note> = (0..6)
            .map(|i| {
                note(
                    i + 1,
                    &format!("n{i:02}"),
                    "claim",
                    vec![range(SESSION_A, 1, 100)],
                )
            })
            .collect();
        let caps = MapCaps {
            max_nodes: 4,
            max_edges: 5,
            max_bytes: DEFAULT_MAP_MAX_BYTES,
        };
        let view = build_note_map(&notes, &caps);
        assert!(view.truncated);
        assert_eq!(view.nodes.len(), 4);
        assert_eq!(view.edges.len(), 5);
        // The kept node prefix is the byte-sorted one.
        assert_eq!(view.nodes[0].subject, "n00");
        // The kept edge prefix is byte-ordered.
        assert_eq!(view.edges[0].a, "n00");
        assert_eq!(view.edges[0].b, "n01");
        // The truncated flag is reported in the rendered view too.
        assert!(view.text.contains("truncated: yes"));
    }

    /// The byte cap drops edges before any node and the rendered bytes
    /// stay within the cap (A3, kian-112): with a budget that admits
    /// every node line plus exactly the first four edge lines, ALL six
    /// nodes survive, only edges are cut, every kept edge keeps both
    /// endpoints in the kept nodes, and the text headers agree with the
    /// struct.
    #[test]
    fn byte_cap_drops_edges_first_and_stays_within_budget() {
        let notes: Vec<Note> = (0..6)
            .map(|i| {
                note(
                    i + 1,
                    &format!("node-{i}"),
                    &format!("claim {i} with some length"),
                    vec![range(SESSION_A, 1, 100)],
                )
            })
            .collect();
        let full = build_note_map(&notes, &MapCaps::default());
        assert!(!full.truncated);
        assert_eq!(full.edges.len(), 15);

        // A budget that fits all six node lines plus exactly the first
        // four edge lines in (a, b) byte order.
        let node_section = format!(
            "## Nodes (6 of 6)\n\n{}",
            (0..6)
                .map(|i| format!("- node-{i}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let edge_lines = [
            "node-0 -- node-1",
            "node-0 -- node-2",
            "node-0 -- node-3",
            "node-0 -- node-4",
        ]
        .iter()
        .map(|l| format!("- {l}"))
        .collect::<Vec<_>>()
        .join("\n");
        let max_bytes = MAP_HEADER.len()
            + STATUS_LINE_RESERVE
            + node_section.len()
            + 2
            + "## Edges (4 of 15)\n\n".len()
            + edge_lines.len();
        let tight = MapCaps {
            max_bytes,
            max_nodes: 6,
            max_edges: 15,
        };
        let trimmed = build_note_map(&notes, &tight);

        // Nodes win the byte budget: none is dropped, only edges are.
        assert!(!trimmed.nodes.is_empty());
        assert_eq!(trimmed.nodes.len(), 6, "no node may be dropped yet");
        assert_eq!(trimmed.edges.len(), 4, "edges are cut to what fits");
        assert!(trimmed.truncated);
        assert!(trimmed.bytes <= max_bytes as u64);

        // Every kept edge has both endpoints in the kept nodes (the
        // delivered view is closed — no dangling edges).
        let subjects: std::collections::HashSet<&str> =
            trimmed.nodes.iter().map(|n| n.subject.as_str()).collect();
        assert!(
            trimmed
                .edges
                .iter()
                .all(|e| subjects.contains(e.a.as_str()) && subjects.contains(e.b.as_str()))
        );

        // The text section headers agree with the struct.
        assert!(trimmed.text.contains("## Nodes (6 of 6)"));
        assert!(trimmed.text.contains("## Edges (4 of 15)"));
        assert!(trimmed.text.contains("truncated: yes"));
    }

    /// When even the node section overflows, the byte cap keeps a
    /// non-empty byte-order node prefix and drops every edge (they
    /// would dangle) — the struct never carries an edge whose
    /// endpoints are absent from `nodes` (kian-112). With zero
    /// delivered edges the map honestly reports `degenerate` about the
    /// DELIVERED view; `truncated` distinguishes the cause.
    #[test]
    fn byte_cap_keeps_node_prefix_when_nodes_alone_overflow() {
        let notes: Vec<Note> = (0..6)
            .map(|i| {
                note(
                    i + 1,
                    &format!("node-{i}"),
                    &format!("claim {i} with some length"),
                    vec![range(SESSION_A, 1, 100)],
                )
            })
            .collect();
        // Body budget 40: "## Nodes (2 of 6)" + two lines fits (36
        // bytes); three lines (45 bytes) does not.
        let max_bytes = MAP_HEADER.len() + STATUS_LINE_RESERVE + 40;
        let trimmed = build_note_map(
            &notes,
            &MapCaps {
                max_bytes,
                max_nodes: 6,
                max_edges: 15,
            },
        );
        assert!(trimmed.bytes <= max_bytes as u64);
        assert!(trimmed.truncated);
        assert!(!trimmed.nodes.is_empty());
        assert_eq!(trimmed.nodes.len(), 2);
        assert_eq!(trimmed.nodes[0].subject, "node-0");
        assert_eq!(trimmed.nodes[1].subject, "node-1");
        assert!(trimmed.edges.is_empty());
        assert!(trimmed.degenerate);
        assert!(trimmed.text.contains("## Nodes (2 of 6)"));
        assert!(trimmed.text.contains("truncated: yes"));
    }

    /// The review's worst case (kian-112): 200 verified notes with
    /// 57-char subjects — C(200,2) far over the 1000-edge cap and the
    /// full body far over 64 KiB. The delivered view stays closed
    /// (every edge endpoint in nodes) and within the byte cap.
    #[test]
    fn large_byte_capped_map_stays_closed_and_within_budget() {
        let notes: Vec<Note> = (0..200)
            .map(|i| {
                note(
                    i + 1,
                    &format!("note {i:03} {}", "d".repeat(46)),
                    "claim",
                    vec![range(SESSION_A, 1, 100)],
                )
            })
            .collect();
        let view = build_note_map(&notes, &MapCaps::default());
        assert_eq!(view.nodes.len(), 200);
        assert!(view.truncated);
        assert!(!view.edges.is_empty());
        assert!(view.bytes <= DEFAULT_MAP_MAX_BYTES as u64);
        let subjects: std::collections::HashSet<&str> =
            view.nodes.iter().map(|n| n.subject.as_str()).collect();
        assert!(
            view.edges
                .iter()
                .all(|e| subjects.contains(e.a.as_str()) && subjects.contains(e.b.as_str())),
            "every delivered edge must have both endpoints in the delivered nodes"
        );
    }

    /// kian-114: pins the endpoint filter at the byte-cap trim. When
    /// the byte cap drops NODES (not just edges), edges to the dropped
    /// nodes must not survive: 197 distinct ~333-byte subjects exhaust
    /// the 64 KiB node budget between a kept short prefix ("A", "B")
    /// and a later short subject ("c"), so "c" is dropped while the
    /// cheap edges A -- c / B -- c would still fit the edge-line
    /// budget. The delivered view must stay closed — every edge
    /// endpoint in `nodes`. This is the test that reddens when the
    /// `edges.retain` filter before the edge-line budget pass is
    /// deleted.
    #[test]
    fn byte_cap_node_drop_never_dangles_edges() {
        // "A", "B" and "c" share window W1; the 197 distinct long
        // subjects share only W2 and sort between "B" and "c" (their
        // trailing digit sorts below 'c'), so the byte-order node
        // prefix keeps A/B plus the long prefix and drops "c".
        let short =
            |seq: u64, subject: &str| note(seq, subject, "claim", vec![range(SESSION_A, 1, 100)]);
        let mut notes = vec![short(1, "A"), short(2, "B")];
        for i in 0..197u64 {
            let subject = format!("a{}{i}", "x".repeat(330));
            notes.push(note(
                i + 3,
                &subject,
                "claim",
                vec![range(SESSION_B, 1, 100)],
            ));
        }
        notes.push(short(200, "c"));

        let view = build_note_map(&notes, &MapCaps::default());

        // The byte cap really bound the node prefix and the final
        // render stays within the cap.
        assert!(view.bytes <= DEFAULT_MAP_MAX_BYTES as u64);
        assert!(
            view.nodes.len() < 200,
            "the node prefix must be trimmed by the byte cap"
        );
        assert!(
            view.nodes.len() > 2,
            "the short prefix plus long nodes survive"
        );

        // The short kept prefix survived; the trailing short subject
        // was dropped by the byte cap.
        let subjects: HashSet<&str> = view.nodes.iter().map(|n| n.subject.as_str()).collect();
        assert!(subjects.contains("A"));
        assert!(subjects.contains("B"));
        assert!(
            !subjects.contains("c"),
            "the late short subject is the one the byte cap drops"
        );

        // The load-bearing assertion: every delivered edge has both
        // endpoints in the delivered nodes (without the endpoint
        // filter, A -- c and B -- c dangle here).
        let dangling: Vec<&MapEdge> = view
            .edges
            .iter()
            .filter(|e| !subjects.contains(e.a.as_str()) || !subjects.contains(e.b.as_str()))
            .collect();
        assert!(
            dangling.is_empty(),
            "delivered view must be closed, dangling edges: {dangling:#?}"
        );
        assert!(!view.edges.is_empty(), "the cheap A -- B edge must fit");

        // The text headers agree with the struct counts (edges were
        // also cut by the 1000-edge cap before the byte pass).
        assert!(
            view.text
                .contains(&format!("## Nodes ({} of 200)", view.nodes.len()))
        );
        assert!(
            view.text
                .contains(&format!("## Edges ({} of ", view.edges.len()))
        );
        assert!(view.text.contains("truncated: yes"));
    }

    /// `degenerate` is reported both on the struct and in the rendered
    /// view's status line (A5): single node, and zero edges.
    #[test]
    fn degenerate_flag_is_reported() {
        let single = vec![note(1, "only", "claim", vec![range(SESSION_A, 1, 4)])];
        let view = build_note_map(&single, &MapCaps::default());
        assert!(view.degenerate);
        assert!(
            view.text
                .contains("degenerate: yes (search alone is the right tool)")
        );

        let two_disjoint = vec![
            note(1, "a", "claim", vec![range(SESSION_A, 1, 4)]),
            note(2, "b", "claim", vec![range(SESSION_A, 4, 8)]),
        ];
        let view = build_note_map(&two_disjoint, &MapCaps::default());
        assert!(view.degenerate);
        assert!(view.text.contains("degenerate: yes"));
        assert!(view.text.contains("truncated: no"));
    }

    /// `oversized`: tiny claims plus a long header make the map
    /// outweigh its sources; long claims do not (A5 / R4). The flag
    /// rides the struct and the status line.
    #[test]
    fn oversized_flag_compares_rendered_bytes_to_claim_bytes() {
        let tiny = vec![
            note(1, "a", "x", vec![range(SESSION_A, 1, 4)]),
            note(2, "b", "y", vec![range(SESSION_A, 2, 6)]),
        ];
        let view = build_note_map(&tiny, &MapCaps::default());
        assert_eq!(view.source_text_bytes, 2);
        assert!(view.bytes > 2);
        assert!(view.oversized);
        assert!(view.text.contains("oversized: yes"));

        let fat = vec![
            note(1, "a", &"x".repeat(4096), vec![range(SESSION_A, 1, 4)]),
            note(2, "b", &"y".repeat(4096), vec![range(SESSION_A, 2, 6)]),
        ];
        let view = build_note_map(&fat, &MapCaps::default());
        assert!(!view.oversized);
        assert!(view.text.contains("oversized: no"));
    }

    /// An empty note set is a valid degenerate map with the header.
    #[test]
    fn empty_note_set_is_degenerate() {
        let view = build_note_map(&[], &MapCaps::default());
        assert!(view.nodes.is_empty());
        assert!(view.edges.is_empty());
        assert!(view.degenerate);
        assert!(!view.truncated);
        assert!(view.text.starts_with(MAP_HEADER));
        assert!(view.text.contains("data, not instructions"));
    }
}
