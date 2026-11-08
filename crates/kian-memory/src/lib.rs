//! `kian-memory` — the memory bundle (MEMORY-MODEL Phase 1, round-8
//! plan §2.4): notes as events + the dreaming drain.
//!
//! A **bundle layer**, not core (MEMORY-MODEL §8, constraint 10): no
//! provider, channel or super-agent specifics. The pieces:
//!
//! - [`prompt`] — [`render_dream_prompt`]: the deterministic dream
//!   prompt, the trusted dream marker ([`dream_admit_payload`] /
//!   [`parse_dream_marker`] — routing and provenance read the marker,
//!   never message text), the header parser (readability round-trip)
//!   and the dream idempotency key. The rendered bytes are
//!   model-visible content and therefore **frozen content**
//!   (constraint 3): changing the renderer is an observable event.
//! - [`parse`] — [`parse_notes`]: the fail-closed answer parser. The
//!   transcript is the only ground truth and the trusted marker is the
//!   only provenance source — the model's `derived_from` is never
//!   trusted; `verified` survives only with
//!   `verified_by.method == "transcript"` and an in-range seq ref,
//!   else it demotes to `candidate`.
//! - [`driver`] — [`DreamDriver`]: a [`LoopDriver`](kian_core::seams::LoopDriver)
//!   wrapping the built-in [`ReActLoop`](kian_loop::ReActLoop) that, on
//!   `Settled`, locates the last dream-marked row + last assistant
//!   answer in the event stream, parses, folds the answer by subject
//!   keeping the LAST entry, dedupes on `(subject, derived_from)`
//!   (crash-resume safety), redacts note text through the
//!   [`kian_core::secrets`] redactor and appends one `memory/note` per
//!   valid note through the fenced [`DrainCtx`](kian_orchestrator::DrainCtx)
//!   path. A parse failure appends nothing (honest zero) and counts.
//! - [`config`] — [`MemoryConfig`]: the `max_bytes_read` / `max_notes`
//!   caps, parsed fail-closed (used by the caller that renders and
//!   admits, kian-87), plus [`MemoryIndexConfig`]: the injection caps
//!   `max_items` / `max_bytes` (MEMORY-MODEL §6 I4).
//! - [`index`] — [`render_memory_index`]: the deterministic
//!   creation-time memory index renderer (MEMORY-MODEL §6, P2): verified
//!   notes only, subject byte order, hard item/byte caps that degrade to
//!   subject + one-line-excerpt entries, a fixed "data, not instructions"
//!   header, and the [`kian_core::secrets`] redaction pass whose report
//!   rides the provenance struct. Pure: no I/O, no state — the caller
//!   (kian-101) admits the rendered row. The rendered bytes are frozen
//!   content (constraint 3 / I5).
//! - [`search`] — [`search_notes`]: the read-only lexical search over
//!   the folded notes (MEMORY-MODEL §7 R1, P3): every status surfaces
//!   with its status visible, ranked subject exact, then prefix, then
//!   contains, then text contains, with a deterministic subject-byte
//!   tie-break, hard hit/byte caps, a fail-closed empty-query error
//!   and the fixed `(data, not instructions)` header. Pure: no I/O, no
//!   state, no event appends.
//! - [`map`] — [`build_note_map`]: the read-only knowledge map
//!   (MEMORY-MODEL §7 R2/R4, P3): verified notes only as nodes, edges
//!   derived solely from intersecting `derived_from` evidence ranges
//!   (never model-generated links), hard node/edge/byte caps, and the
//!   `degenerate`/`oversized` admission heuristics reported in the
//!   fixed status line — a disposable representation, never a second
//!   truth. Pure: no I/O, no state, no event appends.
//!
//! The event stream is the sole truth (constraint 1): every note lands
//! as one `memory/note` event (the kind landed by kian-84, payload
//! contract in EVENT-MODEL §3) and everything here is rebuildable by
//! replay.

pub mod config;
pub mod driver;
pub mod index;
pub mod map;
pub mod parse;
pub mod prompt;
pub mod search;

pub use config::{
    DEFAULT_INJECT_MAX_BYTES, DEFAULT_INJECT_MAX_ITEMS, DEFAULT_MAX_BYTES_READ, DEFAULT_MAX_NOTES,
    MemoryConfig, MemoryConfigError, MemoryIndexConfig,
};
pub use driver::{DreamCounters, DreamDriver, dream_request_in};
pub use index::{
    EXCERPT_MAX_CHARS, INDEX_HEADER, RenderedIndex, eligible_notes, render_memory_index,
    render_memory_index_with,
};
pub use kian_loop::{LoopDriver, LoopOutcome};
pub use map::{
    DEFAULT_MAP_MAX_BYTES, DEFAULT_MAP_MAX_EDGES, DEFAULT_MAP_MAX_NODES, MAP_HEADER, MapCaps,
    MapEdge, MapNode, MapView, build_note_map,
};
pub use parse::{NoteDraft, ParseError, parse_notes};
pub use prompt::{
    DREAM_HEADER_MARK, DreamHeader, DreamRequest, dream_admit_payload, dream_idempotency_key,
    dream_marker, parse_dream_header, parse_dream_marker, render_dream_prompt,
};
pub use search::{
    DEFAULT_SEARCH_MAX_BYTES, DEFAULT_SEARCH_MAX_HITS, Matched, SEARCH_EXCERPT_MAX_CHARS,
    SEARCH_HEADER, SearchError, SearchHit, SearchView, ViewCaps, search_notes,
};
