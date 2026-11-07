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
//!   admits, kian-87).
//!
//! The event stream is the sole truth (constraint 1): every note lands
//! as one `memory/note` event (the kind landed by kian-84, payload
//! contract in EVENT-MODEL §3) and everything here is rebuildable by
//! replay.

pub mod config;
pub mod driver;
pub mod parse;
pub mod prompt;

pub use config::{DEFAULT_MAX_BYTES_READ, DEFAULT_MAX_NOTES, MemoryConfig, MemoryConfigError};
pub use driver::{DreamCounters, DreamDriver, dream_request_in};
pub use kian_loop::{LoopDriver, LoopOutcome};
pub use parse::{NoteDraft, ParseError, parse_notes};
pub use prompt::{
    DREAM_HEADER_MARK, DreamHeader, DreamRequest, dream_admit_payload, dream_idempotency_key,
    dream_marker, parse_dream_header, parse_dream_marker, render_dream_prompt,
};
