//! In-memory [`BridgeCtx`] for tests: the same shape the adapter and driver suites drive
//! against, with no store, no engine and no clock.

use std::collections::VecDeque;
use std::sync::Mutex;

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::ctx::{BridgeCtx, BridgeCtxError};

/// One in-memory drain state.
#[derive(Default)]
struct Mem {
    events: Vec<Event>,
    seq: u64,
    inputs: VecDeque<InputRow>,
    in_turn: bool,
    interrupt: bool,
}

/// A minimal drain context: appends to memory, takes queued inputs, reports the stream.
pub struct MemoryCtx {
    session: SessionId,
    state: Mutex<Mem>,
}

impl MemoryCtx {
    #[must_use]
    pub fn new(session: SessionId) -> Self {
        Self {
            session,
            state: Mutex::new(Mem {
                seq: 0,
                ..Mem::default()
            }),
        }
    }

    /// Queues one eligible input row (payload verbatim).
    pub fn push_input(&self, payload: Value) {
        let mut state = self.state.lock().expect("state");
        let row = InputRow {
            id: Uuid::new_v4(),
            session: self.session,
            payload,
            admitted_at: 0,
            eligible_at: 0,
            idempotency_key: None,
        };
        state.inputs.push_back(row);
    }

    /// Requests an interrupt for this drain.
    pub fn request_interrupt(&self) {
        self.state.lock().expect("state").interrupt = true;
    }

    /// The committed stream.
    #[must_use]
    pub fn committed(&self) -> Vec<Event> {
        self.state.lock().expect("state").events.clone()
    }

    /// Kinds of the committed stream, in order (test assertions).
    #[must_use]
    pub fn kinds(&self) -> Vec<EventKind> {
        self.committed().into_iter().map(|e| e.kind).collect()
    }

    fn append_inner(&self, kind: EventKind, bytes: Value) -> Result<Event, BridgeCtxError> {
        let mut state = self.state.lock().expect("state");
        state.seq += 1;
        let event = Event {
            seq: state.seq,
            session: self.session,
            epoch: Epoch(1),
            ts: 0,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        };
        state.events.push(event.clone());
        Ok(event)
    }
}

impl BridgeCtx for MemoryCtx {
    fn session(&self) -> SessionId {
        self.session
    }

    fn epoch(&self) -> Epoch {
        Epoch(1)
    }

    fn in_turn(&self) -> bool {
        self.state.lock().expect("state").in_turn
    }

    fn events(&self) -> BoxFuture<'_, Result<Vec<Event>, BridgeCtxError>> {
        Box::pin(async move { Ok(self.state.lock().expect("state").events.clone()) })
    }

    fn append(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'_, Result<Event, BridgeCtxError>> {
        Box::pin(async move { self.append_inner(kind, bytes) })
    }

    fn begin_turn(&self, bytes: Value) -> BoxFuture<'_, Result<Event, BridgeCtxError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state");
            state.in_turn = true;
            drop(state);
            self.append_inner(EventKind::TurnStart, bytes)
        })
    }

    fn end_turn(&self, bytes: Value) -> BoxFuture<'_, Result<Event, BridgeCtxError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state");
            state.in_turn = false;
            drop(state);
            self.append_inner(EventKind::TurnEnd, bytes)
        })
    }

    fn take_eligible_inputs(&self) -> BoxFuture<'_, Result<Vec<InputRow>, BridgeCtxError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state");
            if state.in_turn {
                return Err(BridgeCtxError::NotAtSafeBoundary);
            }
            let rows: Vec<InputRow> = state.inputs.drain(..).collect();
            drop(state);
            for row in &rows {
                let payload = input_payload(&row.payload);
                self.append_inner(EventKind::InputAdmitted, payload)?;
            }
            Ok(rows)
        })
    }

    fn has_eligible_inputs(&self) -> BoxFuture<'_, Result<bool, BridgeCtxError>> {
        Box::pin(async move { Ok(!self.state.lock().expect("state").inputs.is_empty()) })
    }

    fn interrupt_requested(&self) -> bool {
        self.state.lock().expect("state").interrupt
    }
}

/// The engine's row→event payload convention (same as the driver's delivery convention).
fn input_payload(row: &Value) -> Value {
    if let Some(obj) = row.as_object() {
        if obj.get("synthetic").and_then(Value::as_bool) == Some(true)
            && let Some(content) = obj.get("content")
        {
            return json!({"role": "user", "content": content, "synthetic": true});
        }
        if obj.len() == 2
            && let (Some(content), Some(_spill)) = (obj.get("content"), obj.get("spill"))
        {
            return json!({"role": "user", "content": content});
        }
    }
    json!({"role": "user", "content": row})
}
