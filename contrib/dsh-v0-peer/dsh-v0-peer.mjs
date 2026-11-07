#!/usr/bin/env node
// dsh-v0-peer — the ecosystem half of the kian bridge.
//
// Adapts the stock `dsh --profile sdk` JSON-RPC 2.0 server (newline-
// delimited stdio) onto kian's frozen bridge contract v0 (ADR-010):
//
//   stdout -> {"v":0,"type":"ready","logical_format":0} first, then one
//             {"v":0,"type":"session-event","event":{kind,payload,ts}}
//             line per adapted dsh session-log event;
//   stdin <- {"v":0,"type":"input"|"steer","content":...} and
//             {"v":0,"type":"interrupt"} control frames.
//
// The kian side (crates/kian-dsh/src/mapping.rs) is FROZEN: this peer
// adapts TO the contract, never the reverse. No npm dependencies —
// node: builtins only.
//
// Env knobs (all optional):
//   DSH_BIN dsh binary (default: `dsh` from PATH)
//   DSH_PEER_CWD cwd sent in `initialize` (default: process.cwd())
//   DSH_PEER_PROVIDER provider route (default: "deepseek-official" — the
//                     adapter the sdk profile registers; "deepseek" is NOT
//                     registered and initialize rejects it)
//   DSH_PEER_MODEL model name (default: "deepseek-chat")
//   DSH_PEER_SESSION dsh session id (default: "kian-" + sha256(cwd)[0:16]
//                     — stable per working directory, so a restarted peer
//                     continues the same dsh session and dsh's own durable
//                     log is the replay)
//
// Honesty notes (full table in README.md):
// - The SDK has NO cancel method: an `interrupt` arriving while a dsh step
//   is open kills the child so the turn settles as an attempt with no
//   assistant/message (interrupted, never completed); an `interrupt` at an
//   idle boundary cancels nothing and is logged to stderr only.
// - The SDK emits `request/header` (config + tools) inside each step
//   BEFORE dispatch, but folds settled request records there — successful
//   steps carry no `assistant/attempt`. The pre-flight rule (ADR-010 §3)
//   needs the step's request bytes before any response frame, so this peer
//   synthesizes the v0 `assistant/attempt` from the durable facts it can
//   observe at pre-flight time: the request/header verbatim plus the
//   model-visible surface assembled so far. That reconstruction is the
//   `request` payload; it is recorded, never invented.

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createInterface } from 'node:readline';

const DSH_BIN = process.env.DSH_BIN || 'dsh';
const CWD = process.env.DSH_PEER_CWD || process.cwd();
const PROVIDER = process.env.DSH_PEER_PROVIDER || 'deepseek-official';
const MODEL = process.env.DSH_PEER_MODEL || 'deepseek-chat';
const SESSION_ID =
  process.env.DSH_PEER_SESSION ||
  'kian-' + createHash('sha256').update(CWD).digest('hex').slice(0, 16);

const log = (...args) => process.stderr.write(`[dsh-v0-peer] ${args.join(' ')}\n`);

// --- v0 stdout side -------------------------------------------------------

function emitFrame(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function emitSessionEvent(kind, payload, ts) {
  emitFrame({ v: 0, type: 'session-event', event: { kind, payload, ts } });
}

// Ready goes out IMMEDIATELY: the kian driver must not block on dsh boot.
emitFrame({ v: 0, type: 'ready', logical_format: 0 });

// --- dsh JSON-RPC child ---------------------------------------------------

const child = spawn(DSH_BIN, ['--profile', 'sdk'], {
  stdio: ['pipe', 'pipe', 'inherit'],
});
log(`spawned ${DSH_BIN} --profile sdk (session ${SESSION_ID})`);

child.on('error', (err) => {
  log(`FATAL: cannot spawn ${DSH_BIN}: ${err.message}`);
  process.exit(1);
});

let nextId = 1;
const pending = new Map(); // json-rpc id -> {resolve, reject}

function call(method, params) {
  return new Promise((resolve, reject) => {
    const id = nextId++;
    pending.set(id, { resolve, reject });
    const line = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    child.stdin.write(line + '\n');
  });
}

// --- adaptation state -----------------------------------------------------

// The model-visible surface so far (used to reconstruct the assembled
// request at pre-flight time; kept from the same session.event stream).
const surface = []; // {role, content, tool_call_id?}
let lastHeader = null; // latest request/header payload (EpochHeader)

// v0 step tracking mirrors the kian mapper's: one open step at a time,
// step/end buffered until its outcome is derivable.
let stepOpen = false;
let stepToolCalls = 0;
let pendingStepEnd = null; // {ts}

// Session lifecycle for the shutdown decision.
let initialized = false;
let promptsSent = 0;
let turnsEnded = 0;
let shuttingDown = false;
let interruptedMidStep = false;

const queuedPrompts = []; // content strings waiting for initialize

function assembledRequest() {
  // Durable reconstruction of the request this step is about to send:
  // the request/header verbatim plus the model-visible surface as the
  // peer observed it. Recorded facts only.
  return {
    header: lastHeader,
    messages: surface.map((m) => ({ ...m })),
  };
}

function flushPendingStepEnd(outcome, toolCalls, ts) {
  if (!pendingStepEnd) return;
  const payload = { outcome };
  if (outcome === 'tool_calls') payload.tool_calls = toolCalls;
  emitSessionEvent('step/end', payload, ts);
  pendingStepEnd = null;
}

function beginShutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  call('shutdown', undefined)
    .catch((err) => log(`shutdown request failed: ${err}`))
    .finally(() => {
      child.stdin.end();
      // Belt and braces: do not linger if dsh ignores the shutdown.
      const killTimer = setTimeout(() => {
        log('dsh did not exit after shutdown; killing');
        child.kill('SIGKILL');
      }, 5000);
      killTimer.unref();
    });
}

// --- SDK session.event -> v0 session-event --------------------------------

function onSessionEvent(event) {
  if (!event || typeof event.type !== 'string') {
    log('undecodable session event dropped:', JSON.stringify(event));
    return;
  }
  const kind = event.type;
  const ts = typeof event.time === 'number' ? event.time : Date.now();
  const data = event.data ?? {};

  switch (kind) {
    case 'turn/start':
      emitSessionEvent(kind, { turn: data.turn }, ts);
      break;

    case 'turn/end': {
      const reason = data.reason?.kind ?? 'unknown';
      // The buffered step/end outcome is only derivable now (the SDK
      // step/end carries no outcome; the turn/end reason does).
      if (pendingStepEnd) {
        let outcome;
        if (reason === 'completed') {
          outcome = stepToolCalls > 0 ? 'tool_calls' : 'final';
        } else if (reason === 'error' || reason === 'blocked') {
          outcome = 'error';
          if (reason === 'blocked') {
            log('turn/end reason "blocked" has no v0 counterpart; mapped to error (documented closest call)');
          }
        } else if (reason === 'aborted' || reason === 'interrupted' || reason === 'forked') {
          outcome = 'cancelled';
          if (reason === 'forked') log('turn/end reason "forked" should not appear live; mapped to cancelled');
        } else if (reason === 'max-tokens') {
          outcome = 'final';
          log('turn/end reason "max-tokens" has no v0 counterpart; mapped to final (documented closest call)');
        } else {
          outcome = 'error';
          log(`unknown turn/end reason "${reason}" mapped to error`);
        }
        flushPendingStepEnd(outcome, stepToolCalls, ts);
      }
      emitSessionEvent(kind, { turn: data.turn, reason: data.reason }, ts);
      turnsEnded++;
      // Episode complete and nothing in flight: the SDK-blessed end of
      // the runtime is `shutdown` (no session/close exists).
      if (initialized && promptsSent <= turnsEnded && !interruptedMidStep) {
        beginShutdown();
      }
      break;
    }

    case 'step/start':
      if (pendingStepEnd) {
        // The next step of the same turn: the buffered step ended with
        // tool executions feeding it.
        flushPendingStepEnd('tool_calls', stepToolCalls, ts);
      }
      emitSessionEvent(kind, {}, ts);
      stepOpen = true;
      stepToolCalls = 0;
      break;

    case 'step/end':
      // Buffered: outcome arrives with the turn/end reason (or the next
      // step/start).
      pendingStepEnd = { ts };
      stepOpen = false;
      break;

    case 'user/message': {
      // data IS the UserMessage. Producer kinds other than 'user' mark
      // harness-injected (synthetic) context — carry the strippable
      // marker across (A6).
      const payload = { content: data.content };
      if (data.source?.kind && data.source.kind !== 'user') payload.synthetic = true;
      emitSessionEvent(kind, payload, ts);
      surface.push({ role: 'user', content: data.content });
      break;
    }

    case 'system/message':
      emitSessionEvent(kind, { content: data.message?.content }, ts);
      surface.push({ role: 'system', content: data.message?.content });
      break;

    case 'assistant/message':
      emitSessionEvent(kind, { content: data.message?.content }, ts);
      surface.push({ role: 'assistant', content: data.message?.content });
      break;

    case 'assistant/attempt':
      // A failed/retried/cancelled attempt that committed no message.
      // The SDK embeds the response stream only; the request bytes are
      // the same durable reconstruction as the pre-flight synthesis.
      emitSessionEvent(kind, { status: 'settled-without-message', request: assembledRequest() }, ts);
      break;

    case 'tool/call':
      emitSessionEvent(
        kind,
        { id: data.callId, name: data.name, arguments: data.arguments },
        ts,
      );
      stepToolCalls++;
      break;

    case 'tool/result':
      emitSessionEvent(
        kind,
        {
          call_id: data.message?.toolCallId,
          output: data.message?.content,
          is_error: data.message?.isError === true,
        },
        ts,
      );
      surface.push({
        role: 'tool',
        tool_call_id: data.message?.toolCallId,
        content: data.message?.content,
      });
      break;

    case 'request/header':
      // Appended inside the step BEFORE dispatch — the pre-flight
      // moment. Synthesize the v0 assistant/attempt (request bytes)
      // here so persist-what-you-send holds across the boundary.
      lastHeader = data.header;
      if (stepOpen) {
        emitSessionEvent(
          'assistant/attempt',
          { status: `pre-flight/${data.reason ?? 'unknown'}`, request: assembledRequest() },
          ts,
        );
      } else {
        log('request/header outside an open step (initial header?); not synthesized into an attempt');
      }
      break;

    case 'developer/message':
    case 'request/context':
    case 'session/end-seed':
      // Real durable kinds with no contract-v0 counterpart (the ADR-010
      // table has no row) — left OUT of the v0 stream, logged, never
      // silently. developer/message stays in the surface reconstruction
      // because it is model-visible.
      if (kind === 'developer/message') {
        surface.push({ role: 'developer', content: data.message?.content });
      }
      log(`durable kind "${kind}" has no contract-v0 counterpart; dropped from the v0 stream`);
      break;

    default:
      if (kind.startsWith('plugin:')) {
        log(`plugin record "${kind}" has no contract-v0 counterpart; dropped from the v0 stream`);
      } else {
        log(`unknown session event "${kind}" dropped from the v0 stream`);
      }
      break;
  }
}

// --- child stdout (JSON-RPC) ----------------------------------------------

createInterface({ input: child.stdout }).on('line', (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    log(`non-JSON line from dsh dropped: ${line.slice(0, 200)}`);
    return;
  }
  if (msg.id !== undefined && (msg.result !== undefined || msg.error !== undefined)) {
    const waiter = pending.get(msg.id);
    if (waiter) {
      pending.delete(msg.id);
      if (msg.error) waiter.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else waiter.resolve(msg.result);
    } else {
      log(`unmatched json-rpc response id=${msg.id}`);
    }
    return;
  }
  if (msg.method) {
    switch (msg.method) {
      case 'session.event': {
        const params = msg.params ?? {};
        if (params.sessionId !== SESSION_ID) {
          // Events from other sessions (e.g. subagent children) have no
          // contract-v0 counterpart in this bridge session.
          log(`session.event for foreign session ${params.sessionId} dropped`);
          break;
        }
        onSessionEvent(params.event);
        break;
      }
      case 'session.status':
        log(`session.status ${msg.params?.sessionId} -> ${msg.params?.status}`);
        break;
      case 'subagent.started':
      case 'subagent.finished':
        log(`${msg.method} has no contract-v0 counterpart; dropped from the v0 stream`);
        break;
      default:
        log(`unknown notification ${msg.method} dropped`);
        break;
    }
  }
});

child.on('exit', (code, signal) => {
  for (const waiter of pending.values()) {
    waiter.reject(new Error(`dsh exited (code ${code}, signal ${signal})`));
  }
  pending.clear();
  if (interruptedMidStep) {
    log(`dsh killed for interrupt (code ${code}, signal ${signal}); turn settles interrupted`);
    process.exit(0);
  }
  if (shuttingDown) {
    process.exit(code === null ? 0 : code);
  }
  log(`dsh exited unexpectedly (code ${code}, signal ${signal})`);
  process.exit(1);
});

// --- v0 stdin (control frames from kian) -----------------------------------

async function promptOnce(text) {
  await call('session/prompt', {
    sessionId: SESSION_ID,
    contentBlocks: [{ type: 'text', text }],
  });
}

function sendPrompt(text) {
  promptsSent++;
  // A known session id in a fresh runtime must be resumed before it can
  // be prompted ("already exists"); an unknown id is lazily created by
  // the prompt itself. One honest retry, then the failure is logged —
  // never silently dropped.
  promptOnce(text).catch((err) => {
    if (/already exists/.test(err.message)) {
      call('session/resume', { sessionId: SESSION_ID })
        .then(() => promptOnce(text))
        .catch((err2) => {
          promptsSent--;
          log(`session/prompt failed even after resume: ${err2.message}`);
        });
    } else {
      promptsSent--;
      log(`session/prompt failed: ${err.message}`);
    }
  });
}

function contentAsText(content) {
  if (typeof content === 'string') return content;
  return JSON.stringify(content);
}

createInterface({ input: process.stdin }).on('line', (line) => {
  if (!line.trim()) return;
  let frame;
  try {
    frame = JSON.parse(line);
  } catch {
    log(`undecodable control line dropped: ${line.slice(0, 200)}`);
    return;
  }
  if (frame.v !== 0) {
    log(`control frame version ${frame.v} != 0 dropped`);
    return;
  }
  switch (frame.type) {
    case 'input':
      if (!initialized) {
        queuedPrompts.push(contentAsText(frame.content));
      } else {
        sendPrompt(contentAsText(frame.content));
      }
      break;
    case 'steer':
      // The SDK wire has no agent.inject: a steer can only ride as an
      // ordinary prompt, and the harness-injected marker cannot survive
      // (documented limitation — the synthetic flag is NOT claimed).
      log('steer adapted to an ordinary session/prompt (no inject method in the SDK protocol)');
      if (!initialized) {
        queuedPrompts.push(contentAsText(frame.content));
      } else {
        sendPrompt(contentAsText(frame.content));
      }
      break;
    case 'interrupt':
      // There is NO cancel method in the SDK protocol. Killing the
      // child mid-step settles the turn as an attempt with no
      // assistant/message — interrupted, never completed. At an idle
      // boundary there is nothing to cancel; nothing is faked either way.
      if (stepOpen) {
        log('interrupt: killing dsh mid-step (no cancel method in the SDK protocol)');
        interruptedMidStep = true;
        child.kill('SIGKILL');
      } else {
        log('interrupt at an idle boundary: no turn in flight, nothing cancelled (no cancel method in the SDK protocol)');
      }
      break;
    default:
      log(`unknown control frame type "${frame.type}" dropped`);
      break;
  }
});

process.stdin.on('close', () => {
  // The kian driver went away: never leak a dsh child.
  child.kill('SIGKILL');
  process.exit(0);
});

// --- boot ------------------------------------------------------------------

call('initialize', { cwd: CWD, provider: PROVIDER, model: MODEL })
  .then((result) => {
    initialized = true;
    log(
      `initialized: ${result?.serverInfo?.name ?? '?'} ${result?.serverInfo?.version ?? '?'}`,
    );
    for (const text of queuedPrompts.splice(0)) sendPrompt(text);
  })
  .catch((err) => {
    log(`FATAL: initialize failed: ${err.message}`);
    child.kill('SIGKILL');
    process.exit(1);
  });
