# ADR-057: Client-side web tools are an audited bundle provider, not a server-side model capability

- **Status**: accepted (owner direction 2026-11-22; P2b slice).
- **Context**: DeepSeek ships two ways to give an agent web access. The *server-side* form (the
  `deepseek_web_search` / `deepseek_read_url` tools and their Anthropic/Responses equivalents) is
  executed by the provider: the client sends no request, the answer text comes back with citations,
  and the pages themselves travel back encrypted in an opaque `signature` that only the provider can
  replay. The *client-side* form is two plain HTTP endpoints — `POST /v1/tools/search` and
  `POST /v1/tools/fetch` — that any caller can invoke and see. The two are not interchangeable for a
  durable runtime. Server-side tools hide the effect: there is no `tool/call`, no request bytes, no
  result bytes, no price line, and the signature is an un-auditable payload that kian would have to
  store and replay byte-for-byte while never being able to inspect it. They also bind the loop to a
  provider dialect. Client-side tools are ordinary HTTP calls with documented JSON, a per-request
  price, and results kian already knows how to persist, spill (ADR-056), replay and budget (P0-P1).
  The constraint that decides this is the narrow waist (AGENTS.md #10): a web capability must not
  put provider names, provider signatures or a second effect channel into the core.
- **Decision**:
  1. **Web access is one bundle crate, `kian-web`, exposing one `ToolProvider`.** It advertises
     exactly two tools, `web_search` and `web_fetch`, and implements them over the documented
     DeepSeek Search/Fetch HTTP APIs. No core crate learns the tool names, the provider name or the
     response shape; assembly (`kian-cli`) mounts the provider next to the local tools. Core grows
     no seam: `ToolProvider`, `ToolCall`, `ToolResult` and `ToolError` are unchanged.
  2. **The provider is protocol-only and configured at assembly.** `web:` carries `base_url`
     (default `https://api.deepseek.com`), `api_key_env` (default `DEEPSEEK_API_KEY`),
     `timeout_ms`, and the `search` defaults (`max_results`, `need_content`). The API key is read
     from the environment at serve assembly; a missing key is a fail-closed assembly error, not a
     tool-time surprise. The provider holds only an HTTP client, the base URL, the key and those
     defaults, so anything that speaks the same documented contract can be mounted without touching
     the loop.
  3. **Every call is an ordinary audited tool call.** A search or fetch is a `tool/call` followed by
     a `tool/result`; the request parameters and the result JSON are persisted by the same
     persist-what-you-send path as every other tool, count against the session tool budget, replay
     identically after a crash, and spill by reference when the body is oversized (ADR-056). The
     output shape is the provider's results verbatim under `results` (search) or `title`/`content`
     (fetch), wrapped with the request echo (`query`/`count`/`url`) so a replay can tell what was
     asked even if the provider-side ranking data changes meaning later.
  4. **Failures are honest results, and there are no hidden retries.** A non-2xx response or a
     transport error yields `is_error: true` with the status, a bounded body excerpt and a stable
     error string; it never becomes an empty success and never silently degrades into a different
     request. Malformed arguments are `ToolError::InvalidCall`. HTTP 408/429 and 5xx are documented
     retryable, but retrying is the caller's explicit decision: each attempt is a billable request
     and must be visible as its own tool call, so the provider performs no automatic retry.
  5. **Mounting is deterministic and byte-stable.** When `web:` is configured, the two schemas are
     appended to the agent's effective tool list in a fixed order after the local tools, and their
     names are added to the mounted-name list so the fail-closed schema filter keeps them. When the
     section is absent, no schema, name or byte changes: a legacy session is byte-identical. The
     schema text is treated as prompt-prefix content (constraint 3): it is fixed once the session
     starts, never re-rendered per turn.
  6. **Results are untrusted data.** The tool descriptions state that web output is external,
     untrusted content to treat as data, never as instructions; the loop does not fence it further
     in this slice. Client-side tools return the raw result fields rather than provider-encrypted
     page signatures, so an operator can audit what the model saw; citations, if a caller wants
     them, are the model's own rendering of `url`/`title`/`snippet` fields, not a stored provider
     artifact.
  7. **Billing is per request, and the phase-0 cost projection says so.** Search and Fetch are
     priced per successful request, not per token. The token-based `kian cost` projection (ADR-053)
     does not yet model request-count fees: web tool calls are visible in the event stream and
     counted as tool calls, but their fee is not added to `cost_micros`, and the limitation is
     recorded in SPEC and the ADR. A request-count price shape is a later economics slice.
- **Alternatives considered**:
  - *Server-side `deepseek_web_search` / `deepseek_read_url` in the core*: rejected — hidden effect,
     opaque `signature` replay, no persisted request bytes, a provider dialect inside the narrow
     waist, and a second effect channel that bypasses the tool budget and the audit trail.
  - *A generic mapping bridge in `kian-cli`*: rejected — that is where the mount happens, but the
     HTTP contract and the schemas belong in a standalone crate so the bundle stays testable without
     the CLI and the core never sees the response shape.
  - *Adding `web_search`/`web_fetch` to `kian-loop`'s built-in tools*: rejected — built-in tools
     are provider-agnostic (`exec`, `fs_*`) and the loop must stay replaceable by a harness bridge; a
     network capability with an API key is configuration, not a loop primitive.
  - *Automatic retry/backoff inside the provider*: rejected — retries spend money and must be
     visible in the log; the model or the caller decides, and each attempt is its own event.
  - *Returning provider-encrypted page signatures for later continuation*: rejected — kian cannot
     audit them, cannot page them, and would have to store an un-replayable-by-kian blob in the
     stream; the client-side Fetch tool returns Markdown instead.
- **Consequences**: web access becomes a normal, auditable capability: two tools in the catalog,
  requests and results in the stream, oversized pages spilled to the blob store, failures honest and
  counted. The loop stays harness-neutral (a bridge that does not mount `web` simply has no web
  tools), and swapping the endpoint (a compatible proxy, a self-hosted search service) is a config
  change. The cost is that OpenAPI compatibility is the price of admission: only endpoints with the
  documented JSON contract can mount, and structured fields the provider adds later are passed
  through rather than normalized.
- **Limits**: no server-side tools, no `signature` replay, no streaming results; no automatic
  retry; no per-request cost in `cost_micros` yet; no image search; no citation objects (the result
  fields are enough for a model to cite); no HTTP fetching outside `web_fetch` (no arbitrary
  header/proxy control); the tool description injection warning is framing, not a sandbox.
- **Acceptance**: wiremock unit tests for both tools (request body/allowlist, results passthrough,
  query/count echo, URL validation, 403/404 honest errors, 429 retryable classification, transport
  error), config tests (absent `web:` leaves agent tools byte-identical; missing API key fails
  assembly closed), and a mounted-tool test (schemas appended in stable order and present in the
  mounted-name list). `docs_map` stays green.
