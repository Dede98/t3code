# External MCP servers

T3 Code can attach remote Streamable HTTP MCP servers to provider sessions. Open **Settings →
Integrations → External MCP servers**, add a unique server ID and an HTTPS endpoint, then add any
required headers. Mark credentials such as `Authorization` as secret. Secret values are stored by
the T3 server and are not returned to connected clients or written into `settings.json`.

Changes apply to new sessions. You can disable a server without deleting it, or select the provider
instances that should receive it. An omitted provider selection means every instance; selecting no
instances means none.

Configuration is currently available in the web and desktop settings. Mobile sessions use the
server's saved configuration, but the native mobile settings screen cannot edit it yet.

Claude, Cursor, Grok, and T3-managed OpenCode sessions support arbitrary configured headers. Codex
supports a secret `Authorization: Bearer ...` header; other custom headers are not passed to Codex.
OpenCode servers configured as an external runtime are also unsupported because T3 Code does not
control their MCP configuration.

External servers are independent of T3 Code's built-in browser MCP server. Disabling agent browser
access does not disable external servers, and `t3-code` is reserved as the built-in server name.

Only HTTPS endpoints are accepted, except for `http://localhost`, `http://127.0.0.1`, and
`http://[::1]` during local development. OAuth, SSE, stdio servers, and per-thread credentials are
not supported.

Do not put credentials in the endpoint URL. Username/password URL fields are rejected. Query
parameters are sent as part of the URL and may appear in provider diagnostics or upstream server
logs, so use a secret header for credentials instead.
