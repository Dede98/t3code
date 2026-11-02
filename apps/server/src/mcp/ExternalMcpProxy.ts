import { EXTERNAL_MCP_PROXY_PATH_PREFIX } from "@t3tools/provider-core/server/externalMcpServers";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { ServerSettingsService } from "../serverSettings.ts";
import * as ExternalMcpOAuth from "./ExternalMcpOAuth.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";

/** Streamable HTTP headers the upstream needs; everything else, T3's credential first, stays here. */
const FORWARDED_REQUEST_HEADERS = [
  "accept",
  "content-type",
  "mcp-session-id",
  "mcp-protocol-version",
  "last-event-id",
] as const;

const DROPPED_RESPONSE_HEADERS = new Set([
  "connection",
  "content-encoding",
  "content-length",
  "keep-alive",
  "set-cookie",
  "transfer-encoding",
  "www-authenticate",
]);

/**
 * Never a 401: provider CLIs answer one by running OAuth discovery against T3's origin, whose
 * metadata describes T3's own authorization server, not the upstream's.
 */
const needsSignIn = (name: string) =>
  HttpServerResponse.jsonUnsafe(
    {
      error: "needs_sign_in",
      message: `${name} has no usable sign-in right now. Check T3 Code Settings → MCP servers.`,
    },
    { status: 503, headers: { "cache-control": "no-store" } },
  );

const rejected = (status: number, error: string, message: string) =>
  HttpServerResponse.jsonUnsafe(
    { error, message },
    { status, headers: { "cache-control": "no-store" } },
  );

const bearer = (header: string | undefined) => {
  const match = /^Bearer\s+(\S+)$/iu.exec(header ?? "");
  return match?.[1] ?? "";
};

const handler = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const registry = yield* McpSessionRegistry.McpSessionRegistry;
  const oauth = yield* ExternalMcpOAuth.ExternalMcpOAuth;
  const serverSettings = yield* ServerSettingsService;
  const httpClient = HttpClient.withScope(yield* HttpClient.HttpClient);

  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) return HttpServerResponse.text("Bad Request", { status: 400 });
  const rawId = url.value.pathname.slice(EXTERNAL_MCP_PROXY_PATH_PREFIX.length);
  if (rawId.length === 0 || rawId.includes("/")) {
    return HttpServerResponse.text("Not Found", { status: 404 });
  }
  const serverId = (() => {
    try {
      return decodeURIComponent(rawId);
    } catch {
      return undefined;
    }
  })();
  if (serverId === undefined) return HttpServerResponse.text("Not Found", { status: 404 });

  // Only provider sessions hold these tokens; MCP OAuth clients of T3 itself do not qualify.
  const invocation = yield* registry.resolve(bearer(request.headers.authorization));
  if (invocation === undefined) {
    return rejected(
      403,
      "invalid_mcp_credential",
      "A live T3 Code session credential is required.",
    );
  }
  const settings = yield* serverSettings.getSettings;
  const server = Object.hasOwn(settings.externalMcpServers, serverId)
    ? settings.externalMcpServers[serverId as keyof typeof settings.externalMcpServers]
    : undefined;
  if (
    server === undefined ||
    !server.enabled ||
    server.auth?.type !== "oauth" ||
    (server.providerInstances !== undefined &&
      !server.providerInstances.includes(invocation.thread.providerInstanceId))
  ) {
    return HttpServerResponse.text("Not Found", { status: 404 });
  }
  const name = server.displayName ?? serverId;

  // Buffered so a request the upstream refuses can be replayed once with a refreshed token.
  const body =
    request.method === "GET" || request.method === "HEAD" || request.method === "DELETE"
      ? undefined
      : yield* request.arrayBuffer;
  const headers: Record<string, string> = {};
  for (const header of server.headers) headers[header.name] = header.value;
  for (const name of FORWARDED_REQUEST_HEADERS) {
    const value = request.headers[name];
    if (value !== undefined) headers[name] = value;
  }

  const send = (token: string) =>
    httpClient.execute(
      HttpClientRequest.make(request.method)(server.url).pipe(
        HttpClientRequest.setHeaders({ ...headers, authorization: `Bearer ${token}` }),
        body === undefined
          ? (self) => self
          : HttpClientRequest.bodyUint8Array(new Uint8Array(body), headers["content-type"]),
      ),
    );

  const first = yield* oauth.accessToken(serverId);
  if (Option.isNone(first)) return needsSignIn(name);
  let response = yield* send(first.value);
  if (response.status === 401) {
    // An unread body would hold the upstream connection for the rest of the request.
    yield* Effect.ignore(response.arrayBuffer);
    const retry = yield* oauth.accessToken(serverId, first.value);
    if (Option.isNone(retry)) return needsSignIn(name);
    response = yield* send(retry.value);
    if (response.status === 401) {
      // A token minted this moment and still refused means the grant is no longer good. The same
      // token back means the refresh failed for now, which does not end the sign-in.
      if (retry.value !== first.value) yield* oauth.invalidate(serverId, retry.value);
      return needsSignIn(name);
    }
  }

  const responseHeaders: Record<string, string> = {};
  for (const [header, value] of Object.entries(response.headers)) {
    if (DROPPED_RESPONSE_HEADERS.has(header) || value === undefined) continue;
    responseHeaders[header] = value;
  }
  responseHeaders["cache-control"] = "no-store, no-transform";
  return HttpServerResponse.stream(response.stream, {
    status: response.status,
    headers: responseHeaders,
    ...(responseHeaders["content-type"] ? { contentType: responseHeaders["content-type"] } : {}),
  });
}).pipe(
  // The error keeps the request it failed on, Authorization header included.
  Effect.catch((cause) =>
    Effect.logWarning("External MCP proxy request failed", { reason: cause._tag }).pipe(
      Effect.as(rejected(502, "upstream_unreachable", "The MCP server could not be reached.")),
    ),
  ),
);

export const layer = HttpRouter.add("*", `${EXTERNAL_MCP_PROXY_PATH_PREFIX}*`, handler);
