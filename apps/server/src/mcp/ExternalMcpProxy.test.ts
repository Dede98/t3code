import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderInstanceId,
  type ExternalMcpServerConfig,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpClient, HttpClientResponse, HttpRouter } from "effect/http";

import { ServerSettingsService } from "../serverSettings.ts";
import * as ExternalMcpOAuth from "./ExternalMcpOAuth.ts";
import * as ExternalMcpProxy from "./ExternalMcpProxy.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import type * as McpInvocationContext from "./McpInvocationContext.ts";

const disposers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const dispose of disposers.splice(0)) await dispose();
});

const UPSTREAM = "https://mcp.example.com/mcp";
const PROXY = "http://t3.test/mcp-external/linear";
const providerInstanceId = ProviderInstanceId.make("codex_work");

const fixture = (input: {
  readonly upstreamStatuses?: ReadonlyArray<number>;
  readonly tokens?: ReadonlyArray<string | undefined>;
  readonly server?: Partial<ExternalMcpServerConfig>;
}) => {
  const upstream: Array<{ headers: Readonly<Record<string, string>>; body: string }> = [];
  const accessTokenCalls: Array<string | undefined> = [];
  const invalidated: Array<string> = [];
  const statuses = [...(input.upstreamStatuses ?? [200])];
  const tokens = [...(input.tokens ?? ["up-1"])];
  const server: ExternalMcpServerConfig = {
    url: UPSTREAM,
    headers: [{ name: "x-workspace", value: "acme", sensitive: false }],
    enabled: true,
    auth: { type: "oauth" },
    ...input.server,
  };
  const client = HttpClient.make((request) =>
    Effect.sync(() => {
      upstream.push({
        headers: request.headers,
        body: request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "",
      });
      return HttpClientResponse.fromWeb(
        request,
        new Response("upstream-body", {
          status: statuses.shift() ?? 200,
          headers: {
            "content-type": "text/event-stream",
            "mcp-session-id": "sess-1",
            "www-authenticate": 'Bearer resource_metadata="https://mcp.example.com/prm"',
            "set-cookie": "upstream=1",
          },
        }),
      );
    }),
  );
  const { handler, dispose } = HttpRouter.toWebHandler(
    ExternalMcpProxy.layer.pipe(
      Layer.provideMerge(
        Layer.mock(McpSessionRegistry.McpSessionRegistry)({
          resolve: (token) =>
            Effect.succeed(
              token === "t3-session"
                ? ({
                    thread: { providerInstanceId },
                  } as unknown as McpInvocationContext.McpThreadInvocationScope)
                : undefined,
            ),
        }),
      ),
      Layer.provideMerge(
        Layer.mock(ExternalMcpOAuth.ExternalMcpOAuth)({
          accessToken: (_serverId, rejected) =>
            Effect.sync(() => {
              accessTokenCalls.push(rejected);
              return Option.fromNullishOr(tokens.shift());
            }),
          invalidate: (_serverId, token) => Effect.sync(() => void invalidated.push(token)),
        }),
      ),
      Layer.provideMerge(
        Layer.mock(ServerSettingsService)({
          getSettings: Effect.succeed({
            ...DEFAULT_SERVER_SETTINGS,
            externalMcpServers: { linear: server } as ServerSettings["externalMcpServers"],
          }),
        }),
      ),
      Layer.provideMerge(Layer.succeed(HttpClient.HttpClient, client)),
    ),
    { disableLogger: true },
  );
  disposers.push(dispose);
  return { handler, upstream, accessTokenCalls, invalidated };
};

const post = (authorization = "Bearer t3-session") =>
  new Request(PROXY, {
    method: "POST",
    headers: {
      authorization,
      accept: "application/json, text/event-stream",
      "content-type": "application/json",
      "x-t3-internal": "keep-me-local",
    },
    body: '{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
  });

describe("external MCP proxy", () => {
  it("refuses callers without a live provider-session credential", async () => {
    const { handler, upstream } = fixture({});
    for (const authorization of ["", "Bearer someone-else", "Basic t3-session"]) {
      const response = await handler(post(authorization));
      expect(response.status).toBe(403);
      await response.text();
    }
    expect(upstream).toEqual([]);
  });

  it("hides servers the calling provider instance may not use", async () => {
    for (const server of [
      { providerInstances: [ProviderInstanceId.make("claude_personal")] },
      { enabled: false },
      { auth: { type: "none" as const } },
    ]) {
      const { handler, upstream } = fixture({ server });
      const response = await handler(post());
      expect(response.status).toBe(404);
      await response.text();
      expect(upstream).toEqual([]);
    }
  });

  it("swaps T3's credential for the upstream bearer and forwards only MCP headers", async () => {
    const { handler, upstream } = fixture({ server: { providerInstances: [providerInstanceId] } });
    const response = await handler(post());
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("upstream-body");
    expect(response.headers.get("mcp-session-id")).toBe("sess-1");
    expect(response.headers.get("www-authenticate")).toBeNull();
    expect(response.headers.get("set-cookie")).toBeNull();
    expect(upstream).toHaveLength(1);
    expect(upstream[0]?.headers).toMatchObject({
      authorization: "Bearer up-1",
      accept: "application/json, text/event-stream",
      "x-workspace": "acme",
    });
    expect(upstream[0]?.headers["x-t3-internal"]).toBeUndefined();
    expect(upstream[0]?.body).toBe('{"jsonrpc":"2.0","id":1,"method":"tools/list"}');
  });

  it("replays a refused request once with a refreshed token", async () => {
    const { handler, upstream, accessTokenCalls, invalidated } = fixture({
      upstreamStatuses: [401, 200],
      tokens: ["up-1", "up-2"],
    });
    const response = await handler(post());
    expect(response.status).toBe(200);
    await response.text();
    expect(accessTokenCalls).toEqual([undefined, "up-1"]);
    expect(upstream.map((request) => request.headers.authorization)).toEqual([
      "Bearer up-1",
      "Bearer up-2",
    ]);
    expect(upstream[1]?.body).toBe(upstream[0]?.body);
    expect(invalidated).toEqual([]);
  });

  it("answers a refused refreshed token with 503, never a 401 challenge", async () => {
    const twice = fixture({ upstreamStatuses: [401, 401], tokens: ["up-1", "up-2"] });
    const response = await twice.handler(post());
    expect(response.status).toBe(503);
    expect(response.headers.get("www-authenticate")).toBeNull();
    expect(await response.json()).toMatchObject({ error: "needs_sign_in" });
    expect(twice.invalidated).toEqual(["up-2"]);

    const signedOut = fixture({ tokens: [] });
    const missing = await signedOut.handler(post());
    expect(missing.status).toBe(503);
    await missing.text();
    expect(signedOut.upstream).toEqual([]);
  });
});
