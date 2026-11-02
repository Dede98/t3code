import { DEFAULT_SERVER_SETTINGS, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { proxyOAuthExternalMcpServers, resolveExternalMcpServers } from "./externalMcpServers.ts";

describe("resolveExternalMcpServers", () => {
  const instanceId = ProviderInstanceId.make("codex_work");

  it("includes globally enabled servers and matching allowlists", () => {
    const resolved = resolveExternalMcpServers(
      {
        ...DEFAULT_SERVER_SETTINGS,
        externalMcpServers: {
          global: { url: "https://example.com/mcp", headers: [], enabled: true },
          selected: {
            url: "https://selected.example/mcp",
            headers: [],
            enabled: true,
            providerInstances: [instanceId],
          },
          none: {
            url: "https://none.example/mcp",
            headers: [],
            enabled: true,
            providerInstances: [],
          },
          disabled: { url: "https://disabled.example/mcp", headers: [], enabled: false },
        } as never,
      },
      instanceId,
    );

    expect(resolved.map((server) => server.name)).toEqual(["global", "selected"]);
  });

  it("attaches OAuth servers only while signed in", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      externalMcpServers: {
        linear: {
          url: "https://mcp.linear.app/mcp",
          headers: [],
          enabled: true,
          auth: { type: "oauth" },
        },
        static: { url: "https://static.example/mcp", headers: [], enabled: true },
      } as never,
    };

    expect(resolveExternalMcpServers(settings, instanceId).map((server) => server.name)).toEqual([
      "static",
    ]);
    expect(
      resolveExternalMcpServers(settings, instanceId, new Set(["linear"])).map(
        (server) => server.name,
      ),
    ).toEqual(["linear", "static"]);
  });
});

describe("proxyOAuthExternalMcpServers", () => {
  it("routes OAuth servers through T3 with the session credential and leaves others alone", () => {
    const servers = [
      {
        name: "linear",
        url: "https://mcp.linear.app/mcp",
        headers: [{ name: "X-Team", value: "core", sensitive: false }],
        enabled: true,
        auth: { type: "oauth" as const },
      },
      {
        name: "static",
        url: "https://static.example/mcp",
        headers: [{ name: "Authorization", value: "Bearer static", sensitive: true }],
        enabled: true,
      },
    ];

    const proxied = proxyOAuthExternalMcpServers(servers, {
      endpoint: "http://127.0.0.1:3773/mcp",
      authorizationHeader: "Bearer t3-session",
    });

    expect(proxied[0]).toMatchObject({
      url: "http://127.0.0.1:3773/mcp-external/linear",
      headers: [{ name: "Authorization", value: "Bearer t3-session", sensitive: true }],
    });
    expect(proxied[1]).toBe(servers[1]);
  });
});
