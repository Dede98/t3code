import { DEFAULT_SERVER_SETTINGS, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { resolveExternalMcpServers } from "./ExternalMcpServers.ts";

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
});
