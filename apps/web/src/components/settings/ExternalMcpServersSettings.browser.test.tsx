import { EnvironmentId, ExternalMcpServerId, DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts";
import { StrictMode } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import "../../index.css";
import { render } from "../../../test/render";

const mcpState = vi.hoisted(() => ({
  settings: new Map<EnvironmentId, typeof DEFAULT_UNIFIED_SETTINGS>(),
  writes: vi.fn(),
}));

vi.mock(import("../../hooks/useSettings"), async (original) => ({
  ...(await original()),
  useEnvironmentSettings: ((id: EnvironmentId) => mcpState.settings.get(id)) as never,
  useUpdateEnvironmentSettings: ((id: EnvironmentId) =>
    (patch: Partial<typeof DEFAULT_UNIFIED_SETTINGS>) => {
      mcpState.writes(id, patch);
      mcpState.settings.set(id, { ...mcpState.settings.get(id)!, ...patch });
    }) as never,
}));

import { ExternalMcpServersSettings } from "./ExternalMcpServersSettings";

const localId = EnvironmentId.make("local");
const remoteId = EnvironmentId.make("remote");
const serverId = ExternalMcpServerId.make("tools");

// The mocked settings hooks read `mcpState`, so every test starts by resetting it.
function setup() {
  mcpState.writes.mockReset();
  mcpState.settings.clear();
  for (const environmentId of [localId, remoteId]) {
    mcpState.settings.set(environmentId, {
      ...DEFAULT_UNIFIED_SETTINGS,
      externalMcpServers: {
        [serverId]: {
          url: `https://${environmentId}.example.com/mcp`,
          enabled: true,
          headers: [],
        },
      },
    });
  }
}

it("edits only the environment the providers page selected", async () => {
  setup();
  await using _screen = await render(
    <StrictMode>
      <ExternalMcpServersSettings environmentId={remoteId} />
    </StrictMode>,
  );
  await expect
    .element(page.getByRole("textbox", { name: "tools MCP URL" }))
    .toHaveValue("https://remote.example.com/mcp");
  await page.getByRole("switch", { name: "Enable tools" }).click();
  expect(mcpState.writes).toHaveBeenCalledExactlyOnceWith(remoteId, {
    externalMcpServers: {
      [serverId]: { url: "https://remote.example.com/mcp", enabled: false, headers: [] },
    },
  });
  expect(mcpState.settings.get(localId)?.externalMcpServers[serverId]?.enabled).toBe(true);
});
