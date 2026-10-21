import {
  EnvironmentId,
  ExternalMcpServerId,
  DEFAULT_CLIENT_SETTINGS,
  DEFAULT_UNIFIED_SETTINGS,
} from "@t3tools/contracts";
import { StrictMode, useState } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import "../../index.css";
import { render } from "../../../test/render";
import type { SettingsScopeSearch } from "./settingsScope";

const mcpState = vi.hoisted(() => ({
  initialSearch: {} as SettingsScopeSearch,
  environments: [] as { environmentId: EnvironmentId; label: string; displayUrl: string }[],
  settings: new Map<EnvironmentId, typeof DEFAULT_UNIFIED_SETTINGS>(),
  writes: vi.fn(),
}));

vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: (() => ({ environments: [], isReady: true })) as never,
  usePrimaryEnvironment: () => null,
  usePrimaryEnvironmentId: () => null,
}));
vi.mock(import("../../hooks/useSettings"), async (original) => ({
  ...(await original()),
  useClientSettings: ((selector?: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    selector ? selector(DEFAULT_CLIENT_SETTINGS) : DEFAULT_CLIENT_SETTINGS) as never,
  useClientSettingsHydrated: () => true,
  usePrimarySettingsAvailable: () => true,
  usePrimarySettings: ((selector: (settings: typeof DEFAULT_UNIFIED_SETTINGS) => unknown) =>
    selector(DEFAULT_UNIFIED_SETTINGS)) as never,
  useEnvironmentSettings: ((id: EnvironmentId) => mcpState.settings.get(id)) as never,
  useUpdateEnvironmentSettings: ((id: EnvironmentId) =>
    (patch: Partial<typeof DEFAULT_UNIFIED_SETTINGS>) => {
      mcpState.writes(id, patch);
      mcpState.settings.set(id, { ...mcpState.settings.get(id)!, ...patch });
    }) as never,
  useUpdatePrimarySettings: (() => vi.fn()) as never,
}));
vi.mock(import("./SettingsScopeContext"), async (original) => ({
  ...(await original()),
  useSettingsScope: (() => {
    const [search, selectScope] = useState(mcpState.initialSearch);
    return {
      scope: {
        kind: search.checkout
          ? "checkout"
          : search.project
            ? "project"
            : search.machine
              ? "environment"
              : "all",
        environmentIds: [],
      },
      search,
      selectScope,
      environment: null,
      environments: [],
      target: null,
      targets: [],
      connectedEnvironments: mcpState.environments.filter(
        (e) => !search.machine || e.environmentId === search.machine,
      ),
    };
  }) as never,
  useOptionalSettingsScope: () => null,
}));

import { IntegrationsSettingsPanel } from "./IntegrationsSettings";

const localId = EnvironmentId.make("local");
const remoteId = EnvironmentId.make("remote");
const serverId = ExternalMcpServerId.make("tools");

// The mocked settings and scope hooks read `mcpState`, so every test starts by resetting it.
function setup(initialSearch: SettingsScopeSearch) {
  mcpState.initialSearch = initialSearch;
  mcpState.writes.mockReset();
  mcpState.environments = [localId, remoteId].map((environmentId) => ({
    environmentId,
    label: environmentId,
    displayUrl: `https://${environmentId}.example.com`,
  }));
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

async function openSettings() {
  await using stack = new AsyncDisposableStack();
  stack.use(
    await render(
      <StrictMode>
        <IntegrationsSettingsPanel />
      </StrictMode>,
    ),
  );
  await expect
    .element(page.getByRole("heading", { name: "External MCP servers", exact: true }))
    .toBeVisible();
  return stack.move();
}

it.each([
  ["all environments", {}],
  ["project", { project: "project" }],
  ["checkout", { project: "project", machine: remoteId, checkout: "checkout" }],
] as const)("opens and edits the chosen environment from %s", async (_label, search) => {
  setup(search);
  await using _screen = await openSettings();
  await expect.element(page.getByRole("switch", { name: "Enable tools" })).not.toBeInTheDocument();
  await page.getByRole("button", { name: "remote", exact: true }).click();
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

it("shows the selected environment directly", async () => {
  setup({ machine: remoteId });
  await using _screen = await openSettings();
  await expect
    .element(page.getByRole("textbox", { name: "tools MCP URL" }))
    .toHaveValue("https://remote.example.com/mcp");
});
