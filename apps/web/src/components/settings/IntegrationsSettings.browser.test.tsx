import {
  EnvironmentId,
  ExternalMcpServerId,
  DEFAULT_CLIENT_SETTINGS,
  DEFAULT_UNIFIED_SETTINGS,
} from "@t3tools/contracts";
import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { act, StrictMode, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import "../../index.css";
import type { SettingsScopeSearch } from "./settingsScope";
const mcpState = vi.hoisted(() => ({
  initialSearch: {} as SettingsScopeSearch,
  environments: [] as { environmentId: EnvironmentId; label: string; displayUrl: string }[],
  settings: new Map<EnvironmentId, typeof DEFAULT_UNIFIED_SETTINGS>(),
  writes: vi.fn(),
}));

const { listBrowserImportSources } = vi.hoisted(() => ({
  listBrowserImportSources: vi.fn().mockResolvedValue([]),
}));

vi.mock(import("../preview/previewBridge"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    previewBridge: { ...actual.previewBridge, listBrowserImportSources } as never,
  };
});
vi.mock(import("../../env"), async (original) => ({ ...(await original()), isElectron: true }));
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

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  listBrowserImportSources.mockClear();
  mcpState.initialSearch = {};
  mcpState.environments = [];
  mcpState.settings.clear();
  mcpState.writes.mockReset();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function openSettings() {
  const router = createRouter({
    routeTree: createRootRoute({ component: IntegrationsSettingsPanel }),
    history: createMemoryHistory(),
  });
  await router.load();
  await act(async () =>
    root.render(
      <StrictMode>
        <RouterProvider router={router} />
      </StrictMode>,
    ),
  );
  await vi.waitFor(() => expect(container.querySelector("section#browser")).not.toBeNull());
}

describe("Integrations browser discovery", () => {
  it("keeps agent browser access in the single Browser section", async () => {
    await openSettings();

    expect(page.getByRole("heading", { name: "Browser", exact: true }).elements()).toHaveLength(1);
    const access = page.getByRole("switch", { name: "Agent browser access", exact: true });
    await expect.element(access).toBeVisible();
    expect(access.element().closest("section")).toBe(container.querySelector("section#browser"));
    expect(access.element().closest("#agent-browser-access")).not.toBeNull();
  });

  it("does not scan browser files when entering or revisiting settings", async () => {
    await openSettings();
    expect(listBrowserImportSources).not.toHaveBeenCalled();

    await act(async () => root.unmount());
    root = createRoot(container);
    await openSettings();
    expect(listBrowserImportSources).not.toHaveBeenCalled();
  });
});

describe("External MCP environment selection", () => {
  const localId = EnvironmentId.make("local");
  const remoteId = EnvironmentId.make("remote");
  const serverId = ExternalMcpServerId.make("tools");
  beforeEach(() => {
    mcpState.environments = [localId, remoteId].map((environmentId) => ({
      environmentId,
      label: environmentId,
      displayUrl: `https://${environmentId}.example.com`,
    }));
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
  });
  it.each([
    ["all environments", {}],
    ["project", { project: "project" }],
    ["checkout", { project: "project", machine: remoteId, checkout: "checkout" }],
  ] as const)("opens and edits the chosen environment from %s", async (_label, search) => {
    mcpState.initialSearch = search;
    await openSettings();
    await expect
      .element(page.getByRole("switch", { name: "Enable tools" }))
      .not.toBeInTheDocument();
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
    mcpState.initialSearch = { machine: remoteId };
    await openSettings();
    await expect
      .element(page.getByRole("textbox", { name: "tools MCP URL" }))
      .toHaveValue("https://remote.example.com/mcp");
  });
});
