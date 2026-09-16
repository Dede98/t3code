import {
  DEFAULT_CLIENT_SETTINGS,
  DEFAULT_UNIFIED_SETTINGS,
  EnvironmentId,
  ExternalMcpServerId,
  type DeviceServiceState,
} from "@t3tools/contracts";
import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { act, StrictMode, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { SettingsScopeSearch } from "./settingsScope";
import { Button } from "../ui/button";
import { Switch } from "../ui/switch";

const mcpState = vi.hoisted(() => ({
  initialSearch: {} as SettingsScopeSearch,
  environments: [] as {
    environmentId: EnvironmentId;
    label: string;
    displayUrl: string;
  }[],
  settings: new Map<EnvironmentId, typeof DEFAULT_UNIFIED_SETTINGS>(),
  writes: vi.fn(),
}));

const { listBrowserImportSources } = vi.hoisted(() => ({
  listBrowserImportSources: vi.fn().mockResolvedValue([]),
}));

vi.mock("../preview/previewBridge", () => ({
  previewBridge: { listBrowserImportSources },
}));
vi.mock("../../env", () => ({ isElectron: true }));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ environments: [], isReady: true }),
  usePrimaryEnvironment: () => null,
}));
vi.mock("../../hooks/useSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useSettings")>()),
  PRIMARY_SETTINGS_UNAVAILABLE_MESSAGE: "Connect to an environment",
  useClientSettings: (selector?: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    selector ? selector(DEFAULT_CLIENT_SETTINGS) : DEFAULT_CLIENT_SETTINGS,
  useClientSettingsHydrated: () => true,
  usePrimarySettingsAvailable: () => true,
  usePrimarySettings: <A,>(selector: (settings: typeof DEFAULT_UNIFIED_SETTINGS) => A) =>
    selector(DEFAULT_UNIFIED_SETTINGS),
  useUpdatePrimarySettings: () => vi.fn(),
  useEnvironmentSettings: (environmentId: EnvironmentId) => mcpState.settings.get(environmentId),
  useUpdateEnvironmentSettings:
    (environmentId: EnvironmentId) => (patch: Partial<typeof DEFAULT_UNIFIED_SETTINGS>) => {
      mcpState.writes(environmentId, patch);
      mcpState.settings.set(environmentId, { ...mcpState.settings.get(environmentId)!, ...patch });
    },
}));
vi.mock("./settingsLayout", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./settingsLayout")>()),
  SettingsPageContainer: ({ children }: { children: ReactNode }) => children,
}));
// Agent-access rows are separate from the browser and external MCP sections covered here.
vi.mock("./ProjectDefaultsSettings", () => ({ ProjectDefaultsSettings: () => null }));
vi.mock("./SettingsScopeContext", async () => {
  const { useState } = await import("react");
  return {
    useSettingsScope: () => {
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
        connectedEnvironments: mcpState.environments.filter(
          (environment) => !search.machine || environment.environmentId === search.machine,
        ),
        targets: [],
      };
    },
    useOptionalSettingsScope: () => null,
  };
});

import { IntegrationsSettingsPanel } from "./IntegrationsSettings";
import { platformSetupStatus } from "../device/DeviceSetup";

let renderer: ReactTestRenderer | undefined;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  listBrowserImportSources.mockClear();
  mcpState.initialSearch = {};
  mcpState.environments = [];
  mcpState.settings.clear();
  mcpState.writes.mockReset();
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  vi.unstubAllGlobals();
});

async function openSettings() {
  const router = createRouter({
    routeTree: createRootRoute({ component: IntegrationsSettingsPanel }),
    history: createMemoryHistory(),
  });
  await router.load();
  await act(() => {
    renderer = create(
      <StrictMode>
        <RouterProvider router={router} />
      </StrictMode>,
    );
  });
  expect(renderer!.root.findByType(IntegrationsSettingsPanel)).toBeDefined();
}

describe("Integrations browser discovery", () => {
  it("does not scan browser files when entering or revisiting settings", async () => {
    await openSettings();
    expect(listBrowserImportSources).not.toHaveBeenCalled();

    await act(() => renderer?.unmount());
    await openSettings();
    expect(listBrowserImportSources).not.toHaveBeenCalled();
  });

  it("places device settings directly after browser settings", async () => {
    await openSettings();
    const sections = renderer!.root
      .findAll((node) => node.type === "section")
      .map((node) => node.props.id)
      .filter(Boolean);
    expect(sections.indexOf("devices")).toBeGreaterThan(sections.indexOf("browser"));
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
    expect(
      renderer!.root
        .findAllByType(Switch)
        .filter((node) => node.props["aria-label"] === "Enable tools"),
    ).toHaveLength(0);
    const choice = renderer!.root
      .findAllByType(Button)
      .find((node) => node.props.children === "remote")!;
    await act(() => choice.props.onClick());

    const url = renderer!.root
      .findAllByType("input")
      .find((node) => node.props["aria-label"] === "tools MCP URL")!;
    expect(url.props.defaultValue).toBe("https://remote.example.com/mcp");
    const toggle = renderer!.root
      .findAllByType(Switch)
      .find((node) => node.props["aria-label"] === "Enable tools")!;
    await act(() => toggle.props.onCheckedChange(false));

    expect(mcpState.writes).toHaveBeenCalledExactlyOnceWith(remoteId, {
      externalMcpServers: {
        tools: { url: "https://remote.example.com/mcp", enabled: false, headers: [] },
      },
    });
    expect(mcpState.settings.get(localId)!.externalMcpServers[serverId]!.enabled).toBe(true);
  });

  it("opens the editor directly with a single connected environment", async () => {
    mcpState.environments = mcpState.environments.slice(1);
    await openSettings();
    expect(
      renderer!.root
        .findAllByType("input")
        .find((node) => node.props["aria-label"] === "tools MCP URL")?.props.defaultValue,
    ).toBe("https://remote.example.com/mcp");
  });

  it("does not fall back to another environment when the selected one disconnects", async () => {
    mcpState.initialSearch = { machine: remoteId };
    mcpState.environments = mcpState.environments.slice(0, 1);
    await openSettings();
    expect(
      renderer!.root
        .findAllByType("input")
        .filter((node) => node.props["aria-label"] === "tools MCP URL"),
    ).toHaveLength(0);
    expect(mcpState.writes).not.toHaveBeenCalled();
  });
});

const deviceState = (overrides: Partial<DeviceServiceState> = {}): DeviceServiceState => ({
  hosts: [
    {
      id: "local",
      kind: "local",
      label: "This machine",
      hubInstalled: false,
      agentDeviceInstalled: false,
      platforms: [
        { platform: "ios", available: true },
        { platform: "android", available: true },
      ],
    },
  ],
  hostStatus: "ready",
  hostStatuses: {},
  devices: [],
  sessions: [],
  onboardingCompleted: false,
  agentAccessEnabled: false,
  hubBasePath: "/api/device-hub",
  revision: 0,
  ...overrides,
});

describe("device setup guidance", () => {
  it("directs users to install an iOS runtime and create an Android virtual device", () => {
    expect(platformSetupStatus(deviceState(), "ios").message).toContain("Xcode Settings");
    expect(platformSetupStatus(deviceState(), "android").message).toContain("Device Manager");
  });

  it("preserves a specific missing-tool explanation from the server", () => {
    const state = deviceState({
      hosts: [
        {
          ...deviceState().hosts[0]!,
          platforms: [
            { platform: "ios", available: true },
            { platform: "android", available: false, reason: "Android Emulator is missing." },
          ],
        },
      ],
    });
    expect(platformSetupStatus(state, "android").message).toBe("Android Emulator is missing.");
  });
});
