/**
 * Integrations settings - preferences for surfaces T3 Code embeds rather than
 * owns. Browser is the first section: the defaults a preview tab opens at,
 * applied to both hand-opened tabs and agent `preview_open` calls that don't
 * state their own size.
 *
 * @module IntegrationsSettings
 */
import {
  BROWSER_RECORDING_FRAME_RATES,
  DEFAULT_BROWSER_AUTO_SHOW_FLOATING_PREVIEW,
  DEFAULT_BROWSER_RECORDING_FRAME_RATE,
  DEFAULT_BROWSER_VIEWPORT,
  DEFAULT_PREVIEW_APPEARANCE,
  DEFAULT_UNIFIED_SETTINGS,
  DEFAULT_PREVIEW_ZOOM_FACTOR,
  FILL_PREVIEW_VIEWPORT,
  type ExternalMcpHeader,
  PREVIEW_VIEWPORT_MAX_AREA,
  PREVIEW_VIEWPORT_MAX_DIMENSION,
  PREVIEW_VIEWPORT_MIN_DIMENSION,
  PREVIEW_ZOOM_LEVELS,
  type PreviewAppearancePreference,
  type PreviewViewportSetting,
} from "@t3tools/contracts";
import { PREVIEW_VIEWPORT_PRESETS } from "@t3tools/shared/previewViewport";
import { InfoIcon, PlusIcon, Trash2Icon } from "lucide-react";
import { useState, type ReactNode } from "react";

import { ScreenRotationIcon } from "~/browser/ScreenRotationIcon";
import { ensureLocalApi } from "~/localApi";
import { isElectron } from "../../env";

import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import { Input } from "../ui/input";
import { NumberField, NumberFieldGroup, NumberFieldInput } from "../ui/number-field";
import {
  Select,
  SelectGroup,
  SelectGroupLabel,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../ui/select";
import { Switch } from "../ui/switch";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import {
  useClientSettings,
  usePrimarySettings,
  useUpdatePrimarySettings,
} from "~/hooks/useSettings";

import {
  SettingResetButton,
  SettingsPageContainer,
  SettingsRow,
  SettingsSection,
} from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

const FILL_VALUE = "fill";
const RESPONSIVE_VALUE = "responsive";

/**
 * The size a "Responsive" default falls back to when the user switches away
 * from Fill and hasn't typed dimensions yet. Fill has no dimensions to carry
 * over, so the picker needs something concrete to seed the inputs with.
 */
const RESPONSIVE_SEED_SIZE = { width: 1280, height: 800 } as const;

const NO_GROUPING: Intl.NumberFormatOptions = { useGrouping: false };

const APPEARANCE_LABELS: Readonly<Record<PreviewAppearancePreference, string>> = {
  system: "System",
  light: "Light",
  dark: "Dark",
};

const zoomLabel = (zoomFactor: number) => `${Math.round(zoomFactor * 100)}%`;

const viewportSelectValue = (viewport: PreviewViewportSetting): string => {
  if (viewport._tag === "fill") return FILL_VALUE;
  if (
    viewport._tag === "preset" &&
    PREVIEW_VIEWPORT_PRESETS.some((preset) => preset.id === viewport.presetId)
  ) {
    return viewport.presetId;
  }
  return RESPONSIVE_VALUE;
};

/**
 * The trigger renders this rather than a bare `SelectValue`, which would fall
 * back to printing the raw stored value ("fill") because the options are built
 * inline instead of from an `items` map.
 */
const viewportSelectLabel = (viewport: PreviewViewportSetting): string => {
  const value = viewportSelectValue(viewport);
  if (value === FILL_VALUE) return "Fill panel";
  if (value === RESPONSIVE_VALUE) return "Responsive";
  return PREVIEW_VIEWPORT_PRESETS.find((preset) => preset.id === value)?.label ?? "Responsive";
};

const isValidDimension = (value: number) =>
  Number.isInteger(value) &&
  value >= PREVIEW_VIEWPORT_MIN_DIMENSION &&
  value <= PREVIEW_VIEWPORT_MAX_DIMENSION;

/**
 * A sized viewport with width and height swapped. Presets keep their identity
 * through a rotation — `resolvePreviewViewport` already stores rotated presets
 * as the preset id plus swapped dimensions — so a rotated iPad is still an
 * iPad, not an anonymous custom size.
 */
const rotateViewport = (
  viewport: Exclude<PreviewViewportSetting, { readonly _tag: "fill" }>,
): PreviewViewportSetting => ({
  ...viewport,
  width: viewport.height,
  height: viewport.width,
});

function BrowserViewportSetting({ disabled }: { readonly disabled: boolean }) {
  const viewport = useClientSettings((settings) => settings.browserDefaultViewport);
  const updateSettings = useUpdatePrimarySettings();

  const sized = viewport._tag === "fill" ? null : viewport;
  const presentedSize = {
    width: sized?.width ?? RESPONSIVE_SEED_SIZE.width,
    height: sized?.height ?? RESPONSIVE_SEED_SIZE.height,
  };

  const selectViewport = (value: string | null) => {
    if (value === FILL_VALUE) {
      updateSettings({ browserDefaultViewport: FILL_PREVIEW_VIEWPORT });
      return;
    }
    if (value === RESPONSIVE_VALUE) {
      updateSettings({
        browserDefaultViewport: {
          _tag: "freeform",
          width: sized?.width ?? RESPONSIVE_SEED_SIZE.width,
          height: sized?.height ?? RESPONSIVE_SEED_SIZE.height,
        },
      });
      return;
    }
    const preset = PREVIEW_VIEWPORT_PRESETS.find((candidate) => candidate.id === value);
    if (!preset) return;
    updateSettings({
      browserDefaultViewport: {
        _tag: "preset",
        width: preset.width,
        height: preset.height,
        presetId: preset.id,
      },
    });
  };

  // Committed on blur rather than per keystroke: typing "2560" passes through
  // "256", which is a legal dimension, so an onValueChange handler would
  // persist that intermediate size and churn the settings file on every key.
  const commitDimension = (axis: "width" | "height", value: number | null) => {
    if (value === null || !isValidDimension(value)) return;
    const next = { ...presentedSize, [axis]: value };
    if (next.width * next.height > PREVIEW_VIEWPORT_MAX_AREA) return;
    if (sized && next.width === sized.width && next.height === sized.height) return;
    // Typing a size means the preset no longer describes it.
    updateSettings({ browserDefaultViewport: { _tag: "freeform", ...next } });
  };

  return (
    <SettingsRow
      {...searchableSetting("browser-default-viewport")}
      description="The viewport a browser tab opens at, for both you and agents. Fill sizes the page to the panel; any other choice opens the device toolbar at that size."
      resetAction={
        !disabled && viewport._tag !== DEFAULT_BROWSER_VIEWPORT._tag ? (
          <SettingResetButton
            label="default browser viewport"
            onClick={() => updateSettings({ browserDefaultViewport: DEFAULT_BROWSER_VIEWPORT })}
          />
        ) : null
      }
      control={
        <div className="flex w-full flex-wrap items-center justify-end gap-2 sm:w-auto">
          <Select
            value={viewportSelectValue(viewport)}
            onValueChange={selectViewport}
            disabled={disabled}
          >
            <SelectTrigger
              size="sm"
              className="w-full min-w-0 sm:w-44"
              aria-label="Default browser viewport"
            >
              <SelectValue>{viewportSelectLabel(viewport)}</SelectValue>
            </SelectTrigger>
            <SelectPopup align="end" alignItemWithTrigger={false} className="min-w-64">
              <SelectItem value={FILL_VALUE}>Fill panel</SelectItem>
              <SelectItem value={RESPONSIVE_VALUE}>Responsive</SelectItem>
              <SelectGroup>
                <SelectGroupLabel>Standard</SelectGroupLabel>
                {PREVIEW_VIEWPORT_PRESETS.map((preset) => (
                  <SelectItem key={preset.id} value={preset.id}>
                    <span className="flex w-full items-center justify-between gap-5">
                      <span>{preset.label}</span>
                      <span className="text-xs tabular-nums text-muted-foreground">
                        {preset.detail}
                      </span>
                    </span>
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectPopup>
          </Select>

          {sized ? (
            <div className="flex min-w-0 items-center gap-1">
              <NumberField
                value={presentedSize.width}
                min={PREVIEW_VIEWPORT_MIN_DIMENSION}
                max={PREVIEW_VIEWPORT_MAX_DIMENSION}
                disabled={disabled}
                // Pixel counts read as raw numbers; grouping would show "1,024".
                format={NO_GROUPING}
                size="sm"
                className="w-20"
                onValueCommitted={(value) => commitDimension("width", value)}
              >
                <NumberFieldGroup>
                  <NumberFieldInput aria-label="Default viewport width" />
                </NumberFieldGroup>
              </NumberField>
              <span className="text-xs text-muted-foreground">×</span>
              <NumberField
                value={presentedSize.height}
                min={PREVIEW_VIEWPORT_MIN_DIMENSION}
                max={PREVIEW_VIEWPORT_MAX_DIMENSION}
                disabled={disabled}
                format={NO_GROUPING}
                size="sm"
                className="w-20"
                onValueCommitted={(value) => commitDimension("height", value)}
              >
                <NumberFieldGroup>
                  <NumberFieldInput aria-label="Default viewport height" />
                </NumberFieldGroup>
              </NumberField>
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Button
                      size="icon-sm"
                      variant="ghost-muted"
                      disabled={disabled}
                      aria-label={`Rotate to ${
                        presentedSize.height >= presentedSize.width ? "landscape" : "portrait"
                      }`}
                      onClick={() =>
                        updateSettings({ browserDefaultViewport: rotateViewport(sized) })
                      }
                    >
                      <ScreenRotationIcon />
                    </Button>
                  }
                />
                <TooltipPopup side="top">Rotate</TooltipPopup>
              </Tooltip>
            </div>
          ) : null}
        </div>
      }
    />
  );
}

function BrowserZoomSetting({ disabled }: { readonly disabled: boolean }) {
  const zoomFactor = useClientSettings((settings) => settings.browserDefaultZoomFactor);
  const updateSettings = useUpdatePrimarySettings();

  return (
    <SettingsRow
      {...searchableSetting("browser-default-zoom")}
      description="Page zoom applied to new browser tabs."
      resetAction={
        !disabled && zoomFactor !== DEFAULT_PREVIEW_ZOOM_FACTOR ? (
          <SettingResetButton
            label="default browser zoom"
            onClick={() =>
              updateSettings({ browserDefaultZoomFactor: DEFAULT_PREVIEW_ZOOM_FACTOR })
            }
          />
        ) : null
      }
      control={
        <Select
          disabled={disabled}
          value={String(zoomFactor)}
          onValueChange={(value) => {
            const next = PREVIEW_ZOOM_LEVELS.find((level) => String(level) === value);
            if (next !== undefined) updateSettings({ browserDefaultZoomFactor: next });
          }}
        >
          <SelectTrigger className="w-full sm:w-40" aria-label="Default browser zoom">
            <SelectValue>{zoomLabel(zoomFactor)}</SelectValue>
          </SelectTrigger>
          <SelectPopup align="end" alignItemWithTrigger={false}>
            {PREVIEW_ZOOM_LEVELS.map((level) => (
              <SelectItem hideIndicator key={level} value={String(level)}>
                {zoomLabel(level)}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      }
    />
  );
}

function BrowserAppearanceSetting({ disabled }: { readonly disabled: boolean }) {
  const appearance = useClientSettings((settings) => settings.browserDefaultAppearance);
  const updateSettings = useUpdatePrimarySettings();

  return (
    <SettingsRow
      {...searchableSetting("browser-default-appearance")}
      description="The color scheme pages are told to prefer. System follows your OS setting."
      resetAction={
        !disabled && appearance !== DEFAULT_PREVIEW_APPEARANCE ? (
          <SettingResetButton
            label="default browser appearance"
            onClick={() => updateSettings({ browserDefaultAppearance: DEFAULT_PREVIEW_APPEARANCE })}
          />
        ) : null
      }
      control={
        <Select
          disabled={disabled}
          value={appearance}
          onValueChange={(value) => {
            if (value === "system" || value === "light" || value === "dark") {
              updateSettings({ browserDefaultAppearance: value });
            }
          }}
        >
          <SelectTrigger className="w-full sm:w-40" aria-label="Default browser appearance">
            <SelectValue>{APPEARANCE_LABELS[appearance]}</SelectValue>
          </SelectTrigger>
          <SelectPopup align="end" alignItemWithTrigger={false}>
            {Object.entries(APPEARANCE_LABELS).map(([value, label]) => (
              <SelectItem hideIndicator key={value} value={value}>
                {label}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      }
    />
  );
}

function BrowserRecordingFrameRateSetting({ disabled }: { readonly disabled: boolean }) {
  const frameRate = useClientSettings((settings) => settings.browserRecordingFrameRate);
  const updateSettings = useUpdatePrimarySettings();

  return (
    <SettingsRow
      {...searchableSetting("browser-recording-frame-rate")}
      description="Maximum frame rate for browser recordings. 30 fps is the default and uses less CPU and storage; 60 fps captures smoother motion."
      resetAction={
        !disabled && frameRate !== DEFAULT_BROWSER_RECORDING_FRAME_RATE ? (
          <SettingResetButton
            label="browser recording frame rate"
            onClick={() =>
              updateSettings({ browserRecordingFrameRate: DEFAULT_BROWSER_RECORDING_FRAME_RATE })
            }
          />
        ) : null
      }
      control={
        <Select
          disabled={disabled}
          value={String(frameRate)}
          onValueChange={(value) => {
            const next = BROWSER_RECORDING_FRAME_RATES.find((rate) => String(rate) === value);
            if (next !== undefined) {
              updateSettings({ browserRecordingFrameRate: next });
            }
          }}
        >
          <SelectTrigger className="w-full sm:w-40" aria-label="Browser recording frame rate">
            <SelectValue>{frameRate} fps</SelectValue>
          </SelectTrigger>
          <SelectPopup align="end" alignItemWithTrigger={false}>
            {BROWSER_RECORDING_FRAME_RATES.map((rate) => (
              <SelectItem hideIndicator key={rate} value={String(rate)}>
                {rate} fps
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      }
    />
  );
}

function AgentBrowserAccessSetting() {
  const settings = usePrimarySettings();
  const updateSettings = useUpdatePrimarySettings();

  return (
    <SettingsRow
      serverScoped
      {...searchableSetting("agent-browser-access")}
      description="Let agents open and drive the preview browser. When off, the browser tools and the instructions describing them are withheld from agent sessions. Your own browser panel is unaffected."
      status={
        settings.enableAgentBrowserAccess
          ? undefined
          : "Applies to sessions started from now on; a running agent keeps the tools it was given."
      }
      resetAction={
        settings.enableAgentBrowserAccess !== DEFAULT_UNIFIED_SETTINGS.enableAgentBrowserAccess ? (
          <SettingResetButton
            label="agent browser access"
            onClick={() =>
              updateSettings({
                enableAgentBrowserAccess: DEFAULT_UNIFIED_SETTINGS.enableAgentBrowserAccess,
              })
            }
          />
        ) : null
      }
      control={
        <Switch
          checked={settings.enableAgentBrowserAccess}
          onCheckedChange={(checked) =>
            updateSettings({ enableAgentBrowserAccess: Boolean(checked) })
          }
          aria-label="Allow agent browser access"
        />
      }
    />
  );
}

function BrowserAutoShowFloatingPreviewSetting({ disabled }: { readonly disabled: boolean }) {
  const autoShow = useClientSettings((settings) => settings.browserAutoShowFloatingPreview);
  const updateSettings = useUpdatePrimarySettings();

  return (
    <SettingsRow
      {...searchableSetting("browser-auto-show-floating-preview")}
      description="Pop the floating preview into view when an agent opens a browser. An agent that explicitly asks to show or hide its preview still gets what it asked for."
      resetAction={
        !disabled && autoShow !== DEFAULT_BROWSER_AUTO_SHOW_FLOATING_PREVIEW ? (
          <SettingResetButton
            label="auto-show floating preview"
            onClick={() =>
              updateSettings({
                browserAutoShowFloatingPreview: DEFAULT_BROWSER_AUTO_SHOW_FLOATING_PREVIEW,
              })
            }
          />
        ) : null
      }
      control={
        <Switch
          disabled={disabled}
          checked={autoShow}
          onCheckedChange={(checked) =>
            updateSettings({ browserAutoShowFloatingPreview: Boolean(checked) })
          }
          aria-label="Auto-show floating preview"
        />
      }
    />
  );
}

/**
 * Frames the client-local preview defaults as one unavailable block.
 *
 * Disabling each control on its own left the labels and descriptions at full
 * strength, so the group still read as editable. Boxing it puts the reason at
 * the top and dims everything it covers, which is also why the explanation
 * sits outside the dimmed area — the one part that must stay readable is the
 * part saying why the rest isn't.
 *
 * Disabled rather than hidden because these are *client* settings: editing
 * them from a browser tab would write preferences belonging to a different
 * client, reading as though the desktop app had been configured when it
 * hadn't.
 */
function DesktopOnlyBrowserDefaults({ children }: { readonly children: ReactNode }) {
  return (
    <div className="rounded-xl border border-border/60 bg-muted/20 py-1.5">
      <div className="flex items-start gap-2 px-3 py-2 text-[12px] leading-relaxed text-muted-foreground sm:px-4">
        <InfoIcon className="mt-0.5 size-3.5 shrink-0 text-warning" />
        <p>Only available in the desktop app.</p>
      </div>
      <div className="[&_h3]:opacity-64 [&_p]:opacity-64">{children}</div>
    </div>
  );
}

function ExternalMcpServersSettings() {
  const servers = usePrimarySettings((settings) => settings.externalMcpServers);
  const providerInstances = usePrimarySettings((settings) => settings.providerInstances);
  const updateSettings = useUpdatePrimarySettings();
  const [newId, setNewId] = useState("");
  const [newUrl, setNewUrl] = useState("");
  const validId = /^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/.test(newId) && newId !== "t3-code";

  const replaceServer = (id: string, next: (typeof servers)[keyof typeof servers] | null) => {
    const updated = { ...servers } as Record<string, (typeof servers)[keyof typeof servers]>;
    if (next) updated[id] = next;
    else delete updated[id];
    updateSettings({ externalMcpServers: updated as typeof servers });
  };

  const addServer = () => {
    const id = newId.trim();
    const url = newUrl.trim();
    if (!validId || !url || servers[id as keyof typeof servers]) return;
    replaceServer(id, { url, headers: [], enabled: true });
    setNewId("");
    setNewUrl("");
  };

  return (
    <SettingsSection id="external-mcp-servers" title="External MCP servers">
      <p className="px-3 text-sm text-muted-foreground sm:px-4">
        Streamable HTTP servers are attached to new provider sessions. Secrets are stored outside
        settings.json. Codex currently supports sensitive Authorization: Bearer headers only; other
        configured headers still apply to Claude, Cursor, Grok, and T3-managed OpenCode. External
        OpenCode runtimes do not support this integration.
      </p>
      {Object.entries(servers).map(([id, server]) => (
        <div key={id} className="mx-3 space-y-3 rounded-lg border p-3 sm:mx-4">
          <div className="flex items-center gap-2">
            <Input
              className="max-w-48"
              defaultValue={server.displayName ?? ""}
              placeholder={id}
              onBlur={(event) => {
                const displayName = event.currentTarget.value.trim();
                const { displayName: _omit, ...withoutDisplayName } = server;
                replaceServer(id, displayName ? { ...server, displayName } : withoutDisplayName);
              }}
              aria-label={`${id} display name`}
            />
            <Input
              defaultValue={server.url}
              onBlur={(event) =>
                replaceServer(id, { ...server, url: event.currentTarget.value.trim() })
              }
              aria-label={`${id} MCP URL`}
            />
            <Switch
              checked={server.enabled}
              onCheckedChange={(enabled) => replaceServer(id, { ...server, enabled })}
              aria-label={`Enable ${id}`}
            />
            <Button
              type="button"
              size="icon-sm"
              variant="ghost"
              onClick={() => replaceServer(id, null)}
              aria-label={`Delete ${id}`}
            >
              <Trash2Icon className="size-4" />
            </Button>
          </div>
          <div className="space-y-2">
            {server.headers.map((header, index) => (
              <div key={header.name} className="flex items-center gap-2">
                <Input
                  defaultValue={header.name}
                  placeholder="Header name"
                  onBlur={(event) => {
                    const headers = [...server.headers];
                    headers[index] = { ...header, name: event.currentTarget.value.trim() };
                    replaceServer(id, { ...server, headers });
                  }}
                  aria-label={`${id} header ${index + 1} name`}
                />
                <Input
                  defaultValue={header.valueRedacted ? "" : header.value}
                  type={header.sensitive ? "password" : "text"}
                  placeholder={header.valueRedacted ? "Stored secret - enter to replace" : "Value"}
                  autoComplete="off"
                  onBlur={(event) => {
                    if (header.valueRedacted && !event.currentTarget.value) return;
                    const headers = [...server.headers];
                    headers[index] = {
                      ...header,
                      value: event.currentTarget.value,
                      valueRedacted: false,
                    };
                    replaceServer(id, { ...server, headers });
                  }}
                  aria-label={`${id} header ${index + 1} value`}
                />
                <label className="flex items-center gap-1 text-xs text-muted-foreground">
                  <Checkbox
                    checked={header.sensitive}
                    onCheckedChange={async (checked) => {
                      const sensitive = Boolean(checked);
                      if (!sensitive && header.sensitive && header.valueRedacted) {
                        const confirmed = await ensureLocalApi().dialogs.confirm(
                          `Stop storing ${header.name} as a secret? The stored value will be deleted.`,
                          { variant: "destructive" },
                        );
                        if (!confirmed) return;
                      }
                      const headers = [...server.headers];
                      headers[index] = {
                        ...header,
                        sensitive,
                        ...(!sensitive ? { value: "", valueRedacted: false } : {}),
                      };
                      replaceServer(id, { ...server, headers });
                    }}
                  />
                  Secret
                </label>
                <Button
                  type="button"
                  size="icon-sm"
                  variant="ghost"
                  onClick={() =>
                    replaceServer(id, {
                      ...server,
                      headers: server.headers.filter((_, candidate) => candidate !== index),
                    })
                  }
                  aria-label={`Delete ${id} header ${index + 1}`}
                >
                  <Trash2Icon className="size-4" />
                </Button>
              </div>
            ))}
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() =>
                replaceServer(id, {
                  ...server,
                  headers: [
                    ...server.headers,
                    {
                      name: `X-MCP-Header-${server.headers.length + 1}`,
                      value: "",
                      sensitive: true,
                    } satisfies ExternalMcpHeader,
                  ],
                })
              }
            >
              <PlusIcon className="size-4" /> Add header
            </Button>
          </div>
          <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
            <span>Provider instances:</span>
            {Object.entries(providerInstances).map(([instanceId, instance]) => {
              const checked = server.providerInstances?.includes(instanceId as never) ?? true;
              return (
                <label key={instanceId} className="flex items-center gap-1">
                  <Checkbox
                    checked={checked}
                    onCheckedChange={(selected) => {
                      const current = server.providerInstances ?? Object.keys(providerInstances);
                      const next = selected
                        ? [...new Set([...current, instanceId])]
                        : current.filter((candidate) => candidate !== instanceId);
                      replaceServer(id, { ...server, providerInstances: next as never });
                    }}
                  />
                  {instance.displayName || instanceId}
                </label>
              );
            })}
          </div>
        </div>
      ))}
      <div className="mx-3 flex gap-2 rounded-lg border border-dashed p-3 sm:mx-4">
        <Input
          value={newId}
          onChange={(event) => setNewId(event.target.value)}
          placeholder="server-id"
        />
        <Input
          value={newUrl}
          onChange={(event) => setNewUrl(event.target.value)}
          placeholder="https://example.com/mcp"
        />
        <Button type="button" onClick={addServer} disabled={!validId || !newUrl.trim()}>
          Add server
        </Button>
      </div>
    </SettingsSection>
  );
}

export function IntegrationsSettingsPanel() {
  // Client-local preview defaults are editable only where the preview exists.
  const previewDefaultsDisabled = !isElectron;
  const previewDefaults = (
    <>
      <BrowserViewportSetting disabled={previewDefaultsDisabled} />
      <BrowserZoomSetting disabled={previewDefaultsDisabled} />
      <BrowserAppearanceSetting disabled={previewDefaultsDisabled} />
      <BrowserRecordingFrameRateSetting disabled={previewDefaultsDisabled} />
      <BrowserAutoShowFloatingPreviewSetting disabled={previewDefaultsDisabled} />
    </>
  );

  return (
    <SettingsPageContainer>
      <SettingsSection id="browser" title="Browser">
        {/* Server-authoritative, so it stays editable on any client anchored to
            a server; `serverScoped` covers the hosted app, which has none. It
            sits outside the block covering the desktop-only defaults. */}
        <AgentBrowserAccessSetting />
        {previewDefaultsDisabled ? (
          <DesktopOnlyBrowserDefaults>{previewDefaults}</DesktopOnlyBrowserDefaults>
        ) : (
          previewDefaults
        )}
      </SettingsSection>
      <ExternalMcpServersSettings />
    </SettingsPageContainer>
  );
}
