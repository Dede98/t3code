import type { EnvironmentId, ExternalMcpHeader } from "@t3tools/contracts";
import { Plus as PlusIcon, Trash2Icon } from "lucide-react";
import { useState } from "react";

import { useEnvironmentSettings, useUpdateEnvironmentSettings } from "~/hooks/useSettings";
import { cn } from "~/lib/utils";
import { ensureLocalApi } from "~/localApi";

import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import { Input } from "../ui/input";
import { Switch } from "../ui/switch";
import { SettingsSection } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

export function ExternalMcpServersSettings({
  environmentId,
  disabled = false,
}: {
  readonly environmentId: EnvironmentId;
  readonly disabled?: boolean;
}) {
  const settings = useEnvironmentSettings(environmentId);
  const servers = settings.externalMcpServers;
  const providerInstances = settings.providerInstances;
  const updateSettings = useUpdateEnvironmentSettings(environmentId);
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
    <SettingsSection {...searchableSetting("external-mcp-servers")}>
      <p className="px-3 text-sm text-muted-foreground sm:px-4">
        Streamable HTTP servers are attached to new provider sessions. Secrets are stored outside
        settings.json. Codex currently supports sensitive Authorization: Bearer headers only; other
        configured headers still apply to Claude, Cursor, Grok, and T3-managed OpenCode. External
        OpenCode runtimes do not support this integration. Servers apply to every project on this
        environment.
      </p>
      {Object.entries(servers).map(([id, server]) => (
        <div
          key={id}
          inert={disabled}
          aria-disabled={disabled || undefined}
          className={cn(
            "mx-3 space-y-3 rounded-lg border p-3 sm:mx-4",
            disabled && "opacity-50 select-none",
          )}
        >
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
      <div
        inert={disabled}
        aria-disabled={disabled || undefined}
        className={cn(
          "mx-3 flex gap-2 rounded-lg border border-dashed p-3 sm:mx-4",
          disabled && "opacity-50 select-none",
        )}
      >
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
