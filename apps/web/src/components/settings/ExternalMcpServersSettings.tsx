import { useAtomValue } from "@effect/atom-react";
import type {
  EnvironmentId,
  ExternalMcpOAuthStatus,
  ExternalMcpServerConfig,
  ExternalMcpServerId,
} from "@t3tools/contracts";
import { T, useGT } from "gt-react";
import { EllipsisIcon, PlusIcon } from "lucide-react";
import { useState } from "react";

import { useEnvironmentSettings, useUpdateEnvironmentSettings } from "~/hooks/useSettings";
import { cn } from "~/lib/utils";
import { ensureLocalApi } from "~/localApi";
import { useEnvironmentQuery } from "~/state/query";
import { providersIgnoringCheckedAtAtom, serverEnvironment } from "~/state/server";

import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuSeparator, MenuTrigger } from "../ui/menu";
import { Switch } from "../ui/switch";
import {
  ExternalMcpOAuthStatusLine,
  ExternalMcpSignInPanel,
  useExternalMcpOAuth,
} from "./ExternalMcpOAuthControls";
import { ExternalMcpServerEditor } from "./ExternalMcpServerEditor";
import type { ExternalMcpServerDraft } from "./externalMcpServerDraft";
import { SettingsSection } from "./settingsLayout";
import { useSearchableSetting } from "./settingsSearch";

export function ExternalMcpServersSettings({
  environmentId,
  disabled = false,
}: {
  readonly environmentId: EnvironmentId;
  readonly disabled?: boolean;
}) {
  const searchableSetting = useSearchableSetting();
  const settings = useEnvironmentSettings(environmentId);
  const servers = settings.externalMcpServers;
  const updateSettings = useUpdateEnvironmentSettings(environmentId);
  const providers = useAtomValue(providersIgnoringCheckedAtAtom(environmentId));
  const hasOAuthServer = Object.values(servers).some((server) => server.auth?.type === "oauth");
  const oauthStatuses = useEnvironmentQuery(
    hasOAuthServer
      ? serverEnvironment.externalMcpOAuthStatuses({ environmentId, input: {} })
      : null,
  ).data;
  // Null id adds a server.
  const [editing, setEditing] = useState<{
    readonly id: ExternalMcpServerId | null;
    readonly draft?: ExternalMcpServerDraft;
  } | null>(null);

  const replaceServer = (
    id: ExternalMcpServerId,
    next: ExternalMcpServerConfig | null,
    onFailed?: () => void,
  ) => {
    const updated = { ...servers };
    if (next) updated[id] = next;
    else delete updated[id];
    updateSettings({ externalMcpServers: updated }, onFailed ? { onFailed } : undefined);
  };

  // Settings keys are decoded server ids.
  const entries = Object.entries(servers) as Array<[ExternalMcpServerId, ExternalMcpServerConfig]>;

  return (
    <SettingsSection
      {...searchableSetting("external-mcp-servers")}
      headerAction={
        <Button
          size="sm"
          variant="outline"
          disabled={disabled}
          onClick={() => setEditing({ id: null })}
        >
          <PlusIcon className="size-3.5" /> <T>Add server</T>
        </Button>
      }
    >
      <div inert={disabled} className={cn(disabled && "opacity-50 select-none")}>
        {entries.length === 0 ? (
          <p className="px-3 py-4 text-sm text-muted-foreground sm:px-4">
            <T>
              Give agents the tools of a remote MCP server, such as your issue tracker or asset
              store. Servers sign in with OAuth or send fixed headers.
            </T>
          </p>
        ) : (
          entries.map(([id, server]) => (
            <ExternalMcpServerRow
              key={id}
              environmentId={environmentId}
              id={id}
              server={server}
              status={oauthStatuses?.[id]}
              onEdit={() => setEditing({ id })}
              onChange={(next) => replaceServer(id, next)}
            />
          ))
        )}
        {entries.length > 0 ? (
          <p className="border-t border-border/60 px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
            <T>
              Every project on this environment gets these servers. Changes reach running threads
              once their sessions are idle.
            </T>
          </p>
        ) : null}
      </div>
      {editing ? (
        <ExternalMcpServerEditor
          serverId={editing.id}
          server={editing.id === null ? undefined : servers[editing.id]}
          initialDraft={editing.draft}
          takenIds={new Set(Object.keys(servers))}
          providers={providers}
          onSave={(id, next, draft) => {
            const reopen = { id: editing.id, draft };
            replaceServer(id as ExternalMcpServerId, next, () => setEditing(reopen));
            setEditing(null);
          }}
          onClose={() => setEditing(null)}
        />
      ) : null}
    </SettingsSection>
  );
}

function ExternalMcpServerRow({
  environmentId,
  id,
  server,
  status,
  onEdit,
  onChange,
}: {
  readonly environmentId: EnvironmentId;
  readonly id: string;
  readonly server: ExternalMcpServerConfig;
  readonly status: ExternalMcpOAuthStatus | undefined;
  readonly onEdit: () => void;
  readonly onChange: (next: ExternalMcpServerConfig | null) => void;
}) {
  const gt = useGT();
  const oauth = server.auth?.type === "oauth";
  const actions = useExternalMcpOAuth(environmentId, id);
  const name = server.displayName ?? id;
  const connected = status?.state === "connected";
  const flow = status?.flow ?? null;

  async function remove() {
    const confirmed = await ensureLocalApi().dialogs.confirm(
      oauth
        ? gt("Remove {name}? Its stored secrets and sign-in are deleted.", { name })
        : gt("Remove {name}? Its stored secrets are deleted.", { name }),
      { variant: "destructive" },
    );
    if (confirmed) onChange(null);
  }

  return (
    <div className="border-t border-border/60 px-3 py-3 first:border-t-0 sm:px-4">
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 items-center gap-1.5">
            <p className="truncate text-sm font-medium">{name}</p>
            <Badge size="sm" variant="outline">
              {oauth ? gt("OAuth") : server.headers.length > 0 ? gt("Headers") : gt("No auth")}
            </Badge>
            {!server.enabled ? (
              <Badge size="sm" variant="secondary">
                <T>Off</T>
              </Badge>
            ) : null}
          </div>
          <p className="truncate text-xs text-muted-foreground">{server.url}</p>
          {oauth ? <ExternalMcpOAuthStatusLine status={status} error={actions.error} /> : null}
        </div>
        <div className="flex shrink-0 items-center gap-1.5">
          {oauth && !connected && !flow ? (
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={actions.pending || status === undefined}
              onClick={() => void actions.signIn()}
            >
              <T>Sign in</T>
            </Button>
          ) : null}
          <Switch
            checked={server.enabled}
            onCheckedChange={(enabled) => onChange({ ...server, enabled })}
            aria-label={gt("Enable {name}", { name })}
          />
          <Menu>
            <MenuTrigger
              render={
                <Button
                  size="icon-sm"
                  variant="ghost"
                  aria-label={gt("{name} actions", { name })}
                />
              }
            >
              <EllipsisIcon className="size-4" />
            </MenuTrigger>
            <MenuPopup align="end">
              <MenuItem onClick={onEdit}>
                <T>Edit…</T>
              </MenuItem>
              {oauth && connected ? (
                <MenuItem onClick={() => void actions.signIn()}>
                  <T>Sign in again</T>
                </MenuItem>
              ) : null}
              {oauth && (connected || status?.state === "needs-login") ? (
                <MenuItem onClick={() => void actions.signOut()}>
                  <T>Sign out</T>
                </MenuItem>
              ) : null}
              <MenuSeparator />
              <MenuItem variant="destructive" onClick={() => void remove()}>
                <T>Remove</T>
              </MenuItem>
            </MenuPopup>
          </Menu>
        </div>
      </div>
      {oauth && flow ? (
        <ExternalMcpSignInPanel key={flow.flowId} serverName={name} flow={flow} actions={actions} />
      ) : null}
    </div>
  );
}
