import type { ExternalMcpServerConfig, ServerProvider } from "@t3tools/contracts";
import { PlusIcon, Trash2Icon } from "lucide-react";
import { useState } from "react";

import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Input } from "../ui/input";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import {
  endsSignIn,
  keepsStoredSecret,
  toConfig,
  toDraft,
  validate,
  type ExternalMcpServerDraft,
  type HeaderDraft,
} from "./externalMcpServerDraft";

export function ExternalMcpServerEditor({
  serverId,
  server,
  initialDraft,
  takenIds,
  providers,
  onSave,
  onClose,
}: {
  /** Null adds a new server. */
  readonly serverId: string | null;
  readonly server: ExternalMcpServerConfig | undefined;
  /** A draft whose save failed, opened again so nothing typed is lost. */
  readonly initialDraft?: ExternalMcpServerDraft | undefined;
  readonly takenIds: ReadonlySet<string>;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly onSave: (
    id: string,
    server: ExternalMcpServerConfig,
    draft: ExternalMcpServerDraft,
  ) => void;
  readonly onClose: () => void;
}) {
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      {/* The form owns the draft so a keystroke re-renders it, not the dialog chrome. */}
      <DialogPopup>
        <ExternalMcpServerForm
          serverId={serverId}
          server={server}
          initialDraft={initialDraft}
          takenIds={takenIds}
          providers={providers}
          onSave={onSave}
          onClose={onClose}
        />
      </DialogPopup>
    </Dialog>
  );
}

function ExternalMcpServerForm({
  serverId,
  server,
  initialDraft,
  takenIds,
  providers,
  onSave,
  onClose,
}: Parameters<typeof ExternalMcpServerEditor>[0]) {
  const isNew = serverId === null;
  const [draft, setDraft] = useState(() => initialDraft ?? toDraft(serverId ?? "", server));
  const [nextKey, setNextKey] = useState(
    () => Math.max(-1, ...draft.headers.map((header) => header.key)) + 1,
  );
  const [showErrors, setShowErrors] = useState(false);
  const problem = validate(draft, takenIds, isNew);
  const update = (patch: Partial<ExternalMcpServerDraft>) =>
    setDraft((current) => ({ ...current, ...patch }));
  const updateHeader = (key: number, patch: Partial<HeaderDraft>) =>
    update({
      headers: draft.headers.map((header) =>
        header.key === key ? { ...header, ...patch } : header,
      ),
    });
  const selectedInstances = draft.providerInstances;

  return (
    <form
      className="flex min-h-0 flex-col"
      onSubmit={(event) => {
        event.preventDefault();
        if (problem) {
          setShowErrors(true);
          return;
        }
        onSave(isNew ? draft.id : serverId, toConfig(draft, server?.enabled ?? true), draft);
      }}
    >
      <DialogHeader>
        <DialogTitle>
          {isNew ? "Add MCP server" : `Edit ${server?.displayName ?? serverId}`}
        </DialogTitle>
        <DialogDescription>
          A Streamable HTTP server whose tools agents on this environment get.
        </DialogDescription>
      </DialogHeader>
      <DialogPanel>
        <div className="space-y-5 text-sm">
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="block space-y-1.5">
              <span>ID</span>
              <Input
                value={draft.id}
                disabled={!isNew}
                autoFocus={isNew}
                placeholder="linear"
                onChange={(event) => update({ id: event.target.value.trim() })}
              />
            </label>
            <label className="block space-y-1.5">
              <span>
                Display name <span className="text-muted-foreground">(optional)</span>
              </span>
              <Input
                value={draft.displayName}
                placeholder={draft.id || "Linear"}
                onChange={(event) => update({ displayName: event.target.value })}
              />
            </label>
          </div>
          <label className="block space-y-1.5">
            <span>URL</span>
            <Input
              type="url"
              value={draft.url}
              placeholder="https://example.com/mcp"
              onChange={(event) => update({ url: event.target.value })}
            />
          </label>

          <fieldset className="space-y-2">
            <legend className="mb-1.5">Authentication</legend>
            <ToggleGroup
              aria-label="Authentication"
              variant="segmented"
              value={[draft.auth]}
              onValueChange={(next) => {
                const auth = next[0];
                if (auth === "headers" || auth === "oauth") update({ auth });
              }}
            >
              <Toggle value="oauth">Sign in with OAuth</Toggle>
              <Toggle value="headers">Headers only</Toggle>
            </ToggleGroup>
            <p className="text-xs text-muted-foreground">
              {draft.auth === "oauth"
                ? "Sign in after saving. This environment keeps the sign-in, refreshes it, and passes the server to agents through T3 Code."
                : "Send fixed headers, such as an API key. Codex only receives a secret Authorization header."}
            </p>
            {draft.auth === "oauth" ? (
              <details className="text-xs">
                <summary className="cursor-pointer text-muted-foreground">Advanced</summary>
                <div className="mt-2 grid gap-3 sm:grid-cols-2">
                  <label className="block space-y-1.5">
                    <span>Client ID</span>
                    <Input
                      size="sm"
                      value={draft.clientId}
                      placeholder="Registered automatically"
                      onChange={(event) => update({ clientId: event.target.value })}
                    />
                  </label>
                  <label className="block space-y-1.5">
                    <span>Scopes</span>
                    <Input
                      size="sm"
                      value={draft.scopes}
                      placeholder="From the server"
                      onChange={(event) => update({ scopes: event.target.value })}
                    />
                  </label>
                </div>
                <p className="mt-2 text-muted-foreground">
                  Only needed when the authorization server does not register clients on its own. It
                  must accept http://127.0.0.1 redirect URIs.
                </p>
              </details>
            ) : null}
          </fieldset>

          <fieldset className="space-y-2">
            <legend className="mb-1.5">
              Headers{" "}
              {draft.auth === "oauth" ? (
                <span className="text-muted-foreground">(optional)</span>
              ) : null}
            </legend>
            {draft.headers.map((header) => (
              <div key={header.key} className="flex items-center gap-2">
                <Input
                  size="sm"
                  value={header.name}
                  placeholder="Authorization"
                  aria-label="Header name"
                  onChange={(event) => updateHeader(header.key, { name: event.target.value })}
                />
                <Input
                  size="sm"
                  value={header.value}
                  type={header.sensitive ? "password" : "text"}
                  autoComplete="off"
                  placeholder={keepsStoredSecret(header) ? "Stored secret, unchanged" : "Value"}
                  aria-label={`${header.name || "Header"} value`}
                  onChange={(event) => updateHeader(header.key, { value: event.target.value })}
                />
                <label className="flex shrink-0 items-center gap-1.5 text-xs text-muted-foreground">
                  <Checkbox
                    checked={header.sensitive}
                    onCheckedChange={(checked) =>
                      updateHeader(header.key, { sensitive: Boolean(checked) })
                    }
                  />
                  Secret
                </label>
                <Button
                  type="button"
                  size="icon-sm"
                  variant="ghost"
                  aria-label={`Remove ${header.name || "header"}`}
                  onClick={() =>
                    update({ headers: draft.headers.filter((item) => item.key !== header.key) })
                  }
                >
                  <Trash2Icon className="size-4" />
                </Button>
              </div>
            ))}
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => {
                update({
                  headers: [
                    ...draft.headers,
                    {
                      key: nextKey,
                      name:
                        draft.auth === "oauth" || draft.headers.length > 0 ? "" : "Authorization",
                      value: "",
                      sensitive: true,
                      storedSecretName: null,
                    },
                  ],
                });
                setNextKey(nextKey + 1);
              }}
            >
              <PlusIcon className="size-4" /> Add header
            </Button>
          </fieldset>

          {providers.length > 0 ? (
            <fieldset className="space-y-2">
              <legend className="mb-1.5">Providers</legend>
              <label className="flex items-center gap-2">
                <Checkbox
                  checked={selectedInstances === null}
                  onCheckedChange={(checked) =>
                    update({
                      providerInstances: checked
                        ? null
                        : providers.map((provider) => provider.instanceId),
                    })
                  }
                />
                Every provider, including ones added later
              </label>
              {selectedInstances !== null ? (
                <div className="grid gap-1.5 ps-6 sm:grid-cols-2">
                  {providers.map((provider) => (
                    <label key={provider.instanceId} className="flex items-center gap-2">
                      <Checkbox
                        checked={selectedInstances.includes(provider.instanceId)}
                        onCheckedChange={(checked) =>
                          update({
                            providerInstances: checked
                              ? [...selectedInstances, provider.instanceId]
                              : selectedInstances.filter((id) => id !== provider.instanceId),
                          })
                        }
                      />
                      <span className="truncate">
                        {provider.displayName ?? provider.instanceId}
                      </span>
                    </label>
                  ))}
                </div>
              ) : null}
            </fieldset>
          ) : null}

          {endsSignIn(draft, server) ? (
            <p className="text-xs text-muted-foreground">
              Saving these changes signs out of {server?.displayName ?? serverId}. Sign in again
              afterwards.
            </p>
          ) : null}
          {showErrors && problem ? (
            <p role="alert" className="text-destructive-foreground">
              {problem}
            </p>
          ) : null}
        </div>
      </DialogPanel>
      <DialogFooter>
        <Button type="button" variant="ghost" onClick={onClose}>
          Cancel
        </Button>
        <Button type="submit">{isNew ? "Add server" : "Save"}</Button>
      </DialogFooter>
    </form>
  );
}
