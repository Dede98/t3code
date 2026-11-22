import type { ExternalMcpServerConfig, ServerProvider } from "@t3tools/contracts";
import { T, Var, useGT, useMessages } from "gt-react";
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
  const gt = useGT();
  const m = useMessages();
  const problem = validate(draft, takenIds, isNew, m);
  const serverName = server?.displayName ?? serverId ?? "";
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
          {isNew ? gt("Add MCP server") : gt("Edit {name}", { name: serverName })}
        </DialogTitle>
        <DialogDescription>
          <T>A Streamable HTTP server whose tools agents on this environment get.</T>
        </DialogDescription>
      </DialogHeader>
      <DialogPanel>
        <div className="space-y-5 text-sm">
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="block space-y-1.5">
              <span>
                <T>ID</T>
              </span>
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
                <T>
                  Display name <span className="text-muted-foreground">(optional)</span>
                </T>
              </span>
              <Input
                value={draft.displayName}
                // oxlint-disable-next-line t3code/no-untranslated-copy -- a product name as the example
                placeholder={draft.id || "Linear"}
                onChange={(event) => update({ displayName: event.target.value })}
              />
            </label>
          </div>
          <label className="block space-y-1.5">
            <span>
              <T>URL</T>
            </span>
            <Input
              type="url"
              value={draft.url}
              placeholder="https://example.com/mcp"
              onChange={(event) => update({ url: event.target.value })}
            />
          </label>

          <fieldset className="space-y-2">
            <legend className="mb-1.5">
              <T>Authentication</T>
            </legend>
            <ToggleGroup
              aria-label={gt("Authentication")}
              variant="segmented"
              value={[draft.auth]}
              onValueChange={(next) => {
                const auth = next[0];
                if (auth === "headers" || auth === "oauth") update({ auth });
              }}
            >
              <Toggle value="oauth">
                <T>Sign in with OAuth</T>
              </Toggle>
              <Toggle value="headers">
                <T>Headers only</T>
              </Toggle>
            </ToggleGroup>
            <p className="text-xs text-muted-foreground">
              {draft.auth === "oauth"
                ? gt(
                    "Sign in after saving. This environment keeps the sign-in, refreshes it, and passes the server to agents through T3 Code.",
                  )
                : gt(
                    "Send fixed headers, such as an API key. Codex only receives a secret Authorization header.",
                  )}
            </p>
            {draft.auth === "oauth" ? (
              <details className="text-xs">
                <summary className="cursor-pointer text-muted-foreground">
                  <T>Advanced</T>
                </summary>
                <div className="mt-2 grid gap-3 sm:grid-cols-2">
                  <label className="block space-y-1.5">
                    <span>
                      <T>Client ID</T>
                    </span>
                    <Input
                      size="sm"
                      value={draft.clientId}
                      placeholder={gt("Registered automatically")}
                      onChange={(event) => update({ clientId: event.target.value })}
                    />
                  </label>
                  <label className="block space-y-1.5">
                    <span>
                      <T>Scopes</T>
                    </span>
                    <Input
                      size="sm"
                      value={draft.scopes}
                      placeholder={gt("From the server")}
                      onChange={(event) => update({ scopes: event.target.value })}
                    />
                  </label>
                </div>
                <p className="mt-2 text-muted-foreground">
                  <T>
                    Only needed when the authorization server does not register clients on its own.
                    It must accept <Var>{"http://127.0.0.1"}</Var> redirect URIs.
                  </T>
                </p>
              </details>
            ) : null}
          </fieldset>

          <fieldset className="space-y-2">
            <legend className="mb-1.5">
              {draft.auth === "oauth" ? (
                <T>
                  Headers <span className="text-muted-foreground">(optional)</span>
                </T>
              ) : (
                <T>Headers</T>
              )}
            </legend>
            {draft.headers.map((header) => (
              <div key={header.key} className="flex items-center gap-2">
                <Input
                  size="sm"
                  value={header.name}
                  // oxlint-disable-next-line t3code/no-untranslated-copy -- an HTTP header name
                  placeholder="Authorization"
                  aria-label={gt("Header name")}
                  onChange={(event) => updateHeader(header.key, { name: event.target.value })}
                />
                <Input
                  size="sm"
                  value={header.value}
                  type={header.sensitive ? "password" : "text"}
                  autoComplete="off"
                  placeholder={
                    keepsStoredSecret(header) ? gt("Stored secret, unchanged") : gt("Value")
                  }
                  aria-label={
                    header.name ? gt("{name} value", { name: header.name }) : gt("Header value")
                  }
                  onChange={(event) => updateHeader(header.key, { value: event.target.value })}
                />
                <label className="flex shrink-0 items-center gap-1.5 text-xs text-muted-foreground">
                  <Checkbox
                    checked={header.sensitive}
                    onCheckedChange={(checked) =>
                      updateHeader(header.key, { sensitive: Boolean(checked) })
                    }
                  />
                  <T>Secret</T>
                </label>
                <Button
                  type="button"
                  size="icon-sm"
                  variant="ghost"
                  aria-label={
                    header.name ? gt("Remove {name}", { name: header.name }) : gt("Remove header")
                  }
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
              <PlusIcon className="size-4" /> <T>Add header</T>
            </Button>
          </fieldset>

          {providers.length > 0 ? (
            <fieldset className="space-y-2">
              <legend className="mb-1.5">
                <T>Providers</T>
              </legend>
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
                <T>Every provider, including ones added later</T>
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
              <T>
                Saving these changes signs out of <Var>{serverName}</Var>. Sign in again afterwards.
              </T>
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
          <T>Cancel</T>
        </Button>
        <Button type="submit">{isNew ? gt("Add server") : gt("Save")}</Button>
      </DialogFooter>
    </form>
  );
}
