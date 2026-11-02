import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ExternalMcpOAuthStatus,
  ExternalMcpServerConfig,
  ExternalMcpServerId,
} from "@t3tools/contracts";
import { isLoopbackHost } from "@t3tools/shared/preview";
import { useRef, useState } from "react";

import { ensureLocalApi } from "../../localApi";
import { serverEnvironment } from "../../state/server";
import { readPreparedConnection } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import { Input } from "../ui/input";

const commandOptions = { reportFailure: false, reportDefect: false };

// The server catches the redirect only when this browser runs on the server's machine.
const serverOnThisMachine = (environmentId: EnvironmentId) => {
  const url = readPreparedConnection(environmentId)?.httpBaseUrl;
  return url !== undefined && isLoopbackHost(new URL(url).hostname);
};

function describe(status: ExternalMcpOAuthStatus | undefined) {
  if (status?.flow) return "Finish signing in in your browser.";
  switch (status?.state) {
    case "connected":
      return "Signed in. New and idle sessions use this server.";
    case "needs-login":
      return "Sign-in expired.";
    default:
      return "Not signed in. Agents do not get this server until you sign in.";
  }
}

export function ExternalMcpOAuthControls({
  environmentId,
  serverId,
  server,
  status,
  disabled,
  onChange,
}: {
  readonly environmentId: EnvironmentId;
  readonly serverId: string;
  readonly server: ExternalMcpServerConfig;
  readonly status: ExternalMcpOAuthStatus | undefined;
  readonly disabled: boolean;
  readonly onChange: (next: ExternalMcpServerConfig) => void;
}) {
  const start = useAtomCommand(serverEnvironment.startExternalMcpOAuth, commandOptions);
  const complete = useAtomCommand(serverEnvironment.completeExternalMcpOAuth, commandOptions);
  const cancel = useAtomCommand(serverEnvironment.cancelExternalMcpOAuth, commandOptions);
  const logout = useAtomCommand(serverEnvironment.logoutExternalMcpOAuth, commandOptions);
  const [callbackUrl, setCallbackUrl] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const oauth = server.auth?.type === "oauth" ? server.auth : undefined;
  const id = serverId as ExternalMcpServerId;
  const flow = status?.flow ?? null;
  const busy = disabled || pending;

  async function run(command: () => Promise<AtomCommandResult<unknown, unknown>>) {
    if (pendingRef.current) return false;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    let succeeded = false;
    try {
      const result = await command();
      if (result._tag === "Success") succeeded = true;
      else if (!isAtomCommandInterrupted(result)) {
        const failure = squashAtomCommandFailure(result);
        setError(failure instanceof Error ? failure.message : "Sign-in failed. Try again.");
      }
    } catch {
      setError("Sign-in failed. Try again.");
    }
    pendingRef.current = false;
    setPending(false);
    return succeeded;
  }

  async function openBrowser(url: string) {
    try {
      await ensureLocalApi().shell.openExternal(url);
    } catch {
      setError("Could not open the browser. Copy the link and open it yourself.");
    }
  }

  async function signIn() {
    // Reserve the tab during the click; browsers block opens after an await.
    const tab = window.desktopBridge ? null : window.open("", "_blank");
    if (tab) tab.opener = null;
    let authorizationUrl: string | undefined;
    const ok = await run(async () => {
      const result = await start({
        environmentId,
        input: {
          serverId: id,
          callbackMode: serverOnThisMachine(environmentId) ? "server" : "client",
        },
      });
      if (result._tag === "Success") authorizationUrl = result.value.flow?.authorizationUrl;
      return result;
    });
    if (!ok || !authorizationUrl) {
      tab?.close();
      return;
    }
    if (tab) tab.location.href = authorizationUrl;
    else await openBrowser(authorizationUrl);
  }

  const message = error ?? status?.message ?? null;

  return (
    <div className="space-y-2 text-sm">
      <label className="flex items-center gap-2">
        <Checkbox
          checked={oauth !== undefined}
          disabled={disabled}
          onCheckedChange={(checked) => {
            const { auth: _auth, ...rest } = server;
            onChange(
              checked
                ? {
                    ...rest,
                    // OAuth owns the Authorization header.
                    headers: server.headers.filter(
                      (header) => header.name.toLowerCase() !== "authorization",
                    ),
                    auth: { type: "oauth" },
                  }
                : rest,
            );
          }}
        />
        Sign in with OAuth
      </label>
      {oauth ? (
        <div className="space-y-2 ps-6">
          <p role="status" className="text-muted-foreground">
            {describe(status)}
            {status?.issuer && URL.canParse(status.issuer)
              ? ` Authorization server: ${new URL(status.issuer).host}.`
              : null}
          </p>
          {message ? (
            <p role="alert" className="text-destructive-foreground [overflow-wrap:anywhere]">
              {message}
            </p>
          ) : null}
          <div className="flex flex-wrap items-center gap-2">
            {flow ? (
              <>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={busy}
                  onClick={() => void openBrowser(flow.authorizationUrl)}
                >
                  Open browser
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  disabled={busy}
                  onClick={() =>
                    void run(() =>
                      cancel({ environmentId, input: { serverId: id, flowId: flow.flowId } }),
                    )
                  }
                >
                  Cancel
                </Button>
              </>
            ) : (
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={busy}
                onClick={() => void signIn()}
              >
                {status?.state === "connected" ? "Sign in again" : "Sign in"}
              </Button>
            )}
            {status?.state === "connected" && !flow ? (
              <Button
                type="button"
                size="sm"
                variant="ghost"
                disabled={busy}
                onClick={() => void run(() => logout({ environmentId, input: { serverId: id } }))}
              >
                Sign out
              </Button>
            ) : null}
          </div>
          {flow ? (
            <form
              className="grid gap-2"
              onSubmit={(event) => {
                event.preventDefault();
                if (!callbackUrl.trim()) return;
                void run(() =>
                  complete({
                    environmentId,
                    input: { serverId: id, flowId: flow.flowId, callbackUrl },
                  }),
                ).then((sent) => {
                  if (sent) setCallbackUrl("");
                });
              }}
            >
              <label className="grid gap-1 text-muted-foreground">
                {flow.serverReceivesCallback
                  ? "If the final 127.0.0.1 page does not load, paste its full URL here."
                  : "After you approve, the browser lands on a 127.0.0.1 page that will not load. Paste its full URL here."}
                <Input
                  size="sm"
                  type="url"
                  autoComplete="off"
                  value={callbackUrl}
                  disabled={busy}
                  maxLength={16_384}
                  onChange={(event) => setCallbackUrl(event.target.value)}
                  aria-label={`${serverId} redirect URL`}
                />
              </label>
              <Button
                type="submit"
                size="sm"
                variant="outline"
                className="w-fit"
                disabled={busy || !callbackUrl.trim()}
              >
                Continue
              </Button>
            </form>
          ) : null}
          <Input
            key={oauth.clientId ?? ""}
            size="sm"
            className="max-w-72"
            defaultValue={oauth.clientId ?? ""}
            placeholder="Client ID (optional)"
            disabled={disabled}
            onBlur={(event) => {
              const clientId = event.currentTarget.value.trim();
              if (clientId === (oauth.clientId ?? "")) return;
              const { clientId: _omit, ...rest } = oauth;
              onChange({ ...server, auth: clientId ? { ...rest, clientId } : rest });
            }}
            aria-label={`${serverId} OAuth client ID`}
          />
        </div>
      ) : null}
    </div>
  );
}
