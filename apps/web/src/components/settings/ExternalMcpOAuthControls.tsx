import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ExternalMcpOAuthStatus,
  ExternalMcpServerId,
} from "@t3tools/contracts";
import { isLoopbackHost } from "@t3tools/shared/preview";
import { T, Var, msg, useGT } from "gt-react";
import { CopyIcon } from "lucide-react";
import { useRef, useState } from "react";

import { writeTextToClipboard } from "../../hooks/useCopyToClipboard";
import { ensureLocalApi } from "../../localApi";
import { serverEnvironment } from "../../state/server";
import { readPreparedConnection } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { ConnectionStatusDot } from "../ConnectionStatusDot";
import { Button, InlineButton } from "../ui/button";
import { Input } from "../ui/input";
import { Spinner } from "../ui/spinner";

const commandOptions = { reportFailure: false, reportDefect: false };

// The server catches the redirect only when this browser runs on the server's machine.
const serverOnThisMachine = (environmentId: EnvironmentId) => {
  const url = readPreparedConnection(environmentId)?.httpBaseUrl;
  return url !== undefined && isLoopbackHost(new URL(url).hostname);
};

const issuerHost = (issuer: string | null) =>
  issuer !== null && URL.canParse(issuer) ? new URL(issuer).host : null;

export function useExternalMcpOAuth(environmentId: EnvironmentId, serverId: string) {
  const start = useAtomCommand(serverEnvironment.startExternalMcpOAuth, commandOptions);
  const complete = useAtomCommand(serverEnvironment.completeExternalMcpOAuth, commandOptions);
  const cancel = useAtomCommand(serverEnvironment.cancelExternalMcpOAuth, commandOptions);
  const logout = useAtomCommand(serverEnvironment.logoutExternalMcpOAuth, commandOptions);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const gt = useGT();
  const id = serverId as ExternalMcpServerId;

  async function run(command: () => Promise<AtomCommandResult<unknown, unknown>>) {
    if (pendingRef.current) return false;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    const result = await command().catch(() => null);
    let succeeded = false;
    if (result === null) setError(gt("Sign-in failed. Try again."));
    else if (result._tag === "Success") succeeded = true;
    else if (!isAtomCommandInterrupted(result)) {
      const failure = squashAtomCommandFailure(result);
      setError(failure instanceof Error ? failure.message : gt("Sign-in failed. Try again."));
    }
    pendingRef.current = false;
    setPending(false);
    return succeeded;
  }

  async function openBrowser(url: string) {
    const opened = await ensureLocalApi()
      .shell.openExternal(url)
      .then(() => true)
      .catch(() => false);
    if (!opened) {
      setError(gt("Could not open the browser. Copy the link and open it yourself."));
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

  return {
    pending,
    error,
    signIn,
    openBrowser,
    signOut: () => run(() => logout({ environmentId, input: { serverId: id } })),
    cancel: (flowId: string) =>
      run(() => cancel({ environmentId, input: { serverId: id, flowId } })),
    complete: (flowId: string, callbackUrl: string) =>
      run(() => complete({ environmentId, input: { serverId: id, flowId, callbackUrl } })),
  };
}

export type ExternalMcpOAuthActions = ReturnType<typeof useExternalMcpOAuth>;

export function ExternalMcpOAuthStatusLine({
  status,
  error,
}: {
  readonly status: ExternalMcpOAuthStatus | undefined;
  readonly error: string | null;
}) {
  const gt = useGT();
  const host = issuerHost(status?.issuer ?? null);
  const [dot, label] =
    status === undefined
      ? ["bg-muted-foreground/40", gt("Checking sign-in…")]
      : status.flow !== null
        ? ["bg-info", gt("Waiting for sign-in")]
        : status?.state === "connected"
          ? ["bg-success", host ? gt("Signed in with {host}", { host }) : gt("Signed in")]
          : status?.state === "needs-login"
            ? ["bg-warning", gt("Sign-in expired. Agents don't get this server until you sign in.")]
            : ["bg-muted-foreground/40", gt("Not signed in. Agents don't get this server yet.")];
  const message = error ?? (status?.state === "connected" ? null : status?.message) ?? null;
  return (
    <div className="mt-1.5 space-y-1 text-xs">
      <p role="status" className="flex items-center gap-1.5 text-muted-foreground">
        <ConnectionStatusDot dotClassName={dot} />
        {label}
      </p>
      {message && message !== label ? (
        <p className="text-destructive-foreground [overflow-wrap:anywhere]">{message}</p>
      ) : null}
    </div>
  );
}

export function ExternalMcpSignInPanel({
  serverName,
  flow,
  actions,
}: {
  readonly serverName: string;
  readonly flow: NonNullable<ExternalMcpOAuthStatus["flow"]>;
  readonly actions: ExternalMcpOAuthActions;
}) {
  const [callbackUrl, setCallbackUrl] = useState("");
  // Without a listener on this machine, pasting is the only way back.
  const [pasteOpen, setPasteOpen] = useState(!flow.serverReceivesCallback);
  const gt = useGT();
  return (
    <div className="mt-3 space-y-3 rounded-lg border border-border/60 bg-muted/30 p-3 text-xs">
      <div className="flex flex-wrap items-center gap-2">
        <Spinner size="xs" />
        <span className="min-w-0 flex-1 text-muted-foreground">
          <T>
            Finish signing in to <Var>{serverName}</Var> in your browser.
          </T>
        </span>
        <Button
          type="button"
          size="xs"
          variant="outline"
          disabled={actions.pending}
          onClick={() => void actions.openBrowser(flow.authorizationUrl)}
        >
          <T>Open again</T>
        </Button>
        <Button
          type="button"
          size="icon-xs"
          variant="ghost"
          aria-label={gt("Copy sign-in link")}
          onClick={() =>
            void writeTextToClipboard(flow.authorizationUrl, {
              target: "MCP sign-in link",
              unavailableMessage: msg(
                "Clipboard API is unavailable while copying MCP sign-in link.",
              ),
              failureMessage: msg("Failed to copy MCP sign-in link to the clipboard."),
            })
          }
        >
          <CopyIcon className="size-3.5" />
        </Button>
        <Button
          type="button"
          size="xs"
          variant="ghost"
          disabled={actions.pending}
          onClick={() => void actions.cancel(flow.flowId)}
        >
          <T>Cancel</T>
        </Button>
      </div>
      {pasteOpen ? (
        <form
          className="space-y-2"
          onSubmit={(event) => {
            event.preventDefault();
            if (!callbackUrl.trim()) return;
            void actions.complete(flow.flowId, callbackUrl).then((sent) => {
              if (sent) setCallbackUrl("");
            });
          }}
        >
          <label className="block space-y-1.5 text-muted-foreground">
            <span>
              <T>
                After you approve, the browser opens a <Var>{"127.0.0.1"}</Var> page that doesn't
                load. Paste that page's full address here.
              </T>
            </span>
            <div className="flex gap-2">
              <Input
                size="sm"
                type="url"
                autoComplete="off"
                value={callbackUrl}
                disabled={actions.pending}
                maxLength={16_384}
                placeholder="http://127.0.0.1:…/callback?code=…"
                onChange={(event) => setCallbackUrl(event.target.value)}
                aria-label={gt("{name} redirect URL", { name: serverName })}
              />
              <Button
                type="submit"
                size="sm"
                variant="outline"
                disabled={actions.pending || !callbackUrl.trim()}
              >
                {actions.pending ? <Spinner size="xs" /> : null} <T>Continue</T>
              </Button>
            </div>
          </label>
        </form>
      ) : (
        <InlineButton tone="muted" onClick={() => setPasteOpen(true)}>
          <T>Browser didn't come back? Paste the redirect address</T>
        </InlineButton>
      )}
    </div>
  );
}
