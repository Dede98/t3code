import { presentThreadShell } from "@t3tools/client-runtime/state/models";
import { useAtomValue } from "@effect/atom-react";
import { useNavigate, useParams } from "@tanstack/react-router";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { resolveSidebarThreadStatus } from "./Sidebar.logic";
import * as Option from "effect/Option";
import {
  CircleAlertIcon,
  CircleCheckIcon,
  MessageCircleQuestionIcon,
  ShieldQuestionIcon,
} from "lucide-react";
import { useCallback, useEffect, useRef } from "react";

import { getClientSettings, useClientSettings } from "../hooks/useSettings";
import { useEnvironmentIds } from "../state/environments";
import { environmentShell } from "../state/shell";
import {
  hasDesktopNotifications,
  hasNotificationSound,
  playNotificationSound,
  setNotificationBadge,
  unlockNotificationAudio,
} from "../threadNotifications";
import { setActiveEnvironmentId } from "../state/entities";
import { toastManager } from "./ui/toast";

export function ThreadNotificationCoordinator() {
  const environmentIds = useEnvironmentIds();
  const navigate = useNavigate();
  const activeEnvironmentIds = useRef(new Set<EnvironmentId>());
  const mode = useClientSettings((settings) => settings.notificationMode);
  const inAppNotificationsEnabled = useClientSettings(
    (settings) => settings.inAppNotificationsEnabled,
  );
  const pending = useRef(new Map<string, { environmentId: EnvironmentId; close?: () => void }>());
  const onNotification = useCallback(
    (environmentId: EnvironmentId, tag: string, close?: () => void) => {
      if (!activeEnvironmentIds.current.has(environmentId)) {
        close?.();
        return;
      }
      pending.current.get(tag)?.close?.();
      pending.current.set(tag, { environmentId, ...(close ? { close } : {}) });
      setNotificationBadge(pending.current.size);
    },
    [],
  );

  useEffect(
    () =>
      window.desktopBridge?.onDesktopNotificationClick((target) => {
        setActiveEnvironmentId(target.environmentId);
        void navigate({ to: "/$environmentId/$threadId", params: target });
      }),
    [navigate],
  );

  useEffect(() => {
    const activeIds = new Set(environmentIds);
    activeEnvironmentIds.current = activeIds;
    const count = pending.current.size;
    for (const [tag, { environmentId, close }] of pending.current) {
      if (activeIds.has(environmentId)) continue;
      close?.();
      pending.current.delete(tag);
    }
    if (count !== pending.current.size) setNotificationBadge(pending.current.size);
  }, [environmentIds]);

  useEffect(() => {
    const clear = () => {
      for (const { close } of pending.current.values()) close?.();
      pending.current.clear();
      setNotificationBadge(0);
    };
    clear();
    if (!hasDesktopNotifications(mode)) return;
    const unsubscribe = window.desktopBridge?.onNotificationBadgeClear?.(clear);
    window.addEventListener("focus", clear);
    return () => {
      unsubscribe?.();
      window.removeEventListener("focus", clear);
      clear();
    };
  }, [mode]);

  useEffect(() => {
    if (!hasNotificationSound(mode)) return;
    document.addEventListener("pointerdown", unlockNotificationAudio);
    document.addEventListener("keydown", unlockNotificationAudio);
    return () => {
      document.removeEventListener("pointerdown", unlockNotificationAudio);
      document.removeEventListener("keydown", unlockNotificationAudio);
    };
  }, [mode]);

  if (mode === "off" && !inAppNotificationsEnabled) return null;

  return environmentIds.map((environmentId) => (
    <EnvironmentNotifications
      key={environmentId}
      environmentId={environmentId}
      onNotification={onNotification}
    />
  ));
}

function EnvironmentNotifications({
  environmentId,
  onNotification,
}: {
  environmentId: EnvironmentId;
  onNotification: (environmentId: EnvironmentId, tag: string, close?: () => void) => void;
}) {
  const shell = useAtomValue(environmentShell.stateValueAtom(environmentId));
  const mode = useClientSettings((settings) => settings.notificationMode);
  const inAppNotificationsEnabled = useClientSettings(
    (settings) => settings.inAppNotificationsEnabled,
  );
  const navigate = useNavigate();
  const { environmentId: activeEnvironmentId, threadId: activeThreadId } = useParams({
    strict: false,
  });
  const previous = useRef(
    new Map<ThreadId, { attention: string | null; completion: number | null }>(),
  );

  useEffect(() => {
    if (shell.status !== "live" || Option.isNone(shell.snapshot)) {
      previous.current.clear();
      return;
    }
    const next = new Map<ThreadId, { attention: string | null; completion: number | null }>();
    for (const rawThread of shell.snapshot.value.threads) {
      if (rawThread.lineage.relationshipToParent === "subagent") continue;
      const thread = presentThreadShell(environmentId, rawThread);
      let status = resolveSidebarThreadStatus(thread);
      if (status === "ready" && thread.latestRun?.status === "failed") status = "failed";
      const prior = previous.current.get(thread.id);
      const attention =
        status === "input" || status === "approval" || status === "failed" || status === "limited"
          ? `${thread.latestRun?.runId ?? ""}:${status}`
          : null;
      const completedAt = Date.parse(thread.latestRun?.completedAt ?? "");
      // Commands left running (a dev server) read as ready; subagents and monitors wait.
      const completion =
        status === "ready" &&
        thread.latestRun?.status === "completed" &&
        Number.isFinite(completedAt)
          ? completedAt
          : (prior?.completion ?? null);
      next.set(thread.id, { attention, completion });
      if (!prior || thread.archivedAt !== null) continue;
      const notificationKind =
        attention && attention !== prior.attention
          ? status === "approval"
            ? "approval"
            : status === "failed"
              ? "failure"
              : "input"
          : completion !== null && completion !== prior.completion
            ? "completion"
            : null;
      const kind =
        notificationKind === null
          ? null
          : notificationKind === "completion"
            ? "completion"
            : "input";
      if (!kind) continue;
      const title =
        kind === "completion"
          ? "Thread completed"
          : notificationKind === "approval"
            ? "Approval needed"
            : status === "limited"
              ? "Usage limit reached"
              : status === "failed"
                ? "Thread failed"
                : "Input needed";
      if (hasNotificationSound(mode)) {
        void playNotificationSound(kind, () =>
          hasNotificationSound(getClientSettings().notificationMode),
        );
      }
      if (
        inAppNotificationsEnabled &&
        document.visibilityState === "visible" &&
        document.hasFocus() &&
        (activeEnvironmentId !== environmentId || activeThreadId !== thread.id)
      ) {
        const toastId = toastManager.add({
          type:
            kind === "completion"
              ? "success"
              : notificationKind === "failure"
                ? "error"
                : "warning",
          title,
          description: thread.title,
          data: {
            hideCopyButton: true,
            leadingIcon:
              kind === "completion" ? (
                <CircleCheckIcon aria-hidden className="size-4 text-success-foreground" />
              ) : notificationKind === "approval" ? (
                <ShieldQuestionIcon aria-hidden className="size-4 text-warning-foreground" />
              ) : notificationKind === "failure" ? (
                <CircleAlertIcon aria-hidden className="size-4 text-destructive-foreground" />
              ) : (
                <MessageCircleQuestionIcon aria-hidden className="size-4 text-info-foreground" />
              ),
          },
          actionProps: {
            children: "Open thread",
            onClick: () => {
              toastManager.close(toastId);
              void navigate({
                to: "/$environmentId/$threadId",
                params: { environmentId, threadId: thread.id },
              });
            },
          },
        });
        continue;
      }
      if (!hasDesktopNotifications(mode)) continue;
      const isFocused = document.visibilityState === "visible" && document.hasFocus();
      if (window.desktopBridge) {
        if (isFocused && activeEnvironmentId === environmentId && activeThreadId === thread.id)
          continue;
        const bridge = window.desktopBridge;
        void bridge
          .showDesktopNotification({
            environmentId,
            threadId: thread.id,
            kind: notificationKind!,
            projectTitle:
              shell.snapshot.value.projects.find((project) => project.id === rawThread.projectId)
                ?.title ?? "Project",
            threadTitle: thread.title,
          })
          .then((shown) => {
            if (
              shown &&
              !document.hasFocus() &&
              hasDesktopNotifications(getClientSettings().notificationMode)
            ) {
              onNotification(environmentId, `${environmentId}:${thread.id}`);
            }
          })
          .catch(() => undefined);
        continue;
      }
      if (isFocused || typeof Notification === "undefined" || Notification.permission !== "granted")
        continue;
      try {
        const notification = new Notification(title, {
          body: thread.title,
          tag: `${environmentId}:${thread.id}`,
          silent: true,
        });
        onNotification(environmentId, notification.tag, () => notification.close());
        notification.addEventListener("click", () => {
          notification.close();
          window.focus();
          void navigate({
            to: "/$environmentId/$threadId",
            params: { environmentId, threadId: thread.id },
          });
        });
      } catch {
        // Some browsers expose Notification but reject desktop presentation.
      }
    }
    previous.current = next;
  }, [
    activeEnvironmentId,
    activeThreadId,
    environmentId,
    inAppNotificationsEnabled,
    mode,
    navigate,
    onNotification,
    shell,
  ]);

  return null;
}
