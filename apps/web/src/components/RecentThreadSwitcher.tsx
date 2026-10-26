import { useAtomValue } from "@effect/atom-react";
import {
  scopeProjectRef,
  scopedThreadKey,
  scopeThreadRef,
} from "@t3tools/client-runtime/environment";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useCallback, useEffect, useEffectEvent, useRef, useState } from "react";

import { isCommandPaletteOpen } from "../commandPaletteBus";
import { useComposerDraftStore } from "../composerDraftStore";
import { environmentCatalog } from "../connection/catalog";
import { resolveShortcutCommand } from "../keybindings";
import { isEditableFocused } from "../lib/editableFocus";
import { isPreviewFocused } from "../lib/previewFocus";
import { isTerminalFocused } from "../lib/terminalFocus";
import { isModelPickerOpen } from "../modelPickerVisibility";
import {
  beginThreadSwitch,
  stepThreadSwitch,
  threadSwitchModifiers,
  useRecentThreadStore,
  type RecentThread,
  type ThreadSwitchSession,
} from "../recentThreadStore";
import { selectActiveRightPanel, useRightPanelStore } from "../rightPanelStore";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { readProject, readThreadShell, useThreadShell } from "../state/entities";
import { primaryServerKeybindingsAtom } from "../state/server";
import { selectThreadTerminalUiState, useTerminalUiStateStore } from "../terminalUiStateStore";
import {
  buildDraftThreadRouteParams,
  buildThreadRouteParams,
  resolveThreadRouteTarget,
} from "../threadRoutes";
import { Dialog, DialogHeader, DialogPopup, DialogTitle } from "./ui/dialog";
import { toastManager } from "./ui/toast";

function resolveRecentThread(entry: RecentThread) {
  const draft = entry.draftId
    ? useComposerDraftStore.getState().getDraftSession(entry.draftId)
    : null;
  const ref = draft
    ? (draft.promotedTo ?? scopeThreadRef(draft.environmentId, draft.threadId))
    : ((entry.draftId
        ? useRecentThreadStore.getState().entries.find((recent) => recent.draftId === entry.draftId)
            ?.threadRef
        : null) ?? entry.threadRef);
  const catalog = appAtomRegistry.get(environmentCatalog.catalogValueAtom);
  const environment = catalog.entries.get(ref.environmentId);
  if (!environment?.enabled) return null;
  const thread = readThreadShell(ref);
  if (thread?.deletedAt || thread?.archivedAt || (!thread && !draft)) return null;
  const projectId = thread?.projectId ?? draft?.projectId;
  const project = projectId ? readProject(scopeProjectRef(ref.environmentId, projectId)) : null;
  return {
    ref,
    draftId: draft && !draft.promotedTo ? entry.draftId : undefined,
    title: thread?.title || "New thread",
    project: project?.title ?? "No project",
    environment: environment.profile._tag === "Some" ? environment.profile.value.label : "Local",
  };
}

export function RecentThreadSwitcher() {
  const navigate = useNavigate();
  const params = useParams({ strict: false });
  const route = resolveThreadRouteTarget(params);
  const draftId = route?.kind === "draft" ? route.draftId : null;
  const draft = useComposerDraftStore((state) => (draftId ? state.getDraftSession(draftId) : null));
  const currentRef =
    route?.kind === "server"
      ? route.threadRef
      : draft
        ? (draft.promotedTo ?? scopeThreadRef(draft.environmentId, draft.threadId))
        : null;
  const environmentId = currentRef?.environmentId;
  const threadId = currentRef?.threadId;
  const currentThread = useThreadShell(currentRef);
  const keybindings = useAtomValue(primaryServerKeybindingsAtom);
  const [session, setSession] = useState<ThreadSwitchSession | null>(null);
  // Key repeats and quick modifier releases can arrive before React renders.
  const sessionRef = useRef<ThreadSwitchSession | null>(null);
  const listRef = useRef<HTMLDivElement>(null);

  const updateSession = useCallback((next: ThreadSwitchSession | null) => {
    sessionRef.current = next;
    setSession(next);
  }, []);

  const rememberRoute = useEffectEvent((entry: RecentThread | null) => {
    updateSession(null);
    if (entry) useRecentThreadStore.getState().remember(entry);
  });
  const routeReady = draft !== null || (currentThread !== null && !currentThread.deletedAt);
  // oxlint-disable-next-line t3code/react-effect-budget -- A committed route is only known after navigation resolves.
  useEffect(() => {
    // oxlint-disable-next-line react/set-state-in-effect -- A committed router navigation cancels the in-progress keyboard selection.
    rememberRoute(
      routeReady && environmentId && threadId
        ? {
            threadRef: scopeThreadRef(environmentId, threadId),
            ...(draftId ? { draftId } : {}),
          }
        : null,
    );
  }, [environmentId, threadId, draftId, routeReady]);

  // oxlint-disable-next-line t3code/react-effect-budget -- Subscribes to the external draft store.
  useEffect(
    () =>
      useComposerDraftStore.subscribe((state, previous) => {
        if (state.draftThreadsByThreadKey === previous.draftThreadsByThreadKey) return;
        // Follow a visited draft through host changes and background promotion,
        // without counting the background submission as a new visit.
        for (const entry of useRecentThreadStore.getState().entries) {
          if (!entry.draftId) continue;
          const value =
            state.draftThreadsByThreadKey[entry.draftId] ??
            previous.draftThreadsByThreadKey[entry.draftId];
          if (!value) continue;
          useRecentThreadStore
            .getState()
            .updateDraft(
              entry.draftId,
              value.promotedTo ?? scopeThreadRef(value.environmentId, value.threadId),
            );
        }
      }),
    [],
  );

  const commit = useEffectEvent(
    (selected = sessionRef.current?.entries[sessionRef.current.index]) => {
      updateSession(null);
      if (!selected) return;
      // Resolve again: a thread can disappear or a draft can promote while held.
      const target = resolveRecentThread(selected);
      if (!target) return;
      const navigation = target.draftId
        ? navigate({ to: "/draft/$draftId", params: buildDraftThreadRouteParams(target.draftId) })
        : navigate({ to: "/$environmentId/$threadId", params: buildThreadRouteParams(target.ref) });
      void navigation.catch(() => {
        toastManager.add({ type: "error", title: "Could not open the selected thread" });
      });
    },
  );

  const onKeyDown = useEffectEvent((event: KeyboardEvent) => {
    if (event.isComposing || event.defaultPrevented) return;
    const active = sessionRef.current;
    if (active && event.key === "Escape") {
      event.preventDefault();
      event.stopImmediatePropagation();
      updateSession(null);
      return;
    }
    if (active && event.key === "Enter") {
      event.preventDefault();
      event.stopImmediatePropagation();
      commit();
      return;
    }
    if (
      !active &&
      (isCommandPaletteOpen() ||
        isModelPickerOpen() ||
        document.querySelector('[role="dialog"][data-open], [role="alertdialog"]') ||
        (event.target instanceof HTMLElement && event.target.closest("[data-keybinding-capture]")))
    )
      return;
    const terminalOpen = selectThreadTerminalUiState(
      useTerminalUiStateStore.getState().terminalUiStateByThreadKey,
      currentRef,
    ).terminalOpen;
    const previewOpen =
      selectActiveRightPanel(useRightPanelStore.getState().byThreadKey, currentRef) === "preview";
    const command = resolveShortcutCommand(event, keybindings, {
      context: {
        terminalFocus: isTerminalFocused(),
        terminalOpen,
        previewFocus: isPreviewFocused(),
        previewOpen,
        editableFocus: isEditableFocused(event.target),
        modelPickerOpen: isModelPickerOpen(),
      },
    });
    const direction =
      command === "thread.recentPrevious" ? 1 : command === "thread.recentNext" ? -1 : null;
    if (direction === null) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    if (event.repeat) return;
    let next = active
      ? stepThreadSwitch(active, direction)
      : beginThreadSwitch(
          useRecentThreadStore
            .getState()
            .entries.filter((entry) => resolveRecentThread(entry) !== null),
          currentRef,
          direction,
          threadSwitchModifiers(event),
        );
    if (next) {
      let attempts = next.entries.length;
      while (attempts > 0 && !resolveRecentThread(next.entries[next.index]!)) {
        next = stepThreadSwitch(next, direction);
        attempts -= 1;
      }
      if (attempts === 0) next = null;
    }
    updateSession(next);
    // Unmodified custom bindings act as one-shot switches.
    if (next?.modifiers.length === 0) commit();
  });

  const onKeyUp = useEffectEvent((event: KeyboardEvent) => {
    const active = sessionRef.current;
    if (!active) return;
    const released = { ctrlKey: "Control", metaKey: "Meta", altKey: "Alt", shiftKey: "Shift" };
    if (active.modifiers.some((key) => event.key === released[key] || !event[key])) {
      event.preventDefault();
      event.stopImmediatePropagation();
      commit();
    }
  });

  // oxlint-disable-next-line t3code/react-effect-budget -- Window and desktop key listeners need capture-phase subscription.
  useEffect(() => {
    const keydown = (event: KeyboardEvent) => onKeyDown(event);
    const keyup = (event: KeyboardEvent) => onKeyUp(event);
    const cancel = () => updateSession(null);
    const visibility = () => {
      if (document.visibilityState === "hidden") cancel();
    };
    window.addEventListener("keydown", keydown, true);
    window.addEventListener("keyup", keyup, true);
    window.addEventListener("blur", cancel);
    document.addEventListener("visibilitychange", visibility);
    const unsubscribe = window.desktopBridge?.onThreadSwitchKey?.((input) => {
      const event = new KeyboardEvent(input.type, { ...input, cancelable: true });
      if (input.type === "keydown") onKeyDown(event);
      else onKeyUp(event);
    });
    return () => {
      window.removeEventListener("keydown", keydown, true);
      window.removeEventListener("keyup", keyup, true);
      window.removeEventListener("blur", cancel);
      document.removeEventListener("visibilitychange", visibility);
      unsubscribe?.();
    };
  }, [updateSession]);

  const selectedIndex = session?.index;
  // oxlint-disable-next-line t3code/react-effect-budget -- The selected row exists in the DOM only after the dialog renders it.
  useEffect(() => {
    listRef.current
      ?.querySelector(`#recent-thread-${selectedIndex}`)
      ?.scrollIntoView({ block: "nearest" });
  }, [selectedIndex]);

  return (
    <Dialog
      open={session !== null}
      onOpenChange={(open) => {
        if (!open) updateSession(null);
      }}
    >
      <DialogPopup showCloseButton={false} bottomStickOnMobile={false} initialFocus={listRef}>
        <DialogHeader>
          <DialogTitle>Recent threads</DialogTitle>
        </DialogHeader>
        <div
          ref={listRef}
          role="listbox"
          aria-label="Recent threads"
          aria-activedescendant={session ? `recent-thread-${session.index}` : undefined}
          tabIndex={0}
          className="max-h-80 overflow-y-auto px-2 pb-2 outline-none"
        >
          {session?.entries.map((entry, index) => {
            const target = resolveRecentThread(entry);
            return (
              <div
                key={scopedThreadKey(entry.threadRef)}
                id={`recent-thread-${index}`}
                role="option"
                aria-selected={session.index === index}
                aria-disabled={!target}
                className="cursor-default rounded-lg px-3 py-2 aria-selected:bg-accent aria-selected:text-accent-foreground aria-disabled:opacity-50"
                onClick={() => commit(entry)}
              >
                <div className="truncate text-sm font-medium">
                  {target?.title ?? "Thread unavailable"}
                </div>
                <div className="truncate text-xs text-muted-foreground">
                  {target ? `${target.project} · ${target.environment}` : ""}
                </div>
              </div>
            );
          })}
        </div>
        <p className="px-5 pb-4 text-xs text-muted-foreground">
          Release the shortcut modifier to switch. Escape cancels.
        </p>
      </DialogPopup>
    </Dialog>
  );
}
