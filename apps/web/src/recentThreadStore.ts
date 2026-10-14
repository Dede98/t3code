import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { create } from "zustand";

import type { DraftId } from "./composerDraftStore";

export interface RecentThread {
  threadRef: ScopedThreadRef;
  draftId?: DraftId;
}

const sameThread = (left: RecentThread, right: RecentThread) =>
  (left.draftId !== undefined && left.draftId === right.draftId) ||
  scopedThreadKey(left.threadRef) === scopedThreadKey(right.threadRef);

interface RecentThreadState {
  entries: readonly RecentThread[];
  remember: (entry: RecentThread) => void;
  updateDraft: (draftId: DraftId, threadRef: ScopedThreadRef) => void;
}

// Window-local navigation history. Read watermarks and activity on other clients
// must never reorder it. A switch only records a visit once its route commits.
export const useRecentThreadStore = create<RecentThreadState>((set) => ({
  entries: [],
  remember: (entry) =>
    set((state) => ({
      entries: [entry, ...state.entries.filter((other) => !sameThread(entry, other))].slice(0, 50),
    })),
  updateDraft: (draftId, threadRef) =>
    set((state) => {
      const existing = state.entries.find((entry) => entry.draftId === draftId);
      if (!existing || scopedThreadKey(existing.threadRef) === scopedThreadKey(threadRef)) {
        return state;
      }
      const seen = new Set<string>();
      return {
        entries: state.entries
          .map((entry) =>
            entry === existing || scopedThreadKey(entry.threadRef) === scopedThreadKey(threadRef)
              ? { ...entry, draftId, threadRef }
              : entry,
          )
          .filter((entry) => {
            const key = scopedThreadKey(entry.threadRef);
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          }),
      };
    }),
}));

export type ThreadSwitchDirection = 1 | -1;
export type ThreadSwitchModifier = "ctrlKey" | "metaKey" | "altKey" | "shiftKey";

export interface ThreadSwitchSession {
  entries: readonly RecentThread[];
  index: number;
  modifiers: readonly ThreadSwitchModifier[];
}

/** Shift reverses traversal; releasing it must not commit a Ctrl/Alt/Meta cycle. */
export function threadSwitchModifiers(event: Record<ThreadSwitchModifier, boolean>) {
  const modifiers = (["ctrlKey", "metaKey", "altKey"] as const).filter((key) => event[key]);
  return modifiers.length > 0 ? modifiers : event.shiftKey ? (["shiftKey"] as const) : [];
}

export function beginThreadSwitch(
  entries: readonly RecentThread[],
  current: ScopedThreadRef | null,
  direction: ThreadSwitchDirection,
  modifiers: readonly ThreadSwitchModifier[],
): ThreadSwitchSession | null {
  if (entries.length === 0) return null;
  const currentIndex = current
    ? entries.findIndex((entry) => scopedThreadKey(entry.threadRef) === scopedThreadKey(current))
    : -1;
  if (entries.length === 1 && currentIndex === 0) return null;
  const index =
    currentIndex === -1
      ? direction === 1
        ? 0
        : entries.length - 1
      : (currentIndex + direction + entries.length) % entries.length;
  return { entries: [...entries], index, modifiers };
}

export function stepThreadSwitch(
  session: ThreadSwitchSession,
  direction: ThreadSwitchDirection,
): ThreadSwitchSession {
  return {
    ...session,
    index: (session.index + direction + session.entries.length) % session.entries.length,
  };
}
