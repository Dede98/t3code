import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import { beforeEach, describe, expect, it } from "vite-plus/test";

import { DraftId } from "./composerDraftStore";
import { resolveShortcutCommand } from "./keybindings";
import {
  beginThreadSwitch,
  stepThreadSwitch,
  threadSwitchModifiers,
  useRecentThreadStore,
} from "./recentThreadStore";

const ref = (id: string, environment = "local") =>
  scopeThreadRef(EnvironmentId.make(environment), ThreadId.make(id));
const store = () => useRecentThreadStore.getState();
beforeEach(() => useRecentThreadStore.setState({ entries: [] }));

describe("recent thread history", () => {
  it("uses physical Control on macOS desktop and an Option chord in the web client", () => {
    const event = {
      key: "Tab",
      code: "Tab",
      ctrlKey: true,
      metaKey: false,
      shiftKey: false,
      altKey: false,
    };
    const desktop = { platform: "MacIntel", context: { isDesktop: true, isWeb: false } };
    const web = { platform: "MacIntel", context: { isDesktop: false, isWeb: true } };
    expect(resolveShortcutCommand(event, DEFAULT_RESOLVED_KEYBINDINGS, desktop)).toBe(
      "thread.recentPrevious",
    );
    expect(
      resolveShortcutCommand({ ...event, shiftKey: true }, DEFAULT_RESOLVED_KEYBINDINGS, desktop),
    ).toBe("thread.recentNext");
    expect(
      resolveShortcutCommand(
        { ...event, metaKey: true, ctrlKey: false },
        DEFAULT_RESOLVED_KEYBINDINGS,
        desktop,
      ),
    ).toBeNull();
    expect(resolveShortcutCommand(event, DEFAULT_RESOLVED_KEYBINDINGS, web)).toBeNull();
    expect(
      resolveShortcutCommand(
        { ...event, key: "œ", code: "KeyQ", ctrlKey: false, altKey: true },
        DEFAULT_RESOLVED_KEYBINDINGS,
        web,
      ),
    ).toBe("thread.recentPrevious");
  });

  it("orders actual visits, deduplicates, and keeps environments distinct", () => {
    for (const threadRef of [ref("a"), ref("b"), ref("a", "remote"), ref("a")]) {
      store().remember({ threadRef });
    }
    expect(store().entries.map((entry) => entry.threadRef)).toEqual([
      ref("a"),
      ref("a", "remote"),
      ref("b"),
    ]);
  });

  it("keeps the cycle fixed and toggles back to the starting thread after committing", () => {
    for (const id of ["d", "c", "b", "a"]) store().remember({ threadRef: ref(id) });
    const session = beginThreadSwitch(store().entries, ref("a"), 1, ["ctrlKey"])!;
    expect(session.entries[session.index]?.threadRef).toEqual(ref("b"));
    const third = stepThreadSwitch(stepThreadSwitch(session, 1), 1);
    expect(third.entries[third.index]?.threadRef).toEqual(ref("d"));
    expect(store().entries[0]?.threadRef).toEqual(ref("a"));
    store().remember(third.entries[third.index]!);
    const next = beginThreadSwitch(store().entries, ref("d"), 1, ["ctrlKey"])!;
    expect(next.entries[next.index]?.threadRef).toEqual(ref("a"));
  });

  it("wraps in both directions and starts with the latest visit outside a thread", () => {
    for (const id of ["c", "b", "a"]) store().remember({ threadRef: ref(id) });
    const session = beginThreadSwitch(store().entries, ref("a"), -1, ["ctrlKey"])!;
    expect(session.index).toBe(2);
    expect(stepThreadSwitch(session, 1).index).toBe(0);
    expect(beginThreadSwitch(store().entries, null, 1, [])?.index).toBe(0);
    expect(beginThreadSwitch([], null, 1, [])).toBeNull();
    expect(beginThreadSwitch([{ threadRef: ref("a") }], ref("a"), 1, [])).toBeNull();
  });

  it("updates a background draft's host without creating a visit or duplicate", () => {
    const draftId = DraftId.make("draft-a");
    store().remember({ draftId, threadRef: ref("a") });
    store().remember({ threadRef: ref("b") });
    store().updateDraft(draftId, ref("a", "remote"));
    expect(store().entries).toEqual([
      { threadRef: ref("b") },
      { draftId, threadRef: ref("a", "remote") },
    ]);
    store().remember({ threadRef: ref("a", "remote") });
    expect(store().entries).toHaveLength(2);
  });

  it("bounds history and excludes Shift from the release modifiers when reversing", () => {
    for (let i = 0; i < 70; i++) store().remember({ threadRef: ref(String(i)) });
    expect(store().entries).toHaveLength(50);
    expect(
      threadSwitchModifiers({ ctrlKey: true, metaKey: false, altKey: false, shiftKey: true }),
    ).toEqual(["ctrlKey"]);
  });
});
