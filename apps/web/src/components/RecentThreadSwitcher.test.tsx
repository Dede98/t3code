// @vitest-environment jsdom
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type ResolvedKeybindingsConfig,
  type ThreadSwitchKeyEvent,
} from "@t3tools/contracts";
import { DEFAULT_RESOLVED_KEYBINDINGS, parseKeybindingShortcut } from "@t3tools/shared/keybindings";
import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  params: {} as Record<string, string>,
  keybindings: [] as ResolvedKeybindingsConfig,
  navigate: vi.fn(),
  unavailable: new Set<string>(),
  paletteOpen: false,
}));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => state.keybindings }));
vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => state.navigate,
  useParams: () => state.params,
}));
vi.mock("../env", () => ({ isElectron: true }));
vi.mock("../state/server", () => ({ primaryServerKeybindingsAtom: {} }));
vi.mock("../connection/catalog", () => ({ environmentCatalog: { catalogValueAtom: {} } }));
vi.mock("../rpc/atomRegistry", () => ({
  appAtomRegistry: {
    get: () => ({
      entries: new Map([
        ["local", { enabled: true, profile: { _tag: "None" } }],
        ["remote", { enabled: true, profile: { _tag: "Some", value: { label: "Remote" } } }],
      ]),
    }),
  },
}));
vi.mock("../state/entities", () => ({
  useThreadShell: () => ({ title: "Current thread", deletedAt: null }),
  readThreadShell: (ref: { threadId: string }) =>
    state.unavailable.has(ref.threadId) ? null : { title: ref.threadId, projectId: "project" },
  readProject: () => ({ title: "Project" }),
}));
vi.mock("../commandPaletteBus", () => ({ isCommandPaletteOpen: () => state.paletteOpen }));
vi.mock("../modelPickerVisibility", () => ({ isModelPickerOpen: () => false }));
vi.mock("../lib/terminalFocus", () => ({ isTerminalFocused: () => true }));
vi.mock("../lib/previewFocus", () => ({ isPreviewFocused: () => false }));
vi.mock("./ui/toast", () => ({ toastManager: { add: vi.fn() } }));
vi.mock("./ui/dialog", () => ({
  Dialog: ({ open, children }: { open: boolean; children: ReactNode }) => (open ? children : null),
  DialogPopup: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
}));

import { useRecentThreadStore } from "../recentThreadStore";
import {
  DraftId,
  useComposerDraftStore,
  markPromotedDraftThreadByRef,
  finalizePromotedDraftThreadByRef,
} from "../composerDraftStore";
import { RecentThreadSwitcher } from "./RecentThreadSwitcher";

let root: Root;
let host: HTMLDivElement;
let forwarded: ((event: ThreadSwitchKeyEvent) => void) | undefined;
const ref = (id: string, environment = "local") =>
  scopeThreadRef(EnvironmentId.make(environment), ThreadId.make(id));
function key(type = "keydown", overrides: KeyboardEventInit = {}) {
  const event = new KeyboardEvent(type, {
    key: "Tab",
    code: "Tab",
    ctrlKey: true,
    bubbles: true,
    cancelable: true,
    ...overrides,
  });
  window.dispatchEvent(event);
  return event;
}
async function render() {
  await act(() => root.render(<RecentThreadSwitcher />));
}
async function release() {
  await act(() => {
    key("keyup", { key: "Control", ctrlKey: false });
  });
}

beforeEach(async () => {
  vi.clearAllMocks();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  HTMLElement.prototype.scrollIntoView = vi.fn();
  state.params = { environmentId: "local", threadId: "a" };
  state.keybindings = DEFAULT_RESOLVED_KEYBINDINGS;
  state.unavailable.clear();
  state.paletteOpen = false;
  useComposerDraftStore.setState({
    draftThreadsByThreadKey: {},
    logicalProjectDraftThreadKeyByLogicalProjectKey: {},
    draftsByThreadKey: {},
  });
  state.navigate.mockResolvedValue(undefined);
  Object.defineProperty(navigator, "platform", { value: "MacIntel", configurable: true });
  Object.defineProperty(window, "desktopBridge", {
    value: {
      onThreadSwitchKey: (listener: typeof forwarded) => {
        forwarded = listener;
        return () => {
          forwarded = undefined;
        };
      },
    },
    configurable: true,
  });
  useRecentThreadStore.setState({
    entries: ["a", "b", "c", "d"].map((id) => ({ threadRef: ref(id) })),
  });
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  await render();
});
afterEach(async () => {
  await act(() => root?.unmount());
  host?.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("recent thread keyboard navigation", () => {
  it("returns to an unsent draft and follows its background promotion to another environment", async () => {
    const draftId = DraftId.make("draft-recent");
    const store = useComposerDraftStore.getState();
    const localProject = scopeProjectRef(EnvironmentId.make("local"), ProjectId.make("project"));
    const remoteProject = scopeProjectRef(EnvironmentId.make("remote"), ProjectId.make("project"));
    store.setProjectDraftThreadId(localProject, draftId, {
      threadId: ThreadId.make("draft-thread"),
    });
    state.params = { draftId };
    await render();
    state.params = { environmentId: "local", threadId: "a" };
    await render();
    await act(() => {
      key();
    });
    await release();
    expect(state.navigate).toHaveBeenLastCalledWith({ to: "/draft/$draftId", params: { draftId } });
    state.navigate.mockClear();
    await act(() => {
      key();
    });
    store.setDraftThreadContext(draftId, { projectRef: remoteProject });
    const promoted = ref("draft-thread", "remote");
    markPromotedDraftThreadByRef(promoted);
    finalizePromotedDraftThreadByRef(promoted);
    await release();
    expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: promoted,
    });
    expect(useRecentThreadStore.getState().entries[0]?.threadRef).toEqual(ref("a"));
    expect(
      useRecentThreadStore
        .getState()
        .entries.filter((entry) => entry.threadRef.threadId === "draft-thread"),
    ).toHaveLength(1);
  });

  it("handles a quick tap before a render without navigating intermediate threads", async () => {
    await act(() => {
      expect(key().defaultPrevented).toBe(true);
      expect(state.navigate).not.toHaveBeenCalled();
      key("keyup", { key: "Control", ctrlKey: false });
    });
    expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: ref("b"),
    });
  });

  it("cycles three threads, reverses, and ignores OS auto-repeat", async () => {
    await act(() => {
      key();
      key();
      key();
      key("keydown", { repeat: true });
    });
    expect(state.navigate).not.toHaveBeenCalled();
    await act(() => {
      key("keydown", { shiftKey: true });
      key("keyup", { key: "Shift", shiftKey: false });
    });
    expect(state.navigate).not.toHaveBeenCalled();
    await release();
    expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: ref("c"),
    });
  });

  it.each(["escape", "blur", "hidden"])(
    "cancels on %s without changing history",
    async (reason) => {
      const before = useRecentThreadStore.getState().entries;
      await act(() => {
        key();
      });
      await act(() => {
        if (reason === "escape") key("keydown", { key: "Escape" });
        if (reason === "blur") window.dispatchEvent(new Event("blur"));
        if (reason === "hidden") {
          vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
          document.dispatchEvent(new Event("visibilitychange"));
        }
      });
      await release();
      expect(state.navigate).not.toHaveBeenCalled();
      expect(useRecentThreadStore.getState().entries).toEqual(before);
    },
  );

  it("skips missing threads and checks availability again on release", async () => {
    state.unavailable.add("b");
    await act(() => {
      key();
    });
    state.unavailable.add("c");
    await release();
    expect(state.navigate).not.toHaveBeenCalled();
    await act(() => {
      key();
    });
    await release();
    expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: ref("d"),
    });
  });

  it("tracks navigation from any route and keeps remote identity", async () => {
    state.params = { environmentId: "remote", threadId: "a" };
    await render();
    await act(() => {
      key();
    });
    await release();
    expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: ref("a"),
    });
    state.params = {};
    await render();
    await act(() => {
      key();
    });
    await release();
    expect(state.navigate).toHaveBeenLastCalledWith({
      to: "/$environmentId/$threadId",
      params: ref("a", "remote"),
    });
  });

  it("accepts a different hold modifier and a one-shot custom binding", async () => {
    state.keybindings = [
      { command: "thread.recentPrevious", shortcut: parseKeybindingShortcut("alt+q")! },
    ];
    await render();
    await act(() => {
      key("keydown", { key: "q", code: "KeyQ", altKey: true, ctrlKey: false });
    });
    await act(() => {
      key("keyup", { key: "Alt", altKey: false, ctrlKey: false });
    });
    expect(state.navigate).toHaveBeenCalledTimes(1);
    state.keybindings = [
      { command: "thread.recentPrevious", shortcut: parseKeybindingShortcut("f6")! },
    ];
    await render();
    await act(() => {
      key("keydown", { key: "F6", code: "F6", ctrlKey: false });
    });
    expect(state.navigate).toHaveBeenCalledTimes(2);
  });

  it("handles quick keydown and release forwarded from the desktop browser", async () => {
    const input: ThreadSwitchKeyEvent = {
      type: "keydown",
      key: "Tab",
      code: "Tab",
      ctrlKey: true,
      metaKey: false,
      shiftKey: false,
      altKey: false,
      repeat: false,
    };
    await act(() => {
      forwarded?.(input);
      forwarded?.({ ...input, type: "keyup", key: "Control", ctrlKey: false });
    });
    expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: ref("b"),
    });
  });

  it("respects the palette and shortcut recorder but takes Tab before composer handlers", async () => {
    state.paletteOpen = true;
    expect(key().defaultPrevented).toBe(false);
    state.paletteOpen = false;
    const editor = document.createElement("textarea");
    document.body.append(editor);
    editor.setAttribute("data-keybinding-capture", "");
    const press = () =>
      editor.dispatchEvent(
        new KeyboardEvent("keydown", {
          key: "Tab",
          ctrlKey: true,
          bubbles: true,
          cancelable: true,
        }),
      );
    expect(press()).toBe(true);
    editor.removeAttribute("data-keybinding-capture");
    const handleTab = vi.fn();
    editor.addEventListener("keydown", handleTab);
    await act(() => {
      press();
    });
    await release();
    expect(handleTab).not.toHaveBeenCalled();
    expect(state.navigate).toHaveBeenCalledTimes(1);
    editor.remove();
  });
});
