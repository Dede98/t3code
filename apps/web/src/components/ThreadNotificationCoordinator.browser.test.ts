import { WORKING_EXIT_GRACE_MS } from "@t3tools/client-runtime/state/thread-inbox";
import type { AnyRouter } from "@tanstack/react-router";
import type { ClientSettings } from "@t3tools/contracts/settings";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { Atom, AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";

import { fakeTimers, stubGlobal } from "../../test/render";

const state = vi.hoisted(() => ({
  mode: "notifications" as ClientSettings["notificationMode"],
  navigate: vi.fn(),
  settingsListeners: new Set<() => void>(),
  notification: vi.fn(function (_title: string, options: NotificationOptions) {
    return Object.assign(new EventTarget(), { tag: options.tag, close: vi.fn() });
  }),
}));
vi.mock(import("../state/shell"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    environmentShell: {
      ...actual.environmentShell,
      stateValueAtom: ((id: string) => shellAtom(id)) as never,
    },
  };
});
vi.mock(import("../state/presentation"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    environmentSummaries: {
      ...actual.environmentSummaries,
      // Read lazily: the factory runs before this module's own bindings exist.
      get environmentIdsAtom() {
        return environmentIdsAtom as never;
      },
    },
  };
});
vi.mock(import("../hooks/useSettings"), async (original) => ({
  ...(await original()),
  subscribeClientSettings: ((listener: () => void) => {
    state.settingsListeners.add(listener);
    return () => state.settingsListeners.delete(listener);
  }) as never,
  getClientSettings: (() => ({
    notificationMode: state.mode,
    inAppNotificationsEnabled: false,
  })) as never,
}));
vi.mock(import("../threadNotifications"), async (original) => ({
  ...(await original()),
  playNotificationSound: vi.fn(),
  unlockNotificationAudio: vi.fn(),
  setNotificationBadge: vi.fn(),
}));

import { startThreadNotifications } from "./ThreadNotificationCoordinator";

let registry: AtomRegistry.AtomRegistry;
const shellAtom = Atom.family((_id: string) => Atom.make<unknown>(null));
const environmentIdsAtom = Atom.make<ReadonlyArray<string>>([]);

const SHELL_NOW = DateTime.makeUnsafe("2026-09-13T09:00:00.000Z");

function shell(completedAt: string | null) {
  return {
    status: "live",
    snapshot: Option.some({
      projects: [{ id: "project-1", title: "Login app" }],
      threads: [
        {
          id: "thread-1",
          projectId: "project-1",
          title: "Fix the login form",
          providerInstanceId: "codex",
          modelSelection: { instanceId: "codex", model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { rootThreadId: "thread-1", parentThreadId: null, relationshipToParent: null },
          forkedFrom: null,
          createdBy: "user",
          creationSource: "web",
          latestRunId: "run-1",
          activeRunId: null,
          status: completedAt ? "completed" : "running",
          pendingRuntimeRequest: null,
          latestVisibleMessage: null,
          latestUserMessageAt: null,
          hasActionableProposedPlan: false,
          itemCount: 0,
          visibleItemCount: 0,
          createdAt: SHELL_NOW,
          updatedAt: SHELL_NOW,
          latestRunRequestedAt: SHELL_NOW,
          latestRunStartedAt: SHELL_NOW,
          latestRunCompletedAt: completedAt ? DateTime.makeUnsafe(completedAt) : undefined,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      ],
    }),
  };
}

// The mocked modules read `state`, so every test starts by resetting it. Native desktop
// alerts must replace the web Notification API, which stays available to prove it is unused.
function setup(
  bridge: Partial<NonNullable<typeof window.desktopBridge>>,
  activeThreadId = "other-thread",
) {
  vi.clearAllMocks();
  const stack = new AsyncDisposableStack();
  registry = AtomRegistry.make();
  const testRegistry = registry;
  stack.defer(() => testRegistry.dispose());
  state.mode = "notifications";
  state.settingsListeners.clear();
  registry.set(environmentIdsAtom, ["env-1"]);
  registry.set(shellAtom("env-1"), shell(null));
  stack.use(vi.spyOn(document, "hasFocus").mockReturnValue(true));
  stack.use(vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible"));
  stack.use(
    stubGlobal("Notification", Object.assign(state.notification, { permission: "granted" })),
  );
  window.desktopBridge = bridge as NonNullable<typeof window.desktopBridge>;
  stack.defer(() => {
    delete window.desktopBridge;
  });
  const router = {
    state: { matches: [{ params: { environmentId: "env-1", threadId: activeThreadId } }] },
    navigate: state.navigate,
  } as unknown as AnyRouter;
  return {
    start: () => {
      stack.defer(startThreadNotifications(testRegistry, router));
    },
    async complete(completedAt: string) {
      testRegistry.set(shellAtom("env-1"), shell(completedAt));
      await vi.advanceTimersByTimeAsync(WORKING_EXIT_GRACE_MS);
    },
    changeSettings: () => {
      for (const listener of state.settingsListeners) listener();
    },
    [Symbol.asyncDispose]: () => stack.disposeAsync(),
  };
}

it("uses one native desktop alert and routes its click to the owning environment", async () => {
  const show = vi.fn().mockResolvedValue(true);
  let clicked: ((target: { environmentId: string; threadId: string }) => void) | undefined;
  await using app = setup({
    showDesktopNotification: show,
    onDesktopNotificationClick: (listener) => {
      clicked = listener as typeof clicked;
      return () => {};
    },
  });
  app.start();
  using _timers = fakeTimers();
  await app.complete("2026-09-13T10:00:00.000Z");
  expect(show).toHaveBeenCalledExactlyOnceWith({
    environmentId: "env-1",
    threadId: "thread-1",
    kind: "completion",
    projectTitle: "Login app",
    threadTitle: "Fix the login form",
  });
  expect(state.notification).not.toHaveBeenCalled();

  clicked!({ environmentId: "env-1", threadId: "thread-1" });
  expect(state.navigate).toHaveBeenCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "env-1", threadId: "thread-1" },
  });

  state.mode = "off";
  app.changeSettings();
  await app.complete("2026-09-13T10:05:00.000Z");
  expect(show).toHaveBeenCalledOnce();
});

it("does not notify the visible active desktop thread", async () => {
  const show = vi.fn().mockResolvedValue(true);
  await using app = setup(
    { showDesktopNotification: show, onDesktopNotificationClick: () => () => {} },
    "thread-1",
  );
  app.start();
  using _timers = fakeTimers();
  await app.complete("2026-09-13T10:00:00.000Z");
  expect(show).not.toHaveBeenCalled();
});
