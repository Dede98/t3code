import type { ClientSettings } from "@t3tools/contracts/settings";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { expect, it, vi } from "vite-plus/test";

import { render, stubGlobal, type Screen } from "../../test/render";

const state = vi.hoisted(() => ({
  mode: "notifications" as ClientSettings["notificationMode"],
  active: { environmentId: "env-1", threadId: "other-thread" },
  completedAt: null as string | null,
  navigate: vi.fn(),
  setActiveEnvironmentId: vi.fn(),
  notification: vi.fn(function (_title: string, options: NotificationOptions) {
    return Object.assign(new EventTarget(), { tag: options.tag, close: vi.fn() });
  }),
}));

const SHELL_NOW = DateTime.makeUnsafe("2026-09-13T09:00:00.000Z");

function mockThreadShell() {
  return {
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
    status: state.completedAt ? "completed" : "running",
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
    latestRunCompletedAt: state.completedAt ? DateTime.makeUnsafe(state.completedAt) : undefined,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

vi.mock(import("@effect/atom-react"), async (original) => ({
  ...(await original()),
  useAtomValue: (() => ({
    status: "live",
    snapshot: Option.some({ projects: [], threads: [mockThreadShell()] }),
  })) as never,
}));
vi.mock(import("@tanstack/react-router"), async (original) => ({
  ...(await original()),
  useNavigate: (() => state.navigate) as never,
  useParams: (() => state.active) as never,
}));
vi.mock(import("../hooks/useSettings"), async (original) => ({
  ...(await original()),
  useClientSettings: ((
    select: (
      settings: Pick<ClientSettings, "notificationMode" | "inAppNotificationsEnabled">,
    ) => unknown,
  ) => select({ notificationMode: state.mode, inAppNotificationsEnabled: false })) as never,
  getClientSettings: (() => ({ notificationMode: state.mode })) as never,
}));
vi.mock("../state/entities", () => ({ setActiveEnvironmentId: state.setActiveEnvironmentId }));
vi.mock(import("../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironmentIds: (() => ["env-1"]) as never,
}));
vi.mock(import("../threadNotifications"), async (original) => ({
  ...(await original()),
  playNotificationSound: vi.fn(),
  setNotificationBadge: vi.fn(),
}));

import { ThreadNotificationCoordinator } from "./ThreadNotificationCoordinator";

// The mocked modules read `state`, so every test starts by resetting it. Native desktop
// alerts must replace the web Notification API, which stays available to prove it is unused.
function setup(bridge: Partial<NonNullable<typeof window.desktopBridge>>) {
  vi.clearAllMocks();
  Object.assign(state, {
    mode: "notifications",
    active: { environmentId: "env-1", threadId: "other-thread" },
    completedAt: null,
  });
  const stack = new AsyncDisposableStack();
  stack.use(vi.spyOn(document, "hasFocus").mockReturnValue(true));
  stack.use(vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible"));
  stack.use(
    stubGlobal("Notification", Object.assign(state.notification, { permission: "granted" })),
  );
  window.desktopBridge = bridge as NonNullable<typeof window.desktopBridge>;
  stack.defer(() => {
    delete window.desktopBridge;
  });
  let screen: Screen | undefined;
  const renderApp = async () => {
    if (screen) await screen.rerender(<ThreadNotificationCoordinator />);
    else screen = stack.use(await render(<ThreadNotificationCoordinator />));
  };
  return {
    render: renderApp,
    async complete(completedAt: string) {
      state.completedAt = completedAt;
      await renderApp();
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
  await app.render();
  await app.complete("2026-09-13T10:00:00.000Z");
  await app.render();
  expect(show).toHaveBeenCalledExactlyOnceWith({
    environmentId: "env-1",
    threadId: "thread-1",
    kind: "completion",
    projectTitle: "Project",
    threadTitle: "Fix the login form",
  });
  expect(state.notification).not.toHaveBeenCalled();

  clicked!({ environmentId: "env-1", threadId: "thread-1" });
  expect(state.setActiveEnvironmentId).toHaveBeenCalledWith("env-1");
  expect(state.navigate).toHaveBeenCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "env-1", threadId: "thread-1" },
  });

  state.mode = "off";
  await app.render();
  await app.complete("2026-09-13T10:05:00.000Z");
  expect(show).toHaveBeenCalledOnce();
});

it("does not notify the visible active desktop thread", async () => {
  const show = vi.fn().mockResolvedValue(true);
  await using app = setup({
    showDesktopNotification: show,
    onDesktopNotificationClick: () => () => {},
  });
  state.active.threadId = "thread-1";
  await app.render();
  await app.complete("2026-09-13T10:00:00.000Z");
  expect(show).not.toHaveBeenCalled();
});
