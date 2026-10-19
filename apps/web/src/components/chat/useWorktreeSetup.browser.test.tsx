import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId, type WorktreeSetupSnapshot } from "@t3tools/contracts";
import { act, StrictMode, useLayoutEffect } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { useWorktreeSetup } from "./useWorktreeSetup";

const query = vi.hoisted(() => ({ data: null as WorktreeSetupSnapshot | null }));
vi.mock("../../state/query", () => ({ useEnvironmentQuery: () => query }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));

let root: Root;
let observed: ReturnType<typeof useWorktreeSetup>;
const environmentId = EnvironmentId.make("environment-a");
const threadId = ThreadId.make("draft-thread");

// The production ChatView recreates the route reference when its compiler bails out.
const Probe = vi.fn(
  ({
    environment = environmentId,
    thread = threadId,
    revision = 0,
  }: {
    environment?: EnvironmentId;
    thread?: ThreadId;
    revision?: number;
  }) => {
    const ref = scopeThreadRef(environment, thread);
    const setup = useWorktreeSetup({
      routeThreadRef: ref,
      ownerKey: "draft-id",
      activeThread: { id: thread, worktreePath: null },
      threadRef: ref,
      serverProjection: null,
      activityRun: null,
      localPreparing: false,
      sendQueuesBehindSetup: false,
      followUpSent: false,
    });
    useLayoutEffect(() => {
      observed = setup;
    });
    return <output>{revision}</output>;
  },
);

beforeEach(() => {
  query.data = null;
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  root = createRoot(document.createElement("div"));
});

afterEach(async () => {
  await act(() => root.unmount());
  vi.unstubAllGlobals();
});

function snapshot(sequence: number): WorktreeSetupSnapshot {
  return {
    threadId,
    phase: "running",
    startedAt: "2026-10-19T00:00:00.000Z",
    endedAt: null,
    branch: "feature",
    baseRef: "main",
    worktreePath: null,
    setupScript: null,
    stages: [],
    error: null,
    sequence,
  };
}

describe("worktree setup route identity", () => {
  it("renders and rerenders a draft with a fresh reference to the same thread", async () => {
    await act(() =>
      root.render(
        <StrictMode>
          <Probe />
        </StrictMode>,
      ),
    );
    await act(() =>
      root.render(
        <StrictMode>
          <Probe revision={1} />
        </StrictMode>,
      ),
    );
    expect(observed.worktreeSetup).toBeNull();
    expect(observed.isPreparingWorktree).toBe(false);
  });

  it("retains the newest setup snapshot across recreated references and stream cleanup", async () => {
    query.data = snapshot(2);
    await act(() => root.render(<Probe />));
    expect(observed.worktreeSetup?.sequence).toBe(2);
    query.data = snapshot(1);
    await act(() => root.render(<Probe revision={1} />));
    expect(observed.worktreeSetup?.sequence).toBe(2);
    query.data = null;
    await act(() => root.render(<Probe revision={2} />));
    expect(observed.worktreeSetup?.sequence).toBe(2);
  });

  it.each(["environment", "thread"] as const)(
    "drops the retained snapshot when the %s changes",
    async (field) => {
      query.data = snapshot(1);
      await act(() => root.render(<Probe />));
      query.data = null;
      await act(() => root.render(<Probe revision={1} />));
      expect(observed.worktreeSetup?.sequence).toBe(1);
      await act(() =>
        root.render(
          <Probe
            environment={
              field === "environment" ? EnvironmentId.make("environment-b") : environmentId
            }
            thread={field === "thread" ? ThreadId.make("other-thread") : threadId}
            revision={2}
          />,
        ),
      );
      expect(observed.worktreeSetup).toBeNull();
    },
  );
});
