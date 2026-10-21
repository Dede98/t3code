import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId, type WorktreeSetupSnapshot } from "@t3tools/contracts";
import { StrictMode, useLayoutEffect } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

import { render, type Screen } from "../../../test/render";
import { useWorktreeSetup } from "./useWorktreeSetup";

const query = vi.hoisted(() => ({ data: null as WorktreeSetupSnapshot | null }));
vi.mock("../../state/query", () => ({ useEnvironmentQuery: () => query }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));

const environmentId = EnvironmentId.make("environment-a");
const threadId = ThreadId.make("draft-thread");

interface ProbeProps {
  readonly environment?: EnvironmentId;
  readonly thread?: ThreadId;
  readonly revision?: number;
}

// The production ChatView recreates the route reference when its compiler bails out.
const Probe = vi.fn(
  ({
    environment = environmentId,
    thread = threadId,
    revision = 0,
    onSetup,
  }: ProbeProps & { readonly onSetup: (setup: ReturnType<typeof useWorktreeSetup>) => void }) => {
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
      onSetup(setup);
    });
    return <output>{revision}</output>;
  },
);

// The mocked query reads module state, so every test starts from an empty snapshot.
function mountProbe({ strict = false } = {}) {
  query.data = null;
  let observed: ReturnType<typeof useWorktreeSetup> | undefined;
  let screen: Screen | undefined;
  const onSetup = (setup: ReturnType<typeof useWorktreeSetup>) => {
    observed = setup;
  };
  return {
    get observed() {
      if (!observed) throw new Error("The probe has not rendered.");
      return observed;
    },
    async render(props: ProbeProps = {}) {
      const probe = <Probe {...props} onSetup={onSetup} />;
      const ui = strict ? <StrictMode>{probe}</StrictMode> : probe;
      if (screen) await screen.rerender(ui);
      else screen = await render(ui);
    },
    [Symbol.asyncDispose]: async () => {
      await screen?.[Symbol.asyncDispose]();
    },
  };
}

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
    await using probe = mountProbe({ strict: true });
    await probe.render();
    await probe.render({ revision: 1 });
    expect(probe.observed.worktreeSetup).toBeNull();
    expect(probe.observed.isPreparingWorktree).toBe(false);
  });

  it("retains the newest setup snapshot across recreated references and stream cleanup", async () => {
    await using probe = mountProbe();
    query.data = snapshot(2);
    await probe.render();
    expect(probe.observed.worktreeSetup?.sequence).toBe(2);
    query.data = snapshot(1);
    await probe.render({ revision: 1 });
    expect(probe.observed.worktreeSetup?.sequence).toBe(2);
    query.data = null;
    await probe.render({ revision: 2 });
    expect(probe.observed.worktreeSetup?.sequence).toBe(2);
  });

  it.each(["environment", "thread"] as const)(
    "drops the retained snapshot when the %s changes",
    async (field) => {
      await using probe = mountProbe();
      query.data = snapshot(1);
      await probe.render();
      query.data = null;
      await probe.render({ revision: 1 });
      expect(probe.observed.worktreeSetup?.sequence).toBe(1);
      await probe.render({
        environment: field === "environment" ? EnvironmentId.make("environment-b") : environmentId,
        thread: field === "thread" ? ThreadId.make("other-thread") : threadId,
        revision: 2,
      });
      expect(probe.observed.worktreeSetup).toBeNull();
    },
  );
});
