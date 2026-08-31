import type { AgentControlRunOnceId, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";

/** Fiber-local capability used only while an explicitly bound Run-Once seam executes. */
export const AgentControlRunOnceExecutionContext = Context.Reference<AgentControlRunOnceId | null>(
  "t3/agentControl/runOnce/AgentControlRunOnceExecutionContext",
  {
    defaultValue: () => null,
  },
);

// Human mode commands and Run-Once external materialization are constructed in
// different Layers (and tests deliberately construct more than one native
// Layer). Keep the fence process-wide, but keyed by project: same-project work
// is ordered while unrelated projects remain independent.
const projectFences = new Map<ProjectId, Semaphore.Semaphore>();

const projectFence = (projectId: ProjectId): Semaphore.Semaphore => {
  const existing = projectFences.get(projectId);
  if (existing !== undefined) return existing;
  const created = Semaphore.makeUnsafe(1);
  projectFences.set(projectId, created);
  return created;
};

export const withAgentControlRunOnceProjectFence = <A, E, R>(
  projectId: ProjectId,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => projectFence(projectId).withPermit(effect);

/** Legacy test doubles may omit new internal seams; production callers fail closed. */
export const requireRunOnceMethod = <T>(method: T | undefined, name: string): T => {
  if (method === undefined) throw new Error(`Run-Once production seam unavailable: ${name}`);
  return method;
};
