import type { AgentControlRunOnceId, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";

/** Fiber-local capability used only while an explicitly bound Run-Once seam executes. */
export const AgentControlRunOnceExecutionContext = Context.Reference<AgentControlRunOnceId | null>(
  "t3/agentControl/runOnce/AgentControlRunOnceExecutionContext",
  {
    defaultValue: () => null,
  },
);

interface KeyedFenceEntry {
  readonly semaphore: Semaphore.Semaphore;
  references: number;
}

export interface AgentControlRunOnceKeyedFence<K> {
  readonly withPermit: <A, E, R>(key: K, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /** Operational diagnostic: entries exist only while an owner or waiter is active. */
  readonly activeKeyCount: Effect.Effect<number>;
}

/**
 * A keyed, reference-counted mutex. Acquiring a reference is uninterruptible;
 * the bracket releases it after success, failure, or interruption. The entry
 * is evicted only after the last owner or waiter has left, so a replacement
 * semaphore can never overlap an older owner for the same key.
 */
export const makeAgentControlRunOnceKeyedFence = <K>(): AgentControlRunOnceKeyedFence<K> => {
  const entries = new Map<K, KeyedFenceEntry>();

  const withPermit: AgentControlRunOnceKeyedFence<K>["withPermit"] = (key, effect) =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const existing = entries.get(key);
        if (existing !== undefined) {
          existing.references += 1;
          return existing;
        }
        const created: KeyedFenceEntry = {
          semaphore: Semaphore.makeUnsafe(1),
          references: 1,
        };
        entries.set(key, created);
        return created;
      }),
      (entry) => entry.semaphore.withPermit(effect),
      (entry) =>
        Effect.sync(() => {
          const current = entries.get(key);
          if (current !== entry || entry.references < 1) {
            throw new Error("Run-Once keyed fence reference invariant violated");
          }
          entry.references -= 1;
          if (entry.references === 0) entries.delete(key);
        }),
    );

  return {
    withPermit,
    activeKeyCount: Effect.sync(() => entries.size),
  };
};

// Human mode commands and Run-Once external materialization are constructed in
// different Layers (and tests deliberately construct more than one native
// Layer). Keep the fence process-wide, but keyed by project: same-project work
// is ordered while unrelated projects remain independent. Reference-counted
// eviction prevents arbitrary human project ids from becoming process-lifetime
// state.
const projectFences = makeAgentControlRunOnceKeyedFence<ProjectId>();

export const withAgentControlRunOnceProjectFence = <A, E, R>(
  projectId: ProjectId,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => projectFences.withPermit(projectId, effect);

/** Legacy test doubles may omit new internal seams; production callers fail closed. */
export const requireRunOnceMethod = <T>(method: T | undefined, name: string): T => {
  if (method === undefined) throw new Error(`Run-Once production seam unavailable: ${name}`);
  return method;
};
