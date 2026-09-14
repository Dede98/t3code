import { describe, expect, it } from "@effect/vitest";
import {
  AGENT_CONTROL_RUNTIME_RPC_METHODS,
  AGENT_CONTROL_EPIC_RPC_METHODS,
  AGENT_CONTROL_RUN_ONCE_RPC_METHODS,
  AgentControlRunOnceId,
  type AgentControlRunOnceSnapshot,
  type AgentControlRunOnceSnapshotInput,
  AgentControlEpicRpcError,
  type AgentControlEpicStartInput,
  type AgentControlEpicHandoffPublishInput,
  AgentControlCommandPreviouslyRejectedError,
  AgentControlProjectRevisionConflictError,
  CommandId,
  EnvironmentId,
  ProjectId,
  type AgentControlRuntimeRpcError,
  type AgentControlSetProjectModeInput,
  type AgentControlSetProjectModeResult,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/unstable/reactivity";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createAgentControlEnvironmentAtoms } from "./agentControl.ts";
import { squashAtomCommandFailure } from "./runtime.ts";

const environmentId = EnvironmentId.make("environment-a");
const otherEnvironmentId = EnvironmentId.make("environment-b");
const target = {
  environmentId,
  input: {
    projectId: ProjectId.make("project-a"),
    commandId: CommandId.make("enable-armed"),
    expectedRevision: 3,
    mode: "armed",
  } satisfies AgentControlSetProjectModeInput,
};

type PendingCall = {
  environmentId: EnvironmentId;
  input: AgentControlSetProjectModeInput;
  response: Deferred.Deferred<AgentControlSetProjectModeResult, AgentControlRuntimeRpcError>;
};

type SavedRunCall = {
  environmentId: EnvironmentId;
  input: AgentControlRunOnceSnapshotInput;
  response: Deferred.Deferred<AgentControlRunOnceSnapshot>;
};

type EpicCall = {
  environmentId: EnvironmentId;
  input: AgentControlEpicStartInput;
  response: Deferred.Deferred<never, AgentControlEpicRpcError>;
};

type HandoffCall = {
  environmentId: EnvironmentId;
  input: AgentControlEpicHandoffPublishInput;
  response: Deferred.Deferred<never, AgentControlEpicRpcError>;
};

function confirmed(input: AgentControlSetProjectModeInput): AgentControlSetProjectModeResult {
  return {
    state: {
      schemaVersion: 1,
      projectId: input.projectId,
      mode: input.mode,
      pausedFromMode: null,
      revision: input.expectedRevision + 1,
      sequence: 8,
      updatedAt: null,
    },
    eventCreated: true,
    resultSequence: 8,
  };
}

const makeHarness = Effect.fn("makeAgentControlCommandHarness")(function* () {
  const calls: PendingCall[] = [];
  const arrivals = yield* Queue.unbounded<PendingCall>();
  const epicCalls: EpicCall[] = [];
  const epicArrivals = yield* Queue.unbounded<EpicCall>();
  const savedRunArrivals = yield* Queue.unbounded<SavedRunCall>();
  const handoffArrivals = yield* Queue.unbounded<HandoffCall>();
  const handoffCalls: HandoffCall[] = [];
  const supervisors = new Map<EnvironmentId, EnvironmentSupervisor["Service"]>();
  for (const id of [environmentId, otherEnvironmentId]) {
    const client = {
      [AGENT_CONTROL_EPIC_RPC_METHODS.publishHandoff]: Effect.fn(function* (
        input: AgentControlEpicHandoffPublishInput,
      ) {
        const response = yield* Deferred.make<never, AgentControlEpicRpcError>();
        const call = { environmentId: id, input, response };
        handoffCalls.push(call);
        yield* Queue.offer(handoffArrivals, call);
        return yield* Deferred.await(response);
      }),
      [AGENT_CONTROL_RUN_ONCE_RPC_METHODS.getSnapshot]: Effect.fn(function* (
        input: AgentControlRunOnceSnapshotInput,
      ) {
        const response = yield* Deferred.make<AgentControlRunOnceSnapshot>();
        yield* Queue.offer(savedRunArrivals, { environmentId: id, input, response });
        return yield* Deferred.await(response);
      }),
      [AGENT_CONTROL_EPIC_RPC_METHODS.start]: Effect.fn(function* (
        input: AgentControlEpicStartInput,
      ) {
        const response = yield* Deferred.make<never, AgentControlEpicRpcError>();
        const call = { environmentId: id, input, response };
        epicCalls.push(call);
        yield* Queue.offer(epicArrivals, call);
        return yield* Deferred.await(response);
      }),
      [AGENT_CONTROL_RUNTIME_RPC_METHODS.setProjectMode]: Effect.fn(function* (
        input: AgentControlSetProjectModeInput,
      ) {
        const response = yield* Deferred.make<
          AgentControlSetProjectModeResult,
          AgentControlRuntimeRpcError
        >();
        const call = { environmentId: id, input, response };
        calls.push(call);
        yield* Queue.offer(arrivals, call);
        return yield* Deferred.await(response);
      }),
    } as unknown as WsRpcProtocolClient;
    const session: RpcSession = {
      client,
      initialConfig: Effect.never,
      subscribeServerConfig: (input) => client.subscribeServerConfig(input),
      ready: Effect.void,
      probe: Effect.void,
      closed: Effect.never,
    };
    const connectionState: SupervisorConnectionState = {
      ...AVAILABLE_CONNECTION_STATE,
      desired: true,
      network: "online",
      phase: "connected",
      attempt: 1,
      generation: 1,
    };
    supervisors.set(
      id,
      EnvironmentSupervisor.of({
        target: new PrimaryConnectionTarget({
          environmentId: id,
          label: id,
          httpBaseUrl: "https://environment.example.test",
          wsBaseUrl: "wss://environment.example.test",
        }),
        state: yield* SubscriptionRef.make(connectionState),
        session: yield* SubscriptionRef.make(Option.some(session)),
        prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      }),
    );
  }
  const run: EnvironmentRegistry["Service"]["run"] = (id, effect) =>
    Effect.provideService(effect, EnvironmentSupervisor, supervisors.get(id)!);
  const runtime = Atom.runtime(
    Layer.succeed(
      EnvironmentRegistry,
      EnvironmentRegistry.of({ run } as unknown as EnvironmentRegistry["Service"]),
    ),
  );
  const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
    Effect.sync(() => registry.dispose()),
  );
  return {
    registry,
    atoms: createAgentControlEnvironmentAtoms(runtime),
    calls,
    arrivals,
    epicCalls,
    epicArrivals,
    savedRunArrivals,
    handoffArrivals,
    handoffCalls,
  };
});

describe("autonomous task mode commands", () => {
  it.effect(
    "keeps pending across remounts, deduplicates clicks and waits for server confirmation",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, atoms, calls, arrivals } = yield* makeHarness();
          const pending = atoms.pending(target);
          const unmount = registry.mount(pending);
          expect(registry.get(pending)).toBe(false);
          let settled = false;
          const first = atoms.setMode.run(registry, target).then((result) => {
            settled = true;
            return result;
          });
          expect(registry.get(pending)).toBe(true);
          const call = yield* Queue.take(arrivals);
          unmount();
          const remount = registry.mount(atoms.pending({ ...target }));
          expect(registry.get(pending)).toBe(true);
          const duplicate = atoms.setMode.run(registry, target);
          expect(settled).toBe(false);
          expect(calls.map((call) => call.input)).toEqual([target.input]);

          yield* Deferred.succeed(call.response, confirmed(call.input));
          const results = yield* Effect.promise(() => Promise.all([first, duplicate]));
          expect(results).toEqual([
            expect.objectContaining({ _tag: "Success", value: confirmed(target.input) }),
            expect.objectContaining({ _tag: "Success", value: confirmed(target.input) }),
          ]);
          expect(calls).toHaveLength(1);
          expect(registry.get(pending)).toBe(false);
          remount();
        }),
      ),
  );

  it.effect("scopes pending and concurrent requests to both environment and project", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { registry, atoms, arrivals } = yield* makeHarness();
        const otherEnvironment = { ...target, environmentId: otherEnvironmentId };
        const otherProject = {
          ...target,
          input: { ...target.input, projectId: ProjectId.make("project-b") },
        };
        const first = atoms.setMode.run(registry, target);
        const firstCall = yield* Queue.take(arrivals);
        expect(registry.get(atoms.pending(otherEnvironment))).toBe(false);
        expect(registry.get(atoms.pending(otherProject))).toBe(false);

        for (const independentTarget of [otherEnvironment, otherProject]) {
          const independent = atoms.setMode.run(registry, independentTarget);
          const independentCall = yield* Queue.take(arrivals);
          expect(independentCall).toMatchObject(independentTarget);
          expect(registry.get(atoms.pending(independentTarget))).toBe(true);
          yield* Deferred.succeed(independentCall.response, confirmed(independentCall.input));
          expect(yield* Effect.promise(() => independent)).toMatchObject({ _tag: "Success" });
          expect(registry.get(atoms.pending(independentTarget))).toBe(false);
          expect(registry.get(atoms.pending(target))).toBe(true);
        }
        yield* Deferred.succeed(firstCall.response, confirmed(firstCall.input));
        expect(yield* Effect.promise(() => first)).toMatchObject({ _tag: "Success" });
        expect(registry.get(atoms.pending(target))).toBe(false);
      }),
    ),
  );

  it.effect(
    "clears pending after revision conflicts and rejected commands without reporting success",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, atoms, arrivals } = yield* makeHarness();
          const failures = [
            new AgentControlProjectRevisionConflictError({
              code: "revision-conflict",
              projectId: target.input.projectId,
              expectedRevision: target.input.expectedRevision,
              actualRevision: 4,
            }),
            new AgentControlCommandPreviouslyRejectedError({
              code: "command-previously-rejected",
              commandId: target.input.commandId,
              originalErrorCode: "revision-conflict",
            }),
          ];
          for (const failure of failures) {
            const request = atoms.setMode.run(registry, target);
            const call = yield* Queue.take(arrivals);
            expect(registry.get(atoms.pending(target))).toBe(true);
            yield* Deferred.fail(call.response, failure);
            const result = yield* Effect.promise(() => request);
            expect(result._tag).toBe("Failure");
            if (result._tag === "Failure")
              expect(squashAtomCommandFailure(result)).toEqual(failure);
            expect(registry.get(atoms.pending(target))).toBe(false);
          }
        }),
      ),
  );
});

it.effect(
  "Epic starts deduplicate across remounts and retain shared pending while another project command is active",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { registry, atoms, arrivals, epicArrivals, epicCalls } = yield* makeHarness();
        const epicTarget = {
          environmentId,
          input: {
            projectId: target.input.projectId,
            commandId: CommandId.make("epic-start"),
            expectedRevision: 3,
            epicNumber: 100,
            expectedFingerprint: "native-scope",
          },
        };
        const unmount = registry.mount(atoms.pending(epicTarget));
        const first = atoms.epicStart.run(registry, epicTarget);
        const call = yield* Queue.take(epicArrivals);
        unmount();
        const remount = registry.mount(atoms.pending(epicTarget));
        const repeated = atoms.epicStart.run(registry, { ...epicTarget });
        expect(epicCalls).toHaveLength(1);
        expect(registry.get(atoms.pending(epicTarget))).toBe(true);
        expect(
          registry.get(atoms.pending({ ...epicTarget, environmentId: otherEnvironmentId })),
        ).toBe(false);

        const modeRequest = atoms.setMode.run(registry, target);
        const modeCall = yield* Queue.take(arrivals);
        const failure = new AgentControlEpicRpcError({
          code: "scope-changed",
          message: "Inspect the changed Epic again",
        });
        yield* Deferred.fail(call.response, failure);
        const results = yield* Effect.promise(() => Promise.all([first, repeated]));
        expect(results.every((result) => result._tag === "Failure")).toBe(true);
        expect(epicCalls).toHaveLength(1);
        expect(registry.get(atoms.pending(epicTarget))).toBe(true);
        yield* Deferred.succeed(modeCall.response, confirmed(modeCall.input));
        yield* Effect.promise(() => modeRequest);
        expect(registry.get(atoms.pending(epicTarget))).toBe(false);
        remount();
      }),
    ),
);

it.effect("keeps distinct historical child lookups separate while sharing project pending", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { registry, atoms, savedRunArrivals } = yield* makeHarness();
      const firstTarget = {
        environmentId,
        input: {
          projectId: target.input.projectId,
          runId: AgentControlRunOnceId.make("old-child-a"),
        },
      };
      const secondTarget = {
        ...firstTarget,
        input: { ...firstTarget.input, runId: AgentControlRunOnceId.make("old-child-b") },
      };
      const first = atoms.getRun.run(registry, firstTarget);
      const firstCall = yield* Queue.take(savedRunArrivals);
      const second = atoms.getRun.run(registry, secondTarget);
      const secondCall = yield* Queue.take(savedRunArrivals);
      expect(firstCall.input.runId).toBe("old-child-a");
      expect(secondCall.input.runId).toBe("old-child-b");
      const result: AgentControlRunOnceSnapshot = {
        projectId: target.input.projectId,
        projectState: { ...confirmed(target.input).state, schemaVersion: 1 },
        tasks: [],
        runs: [],
        nextTaskId: null,
        blockers: [],
      };
      yield* Deferred.succeed(secondCall.response, result);
      yield* Effect.promise(() => second);
      expect(registry.get(atoms.pending(firstTarget))).toBe(true);
      yield* Deferred.succeed(firstCall.response, result);
      yield* Effect.promise(() => first);
      expect(registry.get(atoms.pending(firstTarget))).toBe(false);
    }),
  ),
);

it.effect(
  "deduplicates handoff clicks across remounts while isolating projects, environments and Epic runs",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { registry, atoms, handoffArrivals, handoffCalls } = yield* makeHarness();
        const publication = {
          environmentId,
          input: {
            projectId: target.input.projectId,
            commandId: CommandId.make("publish-epic"),
            expectedRevision: 7,
            epicRunId: "epic-a",
            expectedCommitSha: "verified-commit",
            expectedTargetBranch: "main",
          },
        };
        const unmount = registry.mount(atoms.pending(publication));
        const first = atoms.epicPublishHandoff.run(registry, publication);
        const firstCall = yield* Queue.take(handoffArrivals);
        unmount();
        const remount = registry.mount(atoms.pending(publication));
        const repeated = atoms.epicPublishHandoff.run(registry, { ...publication });
        expect(handoffCalls).toHaveLength(1);
        expect(registry.get(atoms.pending(publication))).toBe(true);
        const failure = new AgentControlEpicRpcError({
          code: "offline",
          message: "Reconnect and retry the same handoff",
        });
        for (const separate of [
          { ...publication, environmentId: otherEnvironmentId },
          {
            ...publication,
            input: { ...publication.input, projectId: ProjectId.make("project-b") },
          },
          { ...publication, input: { ...publication.input, epicRunId: "epic-b" } },
        ]) {
          const request = atoms.epicPublishHandoff.run(registry, separate);
          const call = yield* Queue.take(handoffArrivals);
          expect(call).toMatchObject(separate);
          yield* Deferred.fail(call.response, failure);
          expect((yield* Effect.promise(() => request))._tag).toBe("Failure");
          expect(registry.get(atoms.pending(publication))).toBe(true);
        }
        yield* Deferred.fail(firstCall.response, failure);
        const results = yield* Effect.promise(() => Promise.all([first, repeated]));
        expect(results.every((result) => result._tag === "Failure")).toBe(true);
        expect(handoffCalls).toHaveLength(4);
        expect(registry.get(atoms.pending(publication))).toBe(false);
        const retry = atoms.epicPublishHandoff.run(registry, publication);
        const retryCall = yield* Queue.take(handoffArrivals);
        expect(retryCall.input).toEqual(firstCall.input);
        yield* Deferred.fail(retryCall.response, failure);
        yield* Effect.promise(() => retry);
        expect(registry.get(atoms.pending(publication))).toBe(false);
        remount();
      }),
    ),
);
