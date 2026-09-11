import { describe, expect, it } from "@effect/vitest";
import {
  AGENT_CONTROL_RUNTIME_RPC_METHODS,
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
  const supervisors = new Map<EnvironmentId, EnvironmentSupervisor["Service"]>();
  for (const id of [environmentId, otherEnvironmentId]) {
    const client = {
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
  return { registry, atoms: createAgentControlEnvironmentAtoms(runtime), calls, arrivals };
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
