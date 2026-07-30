import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationRpcError,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlTaskId,
  CommandId,
  ProjectId,
  ThreadId,
  type AgentControlControlledThreadReservationCommandResult,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { deriveAgentControlControlledThreadActivationCommandId } from "../identity.ts";
import { AgentControlControlledThreadActivation } from "../Services/AgentControlControlledThreadActivation.ts";
import {
  AgentControlControlledThreadActivationHooks,
  AgentControlControlledThreadActivationHooksNoop,
  type AgentControlControlledThreadActivationHooksShape,
} from "../Services/AgentControlControlledThreadActivationHooks.ts";
import {
  AgentControlControlledThreadMaterializationCoordinator,
  AgentControlControlledThreadMaterializationCoordinatorError,
  type AgentControlControlledThreadMaterializationCoordinatorReason,
  type AgentControlControlledThreadMaterializationCoordinatorShape,
} from "../Services/AgentControlControlledThreadMaterializationCoordinator.ts";
import {
  AgentControlControlledThreadReservation,
  type AgentControlControlledThreadReservationShape,
} from "../Services/AgentControlControlledThreadReservation.ts";
import { AgentControlControlledThreadActivationLive } from "./AgentControlControlledThreadActivation.ts";

const projectId = ProjectId.make("activation-project");
const taskId = AgentControlTaskId.make("activation-task");
const controlledThreadReservationId = AgentControlControlledThreadReservationId.make(
  "controlled-thread-reservation-activation-test",
);
const threadId = ThreadId.make("t3-auto-reserved-thread-activation-test");
const input = {
  commandId: CommandId.make("activation-prepare-command"),
  projectId,
  taskId,
} as const;
const prepared: AgentControlControlledThreadReservationCommandResult = {
  reservation: {
    controlledThreadReservationId,
    threadId,
    projectId,
    taskId,
    stageRunId: AgentControlStageRunId.make("activation-stage-run"),
    attemptId: AgentControlAttemptId.make("activation-attempt"),
    roleId: AgentControlRoleId.make("planning"),
    status: "prepared",
    revision: 1,
    preparedAt: "2026-07-29T10:00:00.000Z",
  },
  resultSequence: 41,
  eventCreated: true,
};

const reservationService = (
  prepareInitial: AgentControlControlledThreadReservationShape["prepareInitial"],
) =>
  AgentControlControlledThreadReservation.of({
    get: () => Effect.die("get is outside activation"),
    list: () => Effect.die("list is outside activation"),
    prepareInitial,
  });

const coordinatorService = (
  materializeInitial: AgentControlControlledThreadMaterializationCoordinatorShape["materializeInitial"],
) =>
  AgentControlControlledThreadMaterializationCoordinator.of({
    materializeInitial,
  });

const buildActivation = (
  reservation: AgentControlControlledThreadReservationShape,
  coordinator: AgentControlControlledThreadMaterializationCoordinatorShape,
  hooks?: AgentControlControlledThreadActivationHooksShape,
) =>
  Layer.build(
    AgentControlControlledThreadActivationLive.pipe(
      Layer.provide(Layer.succeed(AgentControlControlledThreadReservation, reservation)),
      Layer.provide(
        Layer.succeed(AgentControlControlledThreadMaterializationCoordinator, coordinator),
      ),
      Layer.provide(
        hooks === undefined
          ? AgentControlControlledThreadActivationHooksNoop
          : Layer.succeed(
              AgentControlControlledThreadActivationHooks,
              AgentControlControlledThreadActivationHooks.of(hooks),
            ),
      ),
    ),
  ).pipe(
    Effect.map((context) => Context.get(context, AgentControlControlledThreadActivation)),
    Effect.scoped,
  );

it.effect("runs prepare, deterministic materialization, and hooks in order", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyArray<string>>([]);
    const coordinatorInput = yield* Ref.make<{
      readonly commandId: CommandId;
      readonly projectId: ProjectId;
      readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
    } | null>(null);
    const activation = yield* buildActivation(
      reservationService(() =>
        Ref.update(calls, (current) => [...current, "prepare"]).pipe(Effect.as(prepared)),
      ),
      coordinatorService((command) =>
        Ref.set(coordinatorInput, command).pipe(
          Effect.andThen(Ref.update(calls, (current) => [...current, "materialize"])),
          Effect.as({
            commandId: command.commandId,
            controlledThreadReservationId: command.controlledThreadReservationId,
            threadId,
            orchestrationResultSequence: 52,
            status: "bound",
            replayed: false,
          }),
        ),
      ),
      {
        afterPrepareAcceptedBeforeMaterialize: () =>
          Ref.update(calls, (current) => [...current, "after-prepare"]),
        afterMaterializationAcceptedBeforeReturn: () =>
          Ref.update(calls, (current) => [...current, "after-materialize"]),
      },
    );

    const result = yield* activation.activateInitial(input);
    assert.strictEqual(result, prepared);
    assert.deepStrictEqual(yield* Ref.get(calls), [
      "prepare",
      "after-prepare",
      "materialize",
      "after-materialize",
    ]);
    assert.deepStrictEqual(yield* Ref.get(coordinatorInput), {
      commandId: yield* deriveAgentControlControlledThreadActivationCommandId(
        input.commandId,
        controlledThreadReservationId,
      ),
      projectId,
      controlledThreadReservationId,
    });
  }),
);

it.effect("never invokes hooks or coordinator after stable or receiptless prepare failures", () =>
  Effect.gen(function* () {
    for (const code of ["task-missing", "internal-persistence-error"] as const) {
      const coordinatorCalls = yield* Ref.make(0);
      const hookCalls = yield* Ref.make(0);
      const activation = yield* buildActivation(
        reservationService(() =>
          Effect.fail(
            new AgentControlControlledThreadReservationRpcError({
              code,
              operation: "prepare-initial",
              projectId,
              taskId,
              controlledThreadReservationId: null,
            }),
          ),
        ),
        coordinatorService(() =>
          Ref.update(coordinatorCalls, (count) => count + 1).pipe(
            Effect.andThen(Effect.die("coordinator must not run")),
          ),
        ),
        {
          afterPrepareAcceptedBeforeMaterialize: () => Ref.update(hookCalls, (count) => count + 1),
          afterMaterializationAcceptedBeforeReturn: () =>
            Ref.update(hookCalls, (count) => count + 1),
        },
      );

      const result = yield* Effect.result(activation.activateInitial(input));
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, code);
      }
      assert.equal(yield* Ref.get(coordinatorCalls), 0);
      assert.equal(yield* Ref.get(hookCalls), 0);
    }
  }),
);

const coordinatorErrorCases: ReadonlyArray<{
  readonly reason: AgentControlControlledThreadMaterializationCoordinatorReason;
  readonly code: AgentControlControlledThreadReservationRpcError["code"];
}> = [
  { reason: "validation", code: "validation" },
  { reason: "command-identity-conflict", code: "command-identity-mismatch" },
  { reason: "project-unavailable", code: "project-unavailable" },
  { reason: "project-mode-inactive", code: "project-mode-inactive" },
  { reason: "task-unavailable", code: "state-not-available" },
  { reason: "source-snapshot-stale", code: "source-snapshot-stale" },
  { reason: "stage-run-unavailable", code: "state-not-available" },
  { reason: "lease-unavailable", code: "state-not-available" },
  { reason: "lease-expired", code: "lease-expired" },
  { reason: "lease-foreign-runtime", code: "lease-foreign-runtime" },
  { reason: "worktree-unavailable", code: "state-not-available" },
  { reason: "reservation-missing", code: "controlled-thread-reservation-missing" },
  { reason: "reservation-not-prepared", code: "state-not-available" },
  { reason: "reservation-conflict", code: "controlled-thread-reservation-corrupt" },
  { reason: "runtime-policy-unavailable", code: "state-not-available" },
  { reason: "historical-evidence-corrupt", code: "controlled-thread-reservation-corrupt" },
  { reason: "internal-persistence-error", code: "internal-persistence-error" },
];

it.effect("maps every coordinator failure to a closed prepare-initial wire error", () =>
  Effect.gen(function* () {
    for (const testCase of coordinatorErrorCases) {
      const activation = yield* buildActivation(
        reservationService(() => Effect.succeed(prepared)),
        coordinatorService((command) =>
          Effect.fail(
            new AgentControlControlledThreadMaterializationCoordinatorError({
              reason: testCase.reason,
              commandId: command.commandId,
              projectId,
              controlledThreadReservationId,
              cause: {
                path: "/private/secret",
                holder: "runtime-holder",
                fence: 99,
                policyFingerprint: "not-for-wire",
              },
            }),
          ),
        ),
      );

      const result = yield* Effect.result(activation.activateInitial(input));
      assert.equal(result._tag, "Failure", testCase.reason);
      if (result._tag === "Failure") {
        assert.deepStrictEqual(
          {
            code: result.failure.code,
            operation: result.failure.operation,
            projectId: result.failure.projectId,
            taskId: result.failure.taskId,
            controlledThreadReservationId: result.failure.controlledThreadReservationId,
          },
          {
            code: testCase.code,
            operation: "prepare-initial",
            projectId,
            taskId,
            controlledThreadReservationId,
          },
          testCase.reason,
        );
        const encoded = Reflect.ownKeys(result.failure)
          .map((key) => `${String(key)}=${String(Reflect.get(result.failure, key))}`)
          .join("|");
        for (const forbidden of [
          "/private/secret",
          "runtime-holder",
          "policyFingerprint",
          "not-for-wire",
          "fence",
          "cause",
        ]) {
          assert.notInclude(encoded, forbidden, `${testCase.reason}:${forbidden}`);
        }
      }
    }
  }),
);

it.effect("fails closed before materialization for a non-historical prepare result", () =>
  Effect.gen(function* () {
    const coordinatorCalls = yield* Ref.make(0);
    const activation = yield* buildActivation(
      reservationService(() =>
        Effect.succeed({
          ...prepared,
          reservation: { ...prepared.reservation, status: "bound" as const, revision: 3 },
        }),
      ),
      coordinatorService(() =>
        Ref.update(coordinatorCalls, (count) => count + 1).pipe(
          Effect.andThen(Effect.die("coordinator must not run")),
        ),
      ),
    );

    const result = yield* Effect.result(activation.activateInitial(input));
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure.code, "controlled-thread-reservation-corrupt");
    }
    assert.equal(yield* Ref.get(coordinatorCalls), 0);
  }),
);

it.effect("fails closed when the coordinator result is not bound to the derived command", () =>
  Effect.gen(function* () {
    const activation = yield* buildActivation(
      reservationService(() => Effect.succeed(prepared)),
      coordinatorService((command) =>
        Effect.succeed({
          commandId: CommandId.make("wrong-activation-command"),
          controlledThreadReservationId: command.controlledThreadReservationId,
          threadId,
          orchestrationResultSequence: 52,
          status: "bound",
          replayed: false,
        }),
      ),
    );

    const result = yield* Effect.result(activation.activateInitial(input));
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure.code, "controlled-thread-reservation-corrupt");
      assert.equal(result.failure.operation, "prepare-initial");
      assert.equal(result.failure.controlledThreadReservationId, controlledThreadReservationId);
    }
  }),
);
