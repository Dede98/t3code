import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlStageRunLeaseHolderId,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlTaskConsumerGuardError } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import {
  ProviderAdmissionGuard,
  type ProviderAdmissionGuardShape,
} from "../Services/ProviderAdmissionGuard.ts";
import {
  ProviderAdmissionError,
  ProviderAdmissionStore,
} from "../Services/ProviderAdmissionStore.ts";

const isTaskGuardError = Schema.is(AgentControlTaskConsumerGuardError);
const isAdmissionError = Schema.is(ProviderAdmissionError);
const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const store = yield* ProviderAdmissionStore;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;

  const enter: ProviderAdmissionGuardShape["enter"] = Effect.fn("ProviderAdmissionGuard.enter")(
    function* (permit, boundary) {
      const useTask = taskGuard.useTaskForProviderEffectInTransaction;
      if (useTask === undefined) {
        return yield* new ProviderAdmissionError({
          operation: "pre-effect-task-guard",
          reason: "authority-missing",
          admissionId: permit.admissionId,
        });
      }
      const attempt = Effect.fn("ProviderAdmissionGuard.attempt")(function* () {
        const enteredAt = yield* nowIso;
        return yield* sql.withTransaction(
          useTask(ProjectId.make(permit.projectId), AgentControlTaskId.make(permit.taskId), () =>
            store.validateAndEnterInTransaction({ permit, boundary, enteredAt }),
          ),
        );
      });
      yield* attempt().pipe(
        Effect.catchIf(
          (cause) =>
            isAdmissionError(cause) &&
            cause.operation === "pre-effect-stage" &&
            cause.reason === "stale-owner" &&
            leaseEngine.renewOwnedForProviderEffect !== undefined,
          () =>
            Effect.gen(function* () {
              // Permit and task authority were validated before the expiry failure.
              // Renew only the still-owned reservation, then repeat every guard.
              yield* leaseEngine.renewOwnedForProviderEffect!({
                leaseId: AgentControlStageRunLeaseId.make(permit.stageLeaseId),
                holderId: AgentControlStageRunLeaseHolderId.make(permit.stageLeaseHolderId),
                projectId: ProjectId.make(permit.projectId),
                taskId: AgentControlTaskId.make(permit.taskId),
                stageRunId: AgentControlStageRunId.make(permit.stageRunId),
                attemptId: AgentControlAttemptId.make(permit.attemptId),
                fenceToken: permit.stageFenceToken,
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdmissionError({
                      operation: "pre-effect-lease-renewal",
                      reason: "stale-owner",
                      admissionId: permit.admissionId,
                      cause,
                    }),
                ),
              );
              return yield* attempt();
            }),
        ),
        Effect.mapError((cause) =>
          isAdmissionError(cause)
            ? cause
            : new ProviderAdmissionError({
                operation: "pre-effect-guard",
                reason: isTaskGuardError(cause) ? "project-inactive" : "persistence",
                admissionId: permit.admissionId,
                cause,
              }),
        ),
      );
    },
  );

  const quarantineIfEntered: ProviderAdmissionGuardShape["quarantineIfEntered"] = (permit) =>
    nowIso.pipe(Effect.flatMap((observedAt) => store.quarantineIfEntered({ permit, observedAt })));

  return ProviderAdmissionGuard.of({ enter, quarantineIfEntered });
});

export const ProviderAdmissionGuardLive = Layer.effect(ProviderAdmissionGuard, make);
