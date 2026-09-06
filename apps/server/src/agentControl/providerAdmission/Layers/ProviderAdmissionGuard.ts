import { AgentControlTaskId, ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

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
      const enteredAt = yield* nowIso;
      yield* sql
        .withTransaction(
          useTask(ProjectId.make(permit.projectId), AgentControlTaskId.make(permit.taskId), () =>
            store.validateAndEnterInTransaction({ permit, boundary, enteredAt }),
          ),
        )
        .pipe(
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

  const quarantineUnknown: ProviderAdmissionGuardShape["quarantineUnknown"] = (permit) =>
    nowIso.pipe(
      Effect.flatMap((observedAt) =>
        store.quarantine({ permit, reason: "external-outcome-unknown", observedAt }),
      ),
    );

  return ProviderAdmissionGuard.of({ enter, quarantineUnknown });
});

export const ProviderAdmissionGuardLive = Layer.effect(ProviderAdmissionGuard, make);
