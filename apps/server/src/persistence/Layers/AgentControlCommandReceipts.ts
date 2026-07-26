import {
  AgentControlRejectedCommandErrorCode,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlCommandAuthority } from "../../agentControl/AgentControlCommandAuthority.ts";
import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../agentControl/Errors.ts";
import {
  AgentControlCommandReceipt,
  AgentControlCommandReceiptRepository,
  type AgentControlCommandReceiptRepositoryShape,
} from "../Services/AgentControlCommandReceipts.ts";

const PersistedReceiptRow = Schema.Struct({
  commandId: CommandId,
  commandFingerprint: Schema.String,
  authority: AgentControlCommandAuthority,
  aggregateKind: Schema.Literals([
    "project-controller",
    "github-intake",
    "task",
    "stage-run",
    "stage-run-lease",
    "worktree-reservation",
  ]),
  aggregateId: Schema.Union([
    ProjectId,
    AgentControlTaskId,
    AgentControlStageRunId,
    AgentControlStageRunLeaseId,
    AgentControlWorktreeReservationId,
  ]),
  status: Schema.Literals(["accepted", "rejected"]),
  resultSequence: NonNegativeInt,
  resultStreamVersion: NonNegativeInt,
  eventCreated: Schema.Literals([0, 1]),
  acceptedAt: IsoDateTime,
  errorCode: Schema.NullOr(AgentControlRejectedCommandErrorCode),
});

const decodeReceipt = Schema.decodeUnknownEffect(AgentControlCommandReceipt);
const decodePersistedReceiptRow = Schema.decodeUnknownEffect(PersistedReceiptRow);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const makeAgentControlCommandReceiptRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const insert: AgentControlCommandReceiptRepositoryShape["insert"] = (receipt) =>
    decodeReceipt(receipt).pipe(
      Effect.mapError((cause) =>
        decodeError("AgentControlCommandReceiptRepository.insert:input", cause),
      ),
      Effect.flatMap((validated) =>
        sql`
          INSERT INTO agent_control_command_receipts (
            command_id,
            command_fingerprint,
            authority,
            aggregate_kind,
            aggregate_id,
            status,
            result_sequence,
            result_stream_version,
            event_created,
            accepted_at,
            error_code
          ) VALUES (
            ${validated.commandId},
            ${validated.commandFingerprint},
            ${validated.authority},
            ${validated.aggregateKind},
            ${validated.aggregateId},
            ${validated.status},
            ${validated.resultSequence},
            ${validated.resultStreamVersion},
            ${validated.eventCreated ? 1 : 0},
            ${validated.acceptedAt},
            ${validated.errorCode}
          )
        `.pipe(
          Effect.mapError((cause) =>
            sqlError("AgentControlCommandReceiptRepository.insert:query", cause),
          ),
          Effect.asVoid,
        ),
      ),
    );

  const getByCommandId: AgentControlCommandReceiptRepositoryShape["getByCommandId"] = (commandId) =>
    sql<Record<string, unknown>>`
      SELECT
        command_id AS "commandId",
        command_fingerprint AS "commandFingerprint",
        authority,
        aggregate_kind AS "aggregateKind",
        aggregate_id AS "aggregateId",
        status,
        result_sequence AS "resultSequence",
        result_stream_version AS "resultStreamVersion",
        event_created AS "eventCreated",
        accepted_at AS "acceptedAt",
        error_code AS "errorCode"
      FROM agent_control_command_receipts
      WHERE command_id = ${commandId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlCommandReceiptRepository.getByCommandId:query", cause),
      ),
      Effect.flatMap((rows) => {
        const row = rows[0];
        if (row === undefined) return Effect.succeed(Option.none());
        return decodePersistedReceiptRow(row).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlCommandReceiptRepository.getByCommandId:decode", cause),
          ),
          Effect.flatMap((decoded) =>
            decodeReceipt({
              ...decoded,
              eventCreated: decoded.eventCreated === 1,
            }).pipe(
              Effect.mapError((cause) =>
                decodeError("AgentControlCommandReceiptRepository.getByCommandId:invariant", cause),
              ),
            ),
          ),
          Effect.map(Option.some),
        );
      }),
    );

  return AgentControlCommandReceiptRepository.of({ insert, getByCommandId });
});

export const AgentControlCommandReceiptRepositoryLive = Layer.effect(
  AgentControlCommandReceiptRepository,
  makeAgentControlCommandReceiptRepository,
);
