import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { toPersistenceSqlError } from "../Errors.ts";

import {
  GetByCommandIdInput,
  OrchestrationCommandReceipt,
  OrchestrationCommandReceiptRepository,
  type OrchestrationCommandReceiptRepositoryShape,
} from "../Services/OrchestrationCommandReceipts.ts";

const makeOrchestrationCommandReceiptRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const insertReceiptRow = SqlSchema.void({
    Request: OrchestrationCommandReceipt,
    execute: (receipt) =>
      sql`
        INSERT INTO main.orchestration_command_receipts (
          command_id,
          authority,
          aggregate_kind,
          aggregate_id,
          accepted_at,
          result_sequence,
          status,
          error
        )
        VALUES (
          ${receipt.commandId},
          ${receipt.authority},
          ${receipt.aggregateKind},
          ${receipt.aggregateId},
          ${receipt.acceptedAt},
          ${receipt.resultSequence},
          ${receipt.status},
          ${receipt.error}
        )
      `,
  });

  const findReceiptByCommandId = SqlSchema.findOneOption({
    Request: GetByCommandIdInput,
    Result: OrchestrationCommandReceipt,
    execute: ({ commandId }) =>
      sql`
        SELECT
          command_id AS "commandId",
          authority,
          aggregate_kind AS "aggregateKind",
          aggregate_id AS "aggregateId",
          accepted_at AS "acceptedAt",
          result_sequence AS "resultSequence",
          status,
          error
        FROM main.orchestration_command_receipts
        WHERE command_id = ${commandId}
      `,
  });

  const insert: OrchestrationCommandReceiptRepositoryShape["insert"] = (receipt) =>
    insertReceiptRow(receipt).pipe(
      Effect.mapError(toPersistenceSqlError("OrchestrationCommandReceiptRepository.insert:query")),
    );

  const getByCommandId: OrchestrationCommandReceiptRepositoryShape["getByCommandId"] = (input) =>
    findReceiptByCommandId(input).pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationCommandReceiptRepository.getByCommandId:query"),
      ),
    );

  return {
    insert,
    getByCommandId,
  } satisfies OrchestrationCommandReceiptRepositoryShape;
});

export const OrchestrationCommandReceiptRepositoryLive = Layer.effect(
  OrchestrationCommandReceiptRepository,
  makeOrchestrationCommandReceiptRepository,
);
