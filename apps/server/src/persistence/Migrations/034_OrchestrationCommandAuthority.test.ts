import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("034_OrchestrationCommandAuthority", (it) => {
  it.effect("marks pre-authority receipts as legacy during migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 33 });
      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id,
          aggregate_kind,
          aggregate_id,
          accepted_at,
          result_sequence,
          status,
          error
        ) VALUES (
          'cmd-before-authority',
          'project',
          'project-before-authority',
          '2026-01-01T00:00:00.000Z',
          1,
          'accepted',
          NULL
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 34 });

      const receipts = yield* sql<{ readonly authority: string }>`
        SELECT authority
        FROM orchestration_command_receipts
        WHERE command_id = 'cmd-before-authority'
      `;
      assert.deepStrictEqual(receipts, [{ authority: "legacy" }]);
    }),
  );
});
