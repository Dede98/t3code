import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const at = "2026-09-18T08:00:00.000Z";

it.effect(
  "upgrades a failed review without rewriting it and binds immutable checkpoint recovery",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 94 });
      yield* sql`INSERT INTO agent_control_epic_runs(
      epic_run_id,project_id,revision,state_json,state_digest
    ) VALUES (
      'epic-review-migration','review-project',1,
      '{"epicRunId":"epic-review-migration","projectId":"review-project","revision":1}',
      'state-digest'
    )`;
      yield* sql`INSERT INTO agent_control_epic_review_requests(
      request_id,project_id,epic_run_id,idempotency_key,command_id,request_digest,
      request_json,reviewed_commit_sha,reviewed_verification_evidence_id,
      accepted_revision,accepted_at
    ) VALUES (
      'review-request','review-project','epic-review-migration','review-key','review-command',
      'request-digest','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','review-proof',2,${at}
    )`;
      yield* sql`INSERT INTO agent_control_epic_review_repair_intents(
      request_id,attempt,intent_json,intent_digest,provider_instance_id,model,runtime_mode,thread_id,
      turn_request_command_id,message_id,worktree_path,branch_name,created_at
    ) VALUES (
      'review-request',1,'{}','intent-digest','repair-provider','repair-model','approval-required',
      'review-thread','review-turn-command','review-message','/isolated/review-worktree',
      't3auto/review',${at}
    )`;
      yield* sql`INSERT INTO agent_control_epic_review_repair_delivery_claims(
      request_id,attempt,claimed_at
    ) VALUES ('review-request',1,${at})`;
      yield* sql`INSERT INTO agent_control_epic_review_repair_delivery_receipts(
      request_id,attempt,provider_turn_id,accepted_at
    ) VALUES ('review-request',1,'provider-turn',${at})`;
      yield* sql`INSERT INTO agent_control_epic_review_repair_results(
      request_id,attempt,result_json,result_digest,completed_at
    ) VALUES ('review-request',1,'{"status":"failed","code":"review-repair-turn-failed"}','result-digest',${at})`;

      const before = yield* sql`SELECT * FROM agent_control_epic_review_repair_results`;
      yield* runMigrations({ toMigrationInclusive: 95 });
      assert.deepEqual(yield* sql`SELECT * FROM agent_control_epic_review_repair_results`, before);
      const insert = (
        digest: string,
      ) => sql`INSERT INTO agent_control_epic_review_repair_recoveries(
      request_id,attempt,original_result_digest,result_json,result_digest,completed_at)
      VALUES ('review-request',1,${digest},'{"status":"succeeded","candidateCommitSha":"candidate"}','recovery-digest',${at})`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(insert("wrong-original"))));
      yield* insert("result-digest");
      for (const mutation of [
        sql`UPDATE agent_control_epic_review_repair_recoveries SET result_digest='changed'`,
        sql`DELETE FROM agent_control_epic_review_repair_recoveries`,
        sql`UPDATE agent_control_epic_review_repair_results SET result_digest='changed'`,
        sql`DELETE FROM agent_control_epic_review_repair_results`,
        insert("result-digest"),
      ])
        assert.isTrue(Exit.isFailure(yield* Effect.exit(mutation)));
      yield* runMigrations({ toMigrationInclusive: 95 });
      assert.deepEqual(yield* sql`SELECT * FROM agent_control_epic_review_repair_results`, before);
      assert.deepEqual(
        yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_recoveries`,
        [{ count: 1 }],
      );
      assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
