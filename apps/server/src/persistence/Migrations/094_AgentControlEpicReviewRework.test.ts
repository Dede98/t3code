import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const at = "2026-09-18T08:00:00.000Z";

it.effect("retains immutable review requests, repair intents, and repair results", () =>
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
    ) VALUES ('review-request',1,'{}','result-digest',${at})`;

    for (const mutation of [
      sql`UPDATE agent_control_epic_review_requests SET request_digest='changed'`,
      sql`DELETE FROM agent_control_epic_review_requests`,
      sql`UPDATE agent_control_epic_review_repair_intents SET intent_digest='changed'`,
      sql`DELETE FROM agent_control_epic_review_repair_intents`,
      sql`UPDATE agent_control_epic_review_repair_results SET result_digest='changed'`,
      sql`DELETE FROM agent_control_epic_review_repair_results`,
      sql`UPDATE agent_control_epic_review_repair_delivery_claims SET claimed_at='later'`,
      sql`DELETE FROM agent_control_epic_review_repair_delivery_claims`,
      sql`UPDATE agent_control_epic_review_repair_delivery_receipts SET accepted_at='later'`,
      sql`DELETE FROM agent_control_epic_review_repair_delivery_receipts`,
    ])
      assert.isTrue(Exit.isFailure(yield* Effect.exit(mutation)));

    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO agent_control_epic_review_requests(
            request_id,project_id,epic_run_id,idempotency_key,command_id,request_digest,
            request_json,reviewed_commit_sha,reviewed_verification_evidence_id,
            accepted_revision,accepted_at
          ) VALUES (
            'duplicate-key','review-project','epic-review-migration','review-key','other-command',
            'other-digest','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','review-proof',2,${at}
          )`,
        ),
      ),
    );
    assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
