import { CommandId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { loadEpicReviewRepairTurnOwnership } from "../../orchestration/Layers/ProviderCommandReactor.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson } from "../initialPlanning/eventEvidence.ts";

const at = "2026-09-18T12:00:00.000Z";

it.effect("retains autonomous review ownership after its start authority is cancelled", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 94 });
    yield* sql`INSERT INTO agent_control_project_states(
      project_id,mode,paused_from_mode,revision,last_event_sequence,updated_at)
      VALUES ('review-owner-project','armed',NULL,1,1,${at})`;
    const activeDocument = {
      epicRunId: "review-owner-run",
      projectId: "review-owner-project",
      revision: 2,
      status: "verifying",
      activeReviewReworkId: "review-owner-request",
      reviewReworks: [{ requestId: "review-owner-request", status: "repairing" }],
      updatedAt: at,
    };
    const activeState = canonicalJson(activeDocument);
    yield* sql`INSERT INTO agent_control_epic_runs(
      epic_run_id,project_id,revision,state_json,state_digest)
      VALUES ('review-owner-run','review-owner-project',2,${activeState},'fixture')`;
    yield* sql`INSERT INTO agent_control_epic_review_requests(
      request_id,project_id,epic_run_id,idempotency_key,command_id,request_digest,request_json,
      reviewed_commit_sha,reviewed_verification_evidence_id,accepted_revision,accepted_at)
      VALUES ('review-owner-request','review-owner-project','review-owner-run','key','request-command',
        'digest','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','proof',2,${at})`;
    const commandId = CommandId.make("review-owner-turn");
    yield* sql`INSERT INTO agent_control_epic_review_repair_intents(
      request_id,attempt,intent_json,intent_digest,provider_instance_id,model,runtime_mode,
      thread_id,turn_request_command_id,message_id,worktree_path,branch_name,created_at)
      VALUES ('review-owner-request',1,'{}','intent','provider','model','approval-required',
        'review-owner-thread',${commandId},'review-owner-message','/tmp/review-owner',
        't3auto/review-owner',${at})`;

    const available = yield* loadEpicReviewRepairTurnOwnership(sql, commandId);
    assert.isNotNull(available);
    assert.isTrue(available!.mayStart);

    const stoppedState = canonicalJson({
      ...activeDocument,
      revision: 3,
      status: "stopped",
      activeReviewReworkId: null,
    });
    yield* sql`UPDATE agent_control_epic_runs
      SET revision=3,state_json=${stoppedState},state_digest='stopped-fixture'
      WHERE epic_run_id='review-owner-run'`;

    const cancelled = yield* loadEpicReviewRepairTurnOwnership(sql, commandId);
    assert.isNotNull(cancelled);
    assert.isFalse(cancelled!.mayStart);
    assert.deepEqual(
      yield* sql`SELECT request_id AS "requestId",attempt
        FROM agent_control_epic_review_repair_cancellations`,
      [{ requestId: "review-owner-request", attempt: 1 }],
    );
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
