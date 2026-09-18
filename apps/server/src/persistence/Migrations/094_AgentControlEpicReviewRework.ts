import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Immutable review requests bind every rework to the exact result a human inspected. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE main.agent_control_epic_review_requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    epic_run_id TEXT NOT NULL REFERENCES agent_control_epic_runs(epic_run_id),
    idempotency_key TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    reviewed_commit_sha TEXT NOT NULL,
    reviewed_verification_evidence_id TEXT NOT NULL,
    accepted_revision INTEGER NOT NULL CHECK (accepted_revision >= 1),
    accepted_at TEXT NOT NULL,
    UNIQUE (project_id, epic_run_id, idempotency_key)
  )`;
  yield* sql`CREATE INDEX main.agent_control_epic_review_requests_epic
    ON agent_control_epic_review_requests(epic_run_id, accepted_revision)`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_review_requests_no_update
    BEFORE UPDATE ON agent_control_epic_review_requests
    BEGIN SELECT RAISE(ABORT, 'Epic review request evidence is immutable'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_review_requests_no_delete
    BEFORE DELETE ON agent_control_epic_review_requests
    BEGIN SELECT RAISE(ABORT, 'Epic review request evidence is immutable'); END`;
  yield* sql`CREATE TABLE main.agent_control_epic_review_repair_intents (
    request_id TEXT NOT NULL REFERENCES agent_control_epic_review_requests(request_id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    intent_json TEXT NOT NULL CHECK (json_valid(intent_json)),
    intent_digest TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL,
    model TEXT NOT NULL,
    runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('full-access','approval-required')),
    thread_id TEXT NOT NULL UNIQUE,
    turn_request_command_id TEXT NOT NULL UNIQUE,
    message_id TEXT NOT NULL UNIQUE,
    worktree_path TEXT NOT NULL,
    branch_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(request_id,attempt)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_review_repair_results (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    result_json TEXT NOT NULL CHECK (json_valid(result_json)),
    result_digest TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    PRIMARY KEY(request_id,attempt),
    FOREIGN KEY(request_id,attempt)
      REFERENCES agent_control_epic_review_repair_intents(request_id,attempt)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_review_repair_delivery_claims (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    claimed_at TEXT NOT NULL,
    PRIMARY KEY(request_id,attempt),
    FOREIGN KEY(request_id,attempt)
      REFERENCES agent_control_epic_review_repair_intents(request_id,attempt)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_review_repair_delivery_receipts (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    provider_turn_id TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY(request_id,attempt),
    UNIQUE(provider_turn_id),
    FOREIGN KEY(request_id,attempt)
      REFERENCES agent_control_epic_review_repair_delivery_claims(request_id,attempt)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_review_repair_cancellations (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    cancelled_at TEXT NOT NULL,
    PRIMARY KEY(request_id,attempt),
    FOREIGN KEY(request_id,attempt)
      REFERENCES agent_control_epic_review_repair_intents(request_id,attempt)
  )`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_review_repair_cancel_on_authority_loss
    AFTER UPDATE OF state_json ON agent_control_epic_runs
    WHEN json_extract(OLD.state_json,'$.activeReviewReworkId') IS NOT NULL
      AND json_extract(NEW.state_json,'$.activeReviewReworkId') IS NOT
        json_extract(OLD.state_json,'$.activeReviewReworkId')
    BEGIN
      INSERT OR IGNORE INTO agent_control_epic_review_repair_cancellations(
        request_id,attempt,cancelled_at)
      SELECT intent.request_id,intent.attempt,
        COALESCE(json_extract(NEW.state_json,'$.updatedAt'),CURRENT_TIMESTAMP)
      FROM agent_control_epic_review_repair_intents intent
      LEFT JOIN agent_control_epic_review_repair_results result
        ON result.request_id=intent.request_id AND result.attempt=intent.attempt
      WHERE intent.request_id=json_extract(OLD.state_json,'$.activeReviewReworkId')
        AND result.request_id IS NULL;
    END`;
  for (const table of [
    "agent_control_epic_review_repair_intents",
    "agent_control_epic_review_repair_results",
    "agent_control_epic_review_repair_delivery_claims",
    "agent_control_epic_review_repair_delivery_receipts",
    "agent_control_epic_review_repair_cancellations",
  ]) {
    yield* sql.unsafe(
      `CREATE TRIGGER main.${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT, 'Epic review repair evidence is immutable'); END`,
    );
    yield* sql.unsafe(
      `CREATE TRIGGER main.${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT, 'Epic review repair evidence is immutable'); END`,
    );
  }
});
