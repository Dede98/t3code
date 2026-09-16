import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

// This projection deliberately sits beside the existing provider authority.
// Existing autonomous admissions keep their complete evidence/fence history;
// migration only starts coordinating new starts across manual and automatic work.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE main.resource_admission_provider_scopes (
    account_scope TEXT PRIMARY KEY NOT NULL CHECK(length(account_scope)>0),
    max_concurrent INTEGER NOT NULL CHECK(max_concurrent BETWEEN 1 AND 64),
    interactive_reserve INTEGER NOT NULL CHECK(interactive_reserve BETWEEN 0 AND max_concurrent),
    background_aging_ms INTEGER NOT NULL CHECK(background_aging_ms BETWEEN 0 AND 86400000),
    max_interactive_burst INTEGER NOT NULL CHECK(max_interactive_burst BETWEEN 1 AND 1024),
    consecutive_interactive_grants INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_interactive_grants>=0),
    last_fence_token INTEGER NOT NULL DEFAULT 0 CHECK(last_fence_token>=0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>=1),
    updated_at TEXT NOT NULL
  ) STRICT`;
  yield* sql`CREATE TABLE main.resource_admission_provider_requests (
    request_id TEXT PRIMARY KEY NOT NULL CHECK(length(request_id)>64),
    idempotency_key TEXT UNIQUE NOT NULL CHECK(length(idempotency_key)>0),
    request_fingerprint TEXT UNIQUE NOT NULL CHECK(length(request_fingerprint)=64),
    provider_instance_id TEXT NOT NULL CHECK(length(provider_instance_id)>0),
    thread_id TEXT NOT NULL CHECK(length(thread_id)>0),
    account_scope TEXT NOT NULL REFERENCES resource_admission_provider_scopes(account_scope),
    workload_class TEXT NOT NULL CHECK(workload_class IN ('interactive','background')),
    source TEXT NOT NULL CHECK(source IN ('manual','automatic')),
    stage TEXT CHECK(stage IS NULL OR stage IN ('initial-planning','implementation','verification')),
    handoff_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('waiting','admitted','entered','released','cancelled')),
    wait_reason TEXT CHECK(wait_reason IS NULL OR wait_reason IN ('provider-limit','provider-usage','provider-recovery','interactive-priority')),
    usage_status TEXT NOT NULL CHECK(usage_status IN ('allowed','warning','rejected','unsupported','supported-unusable')),
    requested_at TEXT NOT NULL,
    aging_deadline_at TEXT,
    next_deadline_at TEXT,
    owner_id TEXT,
    lease_expires_at TEXT,
    fence_token INTEGER,
    entered_at TEXT,
    provider_turn_id TEXT,
    completed_at TEXT,
    last_observed_activity TEXT CHECK(last_observed_activity IS NULL OR last_observed_activity IN ('active','inactive','unknown')),
    last_observed_at TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>=1),
    updated_at TEXT NOT NULL,
    CHECK((source='automatic' AND stage IS NOT NULL AND handoff_id IS NOT NULL)
      OR (source='manual' AND stage IS NULL AND handoff_id IS NULL)),
    CHECK((status='waiting' AND owner_id IS NULL AND lease_expires_at IS NULL AND fence_token IS NULL AND entered_at IS NULL AND completed_at IS NULL)
      OR (status='admitted' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL AND fence_token>=1 AND entered_at IS NULL AND completed_at IS NULL)
      OR (status='entered' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL AND fence_token>=1 AND entered_at IS NOT NULL AND completed_at IS NULL)
      OR (status IN ('released','cancelled') AND completed_at IS NOT NULL))
  ) STRICT`;
  yield* sql`CREATE INDEX main.idx_resource_admission_provider_queue
    ON resource_admission_provider_requests(account_scope,status,workload_class,requested_at,request_id)`;
  yield* sql`CREATE INDEX main.idx_resource_admission_provider_active
    ON resource_admission_provider_requests(account_scope,status,source,provider_instance_id)`;
  yield* sql`CREATE INDEX main.idx_resource_admission_provider_deadline
    ON resource_admission_provider_requests(status,lease_expires_at,account_scope,request_id)`;
  yield* sql`CREATE TRIGGER main.resource_admission_provider_requests_no_delete
    BEFORE DELETE ON resource_admission_provider_requests
    BEGIN SELECT RAISE(ABORT,'resource admission requests cannot be deleted'); END`;
  yield* sql`CREATE TRIGGER main.resource_admission_provider_requests_identity_guard
    BEFORE UPDATE ON resource_admission_provider_requests
    WHEN NEW.request_id!=OLD.request_id OR NEW.idempotency_key!=OLD.idempotency_key
      OR NEW.request_fingerprint!=OLD.request_fingerprint
      OR NEW.provider_instance_id!=OLD.provider_instance_id
      OR NEW.thread_id!=OLD.thread_id
      OR NEW.account_scope!=OLD.account_scope OR NEW.workload_class!=OLD.workload_class
      OR NEW.source!=OLD.source OR NEW.stage IS NOT OLD.stage OR NEW.handoff_id IS NOT OLD.handoff_id
      OR NEW.requested_at!=OLD.requested_at OR NEW.revision!=OLD.revision+1
    BEGIN SELECT RAISE(ABORT,'resource admission request identity is immutable'); END`;
  yield* sql`CREATE TABLE main.resource_admission_wait_status (
    request_id TEXT PRIMARY KEY NOT NULL,
    handoff_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN (
      'provider-limit','local-capacity','cpu-pressure','ram-pressure','gpu-pressure',
      'interactive-priority','telemetry-unavailable','unsupported-requirement'
    )),
    detail TEXT,
    updated_at TEXT NOT NULL
  ) STRICT`;
  yield* sql`CREATE INDEX main.idx_resource_admission_wait_handoff
    ON resource_admission_wait_status(handoff_id,updated_at)`;
});
