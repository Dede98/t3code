import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_native_terminal_receipts (
    stage TEXT NOT NULL CHECK(stage IN ('initial-planning','implementation')),
    handoff_id TEXT NOT NULL,
    handoff_fingerprint TEXT NOT NULL,
    provider_delivery_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL,
    provider_turn_id TEXT NOT NULL,
    runtime_mode TEXT NOT NULL,
    delivery_revision INTEGER NOT NULL CHECK(delivery_revision > 0),
    terminal_state TEXT NOT NULL CHECK(terminal_state IN ('completed','failed','interrupted')),
    terminal_at TEXT NOT NULL,
    native_event_id TEXT NOT NULL,
    event_json TEXT NOT NULL CHECK(json_valid(event_json)),
    PRIMARY KEY(stage,handoff_id), UNIQUE(stage,provider_delivery_id), UNIQUE(stage,native_event_id)
  )`;
  for (const operation of ["UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`CREATE TRIGGER agent_control_native_terminal_receipts_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_native_terminal_receipts
      BEGIN SELECT RAISE(ABORT,'native terminal receipt is immutable'); END`).unprepared;
  }
  const stages = [
    ["initial-planning", "agent_control_initial_planning_deliveries"],
    ["implementation", "agent_control_implementation_deliveries"],
  ] as const;
  const deliveryBindings = stages
    .map(
      ([stage, table]) => `(NEW.stage='${stage}' AND EXISTS (
    SELECT 1 FROM ${table} delivery
    JOIN ${table.replace(/_deliveries$/, "_handoff_intents")} intent ON intent.handoff_id=delivery.handoff_id
    JOIN ${table.replace(/_deliveries$/, "_delivery_attestations")} attestation ON attestation.provider_delivery_id=delivery.provider_delivery_id
    WHERE delivery.handoff_id=NEW.handoff_id
      AND intent.handoff_fingerprint=NEW.handoff_fingerprint AND intent.runtime_mode=NEW.runtime_mode
      AND delivery.provider_instance_id=NEW.provider_instance_id
      AND attestation.provider_instance_id=NEW.provider_instance_id
      AND delivery.provider_delivery_id=NEW.provider_delivery_id AND delivery.thread_id=NEW.thread_id
      AND delivery.provider_turn_id=NEW.provider_turn_id AND delivery.revision=NEW.delivery_revision
      AND delivery.state=NEW.terminal_state AND delivery.terminal_at=NEW.terminal_at))`,
    )
    .join(" OR ");
  yield* sql.unsafe(`CREATE TRIGGER agent_control_native_terminal_receipts_validate
    BEFORE INSERT ON agent_control_native_terminal_receipts WHEN NOT COALESCE((
      (${deliveryBindings}) AND json_type(NEW.event_json)='object'
      AND json_extract(NEW.event_json,'$.eventId') IS NEW.native_event_id
      AND json_extract(NEW.event_json,'$.threadId') IS NEW.thread_id
      AND json_extract(NEW.event_json,'$.providerInstanceId') IS NEW.provider_instance_id
      AND json_extract(NEW.event_json,'$.turnId') IS NEW.provider_turn_id
      AND json_extract(NEW.event_json,'$.createdAt') IS NEW.terminal_at
      AND ((json_extract(NEW.event_json,'$.type')='turn.aborted' AND NEW.terminal_state IN ('failed','interrupted'))
        OR (json_extract(NEW.event_json,'$.type')='turn.completed'
          AND json_extract(NEW.event_json,'$.payload.state') IN ('completed','failed','interrupted','cancelled')
          AND NEW.terminal_state=CASE json_extract(NEW.event_json,'$.payload.state')
            WHEN 'completed' THEN 'completed' WHEN 'interrupted' THEN 'interrupted'
            WHEN 'cancelled' THEN 'interrupted' ELSE 'failed' END))
    ),0) BEGIN SELECT RAISE(ABORT,'invalid native terminal receipt'); END`).unprepared;
});
