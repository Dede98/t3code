import { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { canonicalJson } from "./initialPlanning/eventEvidence.ts";

export type NativeTerminalStage = "initial-planning" | "implementation";
export interface NativeTerminalClaim {
  readonly evidence: {
    readonly handoffId: string;
    readonly handoffFingerprint: string;
    readonly providerDeliveryId: string;
    readonly threadId: string;
    readonly providerInstanceId: string;
    readonly runtimeMode: string;
  };
  readonly delivery: {
    readonly state: string;
    readonly revision: number;
    readonly providerTurnId: string | null;
    readonly terminalAt: string | null;
  };
}
export class NativeTerminalReceiptError extends Schema.TaggedError<NativeTerminalReceiptError>()(
  "NativeTerminalReceiptError",
  { operation: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
const decodeEvent = Schema.decodeUnknownEffect(ProviderRuntimeEvent);
const decodeEventJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ProviderRuntimeEvent));
const decodeJsonValue = Schema.decodeUnknownEffect(Schema.Json);
const canonicalEventJson = Effect.fn("canonicalNativeTerminalEventJson")(function* (
  event: ProviderRuntimeEvent,
) {
  return canonicalJson(yield* decodeJsonValue(event));
});
const validateBinding = (claim: NativeTerminalClaim, event: ProviderRuntimeEvent) => {
  if (event.type !== "turn.completed" && event.type !== "turn.aborted") return false;
  const state =
    event.type === "turn.aborted"
      ? null
      : event.payload.state === "completed"
        ? "completed"
        : event.payload.state === "interrupted" || event.payload.state === "cancelled"
          ? "interrupted"
          : "failed";
  return (
    event.threadId === claim.evidence.threadId &&
    event.providerInstanceId === claim.evidence.providerInstanceId &&
    event.turnId !== undefined &&
    event.turnId === claim.delivery.providerTurnId &&
    event.createdAt === claim.delivery.terminalAt &&
    (state === null
      ? claim.delivery.state === "failed" || claim.delivery.state === "interrupted"
      : state === claim.delivery.state)
  );
};

// Called inside the delivery CAS transaction, never from a projection or inferred outcome.
export const recordNativeTerminalReceipt = Effect.fn("recordNativeTerminalReceipt")(
  function* (input: {
    readonly stage: NativeTerminalStage;
    readonly claim: NativeTerminalClaim;
    readonly event: ProviderRuntimeEvent;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const event = yield* decodeEvent(input.event);
    if (!validateBinding(input.claim, event))
      return yield* new NativeTerminalReceiptError({ operation: "record-binding" });
    const { evidence, delivery } = input.claim;
    const eventJson = yield* canonicalEventJson(event);
    yield* sql`INSERT INTO agent_control_native_terminal_receipts
    (stage,handoff_id,handoff_fingerprint,provider_delivery_id,thread_id,provider_instance_id,
     provider_turn_id,runtime_mode,delivery_revision,terminal_state,terminal_at,native_event_id,event_json)
    VALUES (${input.stage},${evidence.handoffId},${evidence.handoffFingerprint},${evidence.providerDeliveryId},
     ${evidence.threadId},${evidence.providerInstanceId},${delivery.providerTurnId},${evidence.runtimeMode},
     ${delivery.revision},${delivery.state},${delivery.terminalAt},${event.eventId},${eventJson})`;
  },
);

export const loadNativeTerminalReceipt = Effect.fn("loadNativeTerminalReceipt")(function* (
  stage: NativeTerminalStage,
  claim: NativeTerminalClaim,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    handoffFingerprint: string;
    providerDeliveryId: string;
    threadId: string;
    providerInstanceId: string;
    providerTurnId: string;
    runtimeMode: string;
    deliveryRevision: number;
    terminalState: string;
    terminalAt: string;
    nativeEventId: string;
    eventJson: string;
  }>`SELECT handoff_fingerprint AS "handoffFingerprint",provider_delivery_id AS "providerDeliveryId",
     thread_id AS "threadId",provider_instance_id AS "providerInstanceId",provider_turn_id AS "providerTurnId",
     runtime_mode AS "runtimeMode",delivery_revision AS "deliveryRevision",terminal_state AS "terminalState",
     terminal_at AS "terminalAt",native_event_id AS "nativeEventId",event_json AS "eventJson"
     FROM agent_control_native_terminal_receipts WHERE stage=${stage} AND handoff_id=${claim.evidence.handoffId}`;
  const row = rows[0];
  if (row === undefined) return Option.none();
  const event = yield* decodeEventJson(row.eventJson);
  const { evidence, delivery } = claim;
  const eventJson = yield* canonicalEventJson(event);
  if (
    rows.length !== 1 ||
    row.handoffFingerprint !== evidence.handoffFingerprint ||
    row.providerDeliveryId !== evidence.providerDeliveryId ||
    row.threadId !== evidence.threadId ||
    row.providerInstanceId !== evidence.providerInstanceId ||
    row.providerTurnId !== delivery.providerTurnId ||
    row.runtimeMode !== evidence.runtimeMode ||
    row.deliveryRevision !== delivery.revision ||
    row.terminalState !== delivery.state ||
    row.terminalAt !== delivery.terminalAt ||
    row.nativeEventId !== event.eventId ||
    row.eventJson !== eventJson ||
    !validateBinding(claim, event)
  )
    return yield* new NativeTerminalReceiptError({ operation: "load-binding" });
  return Option.some(event);
});

const nativeTerminalDeliveryTable = (stage: NativeTerminalStage) =>
  `agent_control_${stage === "initial-planning" ? "initial_planning" : "implementation"}_deliveries`;

const nativeTerminalOutcomeSql = (stage: NativeTerminalStage, delivery: string) => `CASE
  WHEN json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType') = 'turn.aborted'
    THEN ${stage === "initial-planning" ? `CASE WHEN ${delivery}.interrupt_requested = 1 THEN 'interrupted' ELSE 'failed' END` : "'failed'"}
  WHEN json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.providerState') IN ('interrupted', 'cancelled') THEN 'interrupted'
  ELSE json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.providerState') END`;

const acceptedAmbiguousNativeTerminalQuery = (
  stage: NativeTerminalStage,
  delivery: string,
  loadDelivery = false,
): string => `SELECT ${nativeTerminalOutcomeSql(stage, delivery)} AS "state", terminal_turn.completed_at AS "terminalAt"
    FROM ${loadDelivery ? `${nativeTerminalDeliveryTable(stage)} ${delivery},` : ""} projection_turns terminal_turn
    JOIN orchestration_events native_terminal
      ON native_terminal.aggregate_kind = 'thread'
      AND native_terminal.stream_id = terminal_turn.thread_id
    JOIN agent_control_${stage === "initial-planning" ? "initial_planning" : "implementation"}_handoff_intents native_intent
      ON native_intent.handoff_id = ${delivery}.handoff_id
    WHERE ${delivery}.provider_turn_id IS NOT NULL AND ${delivery}.provider_accepted_at IS NOT NULL
      ${loadDelivery ? `AND ${delivery}.handoff_id = ? AND ${delivery}.state = 'ambiguous'` : ""}
      AND terminal_turn.thread_id = ${delivery}.thread_id
      AND terminal_turn.turn_id = ${delivery}.provider_turn_id
      AND terminal_turn.completed_at >= ${delivery}.provider_accepted_at
      AND strftime('%Y-%m-%dT%H:%M:%fZ', terminal_turn.completed_at) = terminal_turn.completed_at
      AND native_terminal.event_type = 'thread.session-set'
      AND native_terminal.actor_kind = 'provider'
      AND substr(native_terminal.command_id, 1, 9) = 'provider:'
      AND native_terminal.occurred_at = terminal_turn.completed_at
      AND json_extract(native_terminal.payload_json, '$.threadId') = ${delivery}.thread_id
      AND json_extract(native_terminal.payload_json, '$.session.threadId') = ${delivery}.thread_id
      AND json_extract(native_terminal.payload_json, '$.session.providerInstanceId') = ${delivery}.provider_instance_id
      AND json_extract(native_terminal.payload_json, '$.session.providerInstanceId') = native_intent.provider_instance_id
      AND json_extract(native_terminal.payload_json, '$.session.runtimeMode') = native_intent.runtime_mode
      AND json_type(native_terminal.payload_json, '$.session.activeTurnId') = 'null'
      AND json_extract(native_terminal.payload_json, '$.session.updatedAt') = terminal_turn.completed_at
      AND json_type(native_terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId') = 'text'
      AND length(json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId')) > 0
      AND json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.providerTurnId') = ${delivery}.provider_turn_id
      AND json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.providerInstanceId') = ${delivery}.provider_instance_id
      AND (
        (json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType') = 'turn.completed'
          AND json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.providerState') IN ('completed', 'interrupted', 'cancelled')
          AND json_extract(native_terminal.payload_json, '$.session.status') = 'ready'
          AND terminal_turn.state IN ('completed', 'interrupted'))
        OR
        ((json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType') = 'turn.aborted'
          OR (json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType') = 'turn.completed'
            AND json_extract(native_terminal.metadata_json, '$.providerRuntimeLifecycle.providerState') = 'failed'))
          AND json_extract(native_terminal.payload_json, '$.session.status') = 'error'
          AND terminal_turn.state = 'error')
      )`;

/** Candidate selection and delivery CAS require the same native authority, never projection alone. */
export const acceptedAmbiguousNativeTerminalPredicate = (
  stage: NativeTerminalStage,
  delivery: string,
  matchObservation = false,
): string =>
  `(SELECT count(*) = 1 ${matchObservation ? 'AND MAX("terminalAt") = ? AND MAX("state") = ?' : ""}
    FROM (${acceptedAmbiguousNativeTerminalQuery(stage, delivery)}))`;

const decodeRecoveredTerminal = Schema.decodeUnknownEffect(
  Schema.Struct({
    state: Schema.Literals(["completed", "failed", "interrupted"]),
    terminalAt: Schema.String,
  }),
);

export const loadAcceptedAmbiguousNativeTerminal = Effect.fn("loadAcceptedAmbiguousNativeTerminal")(
  function* (stage: NativeTerminalStage, handoffId: string) {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe(acceptedAmbiguousNativeTerminalQuery(stage, "delivery", true), [
      handoffId,
    ]);
    if (rows.length === 0) return Option.none();
    if (rows.length !== 1)
      return yield* new NativeTerminalReceiptError({
        operation: "ambiguous-native-terminal-evidence",
      });
    return Option.some(yield* decodeRecoveredTerminal(rows[0]));
  },
);
