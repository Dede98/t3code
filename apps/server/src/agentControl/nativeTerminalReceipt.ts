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
