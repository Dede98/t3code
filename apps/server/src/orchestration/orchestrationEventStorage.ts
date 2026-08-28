import {
  OrchestrationEvent,
  type OrchestrationEvent as OrchestrationEventType,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  canonicalJson,
  parseJsonStrict,
  type JsonValue,
} from "../agentControl/initialPlanning/eventEvidence.ts";
import { encodeAgentControlThreadBindingStorage } from "./agentControlThreadBindingStorage.ts";

export const ORCHESTRATION_EVENT_STORAGE_ENCODING_SCHEMA_ORDER_V1 =
  "orchestration-event-schema-order-v1";
export const ORCHESTRATION_EVENT_STORAGE_ENCODING_ALPHABETICAL_V1 =
  "orchestration-event-alphabetical-v1";

const encodeEvent = Schema.encodeUnknownSync(OrchestrationEvent);

const schemaOrderedPayload = (event: OrchestrationEventType): JsonValue => {
  const encoded = encodeEvent(event);
  if (event.type !== "thread.agent-control-bound") {
    return encoded.payload as JsonValue;
  }
  return {
    ...encoded.payload,
    binding: parseJsonStrict(encodeAgentControlThreadBindingStorage(event.payload.binding)),
  } as JsonValue;
};

export const encodeOrchestrationEventSchemaOrderStorage = (
  event: OrchestrationEventType,
): {
  readonly payloadJson: string;
  readonly metadataJson: string;
} => {
  const encoded = encodeEvent(event);
  return {
    payloadJson: JSON.stringify(schemaOrderedPayload(event)),
    metadataJson: JSON.stringify(encoded.metadata),
  };
};

export const encodeOrchestrationEventAlphabeticalStorage = (
  event: OrchestrationEventType,
): {
  readonly payloadJson: string;
  readonly metadataJson: string;
} => {
  const encoded = encodeEvent(event);
  return {
    payloadJson: canonicalJson(schemaOrderedPayload(event)),
    metadataJson: canonicalJson(encoded.metadata as JsonValue),
  };
};
