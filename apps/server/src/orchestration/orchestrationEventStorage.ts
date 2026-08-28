import {
  OrchestrationEvent,
  type OrchestrationEvent as OrchestrationEventType,
  type OrchestrationEventMetadata,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  type JsonValue,
} from "../agentControl/initialPlanning/eventEvidence.ts";
import { encodeAgentControlThreadBindingStorage } from "./agentControlThreadBindingStorage.ts";
import {
  classifyPersistedOrchestrationMetadata,
  type OrchestrationMetadataStorageEncoding,
  type PersistedOrchestrationMetadata,
} from "./providerRuntimeMessageCorrelation.ts";

export const ORCHESTRATION_EVENT_STORAGE_ENCODING_SCHEMA_ORDER_V1 =
  "orchestration-event-schema-order-v1";
export const ORCHESTRATION_EVENT_STORAGE_ENCODING_ALPHABETICAL_V1 =
  "orchestration-event-alphabetical-v1";

const encodeEvent = Schema.encodeUnknownSync(OrchestrationEvent);
const decodeEvent = Schema.decodeUnknownSync(OrchestrationEvent);

export interface PersistedOrchestrationJson {
  readonly storageClass: unknown;
  readonly text: unknown;
  readonly bytes: unknown;
}

export interface DecodedOrchestrationEventJsonStorage {
  readonly payloadSource: string;
  readonly metadataSource: string;
  readonly payload: OrchestrationEventType["payload"];
  readonly metadata: OrchestrationEventMetadata;
  readonly payloadEncoding:
    | typeof ORCHESTRATION_EVENT_STORAGE_ENCODING_SCHEMA_ORDER_V1
    | typeof ORCHESTRATION_EVENT_STORAGE_ENCODING_ALPHABETICAL_V1;
  readonly metadataEncoding: OrchestrationMetadataStorageEncoding;
}

const persistedTextSource = (input: PersistedOrchestrationJson, column: string): string => {
  if (input.storageClass !== "text" || typeof input.text !== "string") {
    throw new Error(`Invalid orchestration ${column} SQLite storage class`);
  }
  const source = decodeCanonicalUtf8Bytes(input.bytes);
  if (source !== input.text) {
    throw new Error(`Orchestration ${column} TEXT/BLOB mismatch`);
  }
  if (source.includes("\0")) {
    throw new Error(`NUL is not valid in persisted orchestration ${column}`);
  }
  return source;
};

const containsNul = (value: unknown): boolean => {
  if (typeof value === "string") return value.includes("\0");
  if (Array.isArray(value)) return value.some(containsNul);
  if (typeof value !== "object" || value === null) return false;
  return Object.entries(value).some(([key, child]) => key.includes("\0") || containsNul(child));
};

/**
 * The one closed payload/metadata encoding classifier shared by raw reads and
 * SQLite's migration-060 write boundary.
 */
export const decodeOrchestrationEventJsonStorage = (input: {
  readonly eventType: unknown;
  readonly payload: PersistedOrchestrationJson;
  readonly metadata: PersistedOrchestrationMetadata;
}): DecodedOrchestrationEventJsonStorage => {
  if (typeof input.eventType !== "string" || input.eventType.includes("\0")) {
    throw new Error("Invalid orchestration event type for JSON storage");
  }
  const payloadSource = persistedTextSource(input.payload, "payload");
  const payload = parseJsonStrict(payloadSource);
  if (containsNul(payload)) {
    throw new Error("NUL is not valid in persisted orchestration payload values");
  }
  const decodedMetadata = classifyPersistedOrchestrationMetadata(input.metadata);
  const aggregateKind = input.eventType.startsWith("project.") ? "project" : "thread";
  const event = decodeEvent({
    sequence: 1,
    eventId: "orchestration-storage-classifier-event",
    aggregateKind,
    aggregateId: "orchestration-storage-classifier-stream",
    type: input.eventType,
    occurredAt: "1970-01-01T00:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    payload,
    metadata: decodedMetadata.value,
  });
  const schemaOrder = encodeOrchestrationEventSchemaOrderStorage(event).payloadJson;
  const alphabetical = encodeOrchestrationEventAlphabeticalStorage(event).payloadJson;
  const payloadEncoding =
    payloadSource === schemaOrder
      ? ORCHESTRATION_EVENT_STORAGE_ENCODING_SCHEMA_ORDER_V1
      : payloadSource === alphabetical
        ? ORCHESTRATION_EVENT_STORAGE_ENCODING_ALPHABETICAL_V1
        : undefined;
  if (payloadEncoding === undefined) {
    throw new Error("Persisted orchestration payload was transformed by schema decoding");
  }
  return {
    payloadSource,
    metadataSource: decodedMetadata.source,
    payload: event.payload,
    metadata: decodedMetadata.value,
    payloadEncoding,
    metadataEncoding: decodedMetadata.encoding,
  };
};

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
