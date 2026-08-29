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
  providerRuntimeEventMatchesVerificationResultFragment,
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

const routeEncoder = new TextEncoder();
const routeBytes = (tag: number, identifier = ""): Uint8Array => {
  const encoded = routeEncoder.encode(identifier);
  const result = new Uint8Array(encoded.byteLength + 1);
  result[0] = tag;
  result.set(encoded, 1);
  return result;
};

export const ORCHESTRATION_EVENT_ROUTE_INVALID = routeBytes(0);
export const ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_NONE = routeBytes(3);
export const orchestrationEventAuthorityRouteBytes = (
  aggregateKind: "project" | "thread",
  aggregateId: string,
): Uint8Array => routeBytes(aggregateKind === "project" ? 1 : 2, aggregateId);
export const orchestrationEventProjectMembershipRouteBytes = (projectId: string): Uint8Array =>
  routeBytes(4, projectId);

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

const validateVerificationResultCaptureAuthority = (event: OrchestrationEventType): void => {
  const capture = event.metadata.verificationResultCapture;
  if (capture === undefined) return;
  const runtime = event.metadata.providerRuntimeMessage;
  if (
    runtime === undefined ||
    capture.providerInstanceId !== runtime.providerInstanceId ||
    capture.providerTurnId !== runtime.providerTurnId
  ) {
    throw new Error("Verification result capture has no matching runtime authority");
  }
  if (event.type === "thread.message-sent") {
    if (
      capture.disposition !== "presentation" ||
      event.payload.role !== "assistant" ||
      event.payload.turnId !== runtime.providerTurnId
    ) {
      throw new Error("Verification presentation capture is not bound to an assistant message");
    }
    return;
  }
  if (event.type === "thread.verification-result-fragment-captured") {
    const runtimeEventMatches = providerRuntimeEventMatchesVerificationResultFragment(
      event.payload.fragment.kind,
      runtime.eventType,
    );
    if (
      capture.disposition !== "authority" ||
      event.payload.turnId !== runtime.providerTurnId ||
      !runtimeEventMatches
    ) {
      throw new Error("Verification authority capture is not bound to a result fragment");
    }
    return;
  }
  throw new Error("Verification result capture is not valid for this event type");
};

const authorityRoute = (
  event: OrchestrationEventType,
): { readonly aggregateKind: "project" | "thread"; readonly aggregateId: string } => {
  const aggregateKind = event.type.startsWith("project.") ? "project" : "thread";
  const payload = event.payload as { readonly projectId?: unknown; readonly threadId?: unknown };
  const aggregateId = aggregateKind === "project" ? payload.projectId : payload.threadId;
  if (typeof aggregateId !== "string") {
    throw new Error("Orchestration payload has no authority routing identifier");
  }
  return { aggregateKind, aggregateId };
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
  validateVerificationResultCaptureAuthority(event);
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

export const classifyOrchestrationEventAuthorityRoute = (input: {
  readonly eventType: unknown;
  readonly payload: PersistedOrchestrationJson;
  readonly metadata: PersistedOrchestrationMetadata;
}): Uint8Array => {
  try {
    const decoded = decodeOrchestrationEventJsonStorage(input);
    const event = decodeEvent({
      sequence: 1,
      eventId: "orchestration-route-classifier-event",
      aggregateKind:
        typeof input.eventType === "string" && input.eventType.startsWith("project.")
          ? "project"
          : "thread",
      aggregateId: "orchestration-route-classifier-stream",
      type: input.eventType,
      occurredAt: "1970-01-01T00:00:00.000Z",
      commandId: null,
      causationEventId: null,
      correlationId: null,
      payload: decoded.payload,
      metadata: decoded.metadata,
    });
    const route = authorityRoute(event);
    return orchestrationEventAuthorityRouteBytes(route.aggregateKind, route.aggregateId);
  } catch {
    return ORCHESTRATION_EVENT_ROUTE_INVALID;
  }
};

export const classifyOrchestrationEventProjectMembershipRoute = (input: {
  readonly eventType: unknown;
  readonly payload: PersistedOrchestrationJson;
  readonly metadata: PersistedOrchestrationMetadata;
}): Uint8Array => {
  try {
    const decoded = decodeOrchestrationEventJsonStorage(input);
    if (input.eventType !== "thread.created") {
      return ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_NONE;
    }
    const payload = decoded.payload as { readonly projectId?: unknown };
    if (typeof payload.projectId !== "string") return ORCHESTRATION_EVENT_ROUTE_INVALID;
    return orchestrationEventProjectMembershipRouteBytes(payload.projectId);
  } catch {
    return ORCHESTRATION_EVENT_ROUTE_INVALID;
  }
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
