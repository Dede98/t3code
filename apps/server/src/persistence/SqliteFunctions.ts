import {
  verificationResultCompletionDetailDigest,
  verificationResultDeltaTextDigest,
  verificationResultOutputEvidenceDigest,
} from "../agentControl/verificationTurn/runtimeEvidence.ts";
import { decodeCanonicalUtf8Bytes } from "../agentControl/initialPlanning/eventEvidence.ts";
import {
  classifyOrchestrationEventAuthorityRoute,
  classifyOrchestrationEventProjectMembershipRoute,
  decodeOrchestrationEventJsonStorage,
  ORCHESTRATION_EVENT_ROUTE_INVALID,
} from "../orchestration/orchestrationEventStorage.ts";

export const SQLITE_FATAL_UTF8_FUNCTION = "t3_fatal_utf8";
export const SQLITE_VERIFICATION_DELTA_DIGEST_FUNCTION = "t3_verification_delta_digest";
export const SQLITE_VERIFICATION_COMPLETION_DIGEST_FUNCTION = "t3_verification_completion_digest";
export const SQLITE_VERIFICATION_EVIDENCE_DIGEST_FUNCTION = "t3_verification_evidence_digest";
export const SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION =
  "t3_orchestration_event_json_storage";
export const SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FUNCTION =
  "t3_orchestration_event_json_storage_protocol";
export const SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION =
  "t3_orchestration_event_authority_route";
export const SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION =
  "t3_orchestration_event_project_membership_route";
export const ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FINGERPRINT =
  "c708e067981805840f3a22a52e3507b3a98f3fca9a30c285da81eea3b6ab0dbd";
export const ORCHESTRATION_EVENT_ROUTE_PROTOCOL_EVENT_TYPE =
  "t3.orchestration-event-route.protocol/v1";
export const ORCHESTRATION_EVENT_AUTHORITY_ROUTE_PROTOCOL_PAYLOAD = "authority-route-callback";
export const ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_PROTOCOL_PAYLOAD =
  "project-membership-route-callback";

const routeProtocolResult = (kind: "authority" | "project-membership"): Uint8Array =>
  new TextEncoder().encode(`${kind}:${ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FINGERPRINT}`);

export const ORCHESTRATION_EVENT_AUTHORITY_ROUTE_PROTOCOL_RESULT = routeProtocolResult("authority");
export const ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_PROTOCOL_RESULT =
  routeProtocolResult("project-membership");

const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const utf8Encoder = new TextEncoder();

/** Pure implementation shared by every registration of the durable SQLite UTF-8 guard. */
export const isFatalUtf8Blob = (value: unknown): 0 | 1 => {
  if (!(value instanceof Uint8Array)) return 0;

  try {
    const decoded = fatalUtf8Decoder.decode(value);
    const roundTrip = utf8Encoder.encode(decoded);
    if (roundTrip.byteLength !== value.byteLength) return 0;
    for (let index = 0; index < value.byteLength; index += 1) {
      if (roundTrip[index] !== value[index]) return 0;
    }
    return 1;
  } catch {
    return 0;
  }
};

/** SQLite adapter for the shared closed orchestration JSON storage classifier. */
export const sqliteOrchestrationEventJsonStorage = (
  eventTypeBytes: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
): 0 | 1 => {
  try {
    const eventType = decodeCanonicalUtf8Bytes(eventTypeBytes);
    const payloadText = decodeCanonicalUtf8Bytes(payloadBytes);
    const metadataText = decodeCanonicalUtf8Bytes(metadataBytes);
    decodeOrchestrationEventJsonStorage({
      eventType,
      payload: { storageClass: "text", text: payloadText, bytes: payloadBytes },
      metadata: { storageClass: "text", text: metadataText, bytes: metadataBytes },
    });
    return 1;
  } catch {
    return 0;
  }
};

export const sqliteOrchestrationEventJsonStorageProtocol = (): string =>
  ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FINGERPRINT;

const sqliteOrchestrationRouteInput = (
  eventTypeBytes: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
) => {
  const eventType = decodeCanonicalUtf8Bytes(eventTypeBytes);
  const payloadText = decodeCanonicalUtf8Bytes(payloadBytes);
  const metadataText = decodeCanonicalUtf8Bytes(metadataBytes);
  return {
    eventType,
    payload: { storageClass: "text", text: payloadText, bytes: payloadBytes },
    metadata: { storageClass: "text", text: metadataText, bytes: metadataBytes },
  } as const;
};

const sqliteOrchestrationRouteProtocolProbe = (
  eventTypeBytes: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
  payload: string,
): boolean => {
  try {
    return (
      decodeCanonicalUtf8Bytes(eventTypeBytes) === ORCHESTRATION_EVENT_ROUTE_PROTOCOL_EVENT_TYPE &&
      decodeCanonicalUtf8Bytes(payloadBytes) === payload &&
      decodeCanonicalUtf8Bytes(metadataBytes) ===
        ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FINGERPRINT
    );
  } catch {
    return false;
  }
};

export const sqliteOrchestrationEventAuthorityRoute = (
  eventTypeBytes: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
): Uint8Array => {
  try {
    if (
      sqliteOrchestrationRouteProtocolProbe(
        eventTypeBytes,
        payloadBytes,
        metadataBytes,
        ORCHESTRATION_EVENT_AUTHORITY_ROUTE_PROTOCOL_PAYLOAD,
      )
    ) {
      return ORCHESTRATION_EVENT_AUTHORITY_ROUTE_PROTOCOL_RESULT;
    }
    return classifyOrchestrationEventAuthorityRoute(
      sqliteOrchestrationRouteInput(eventTypeBytes, payloadBytes, metadataBytes),
    );
  } catch {
    return ORCHESTRATION_EVENT_ROUTE_INVALID;
  }
};

export const sqliteOrchestrationEventProjectMembershipRoute = (
  eventTypeBytes: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
): Uint8Array => {
  try {
    if (
      sqliteOrchestrationRouteProtocolProbe(
        eventTypeBytes,
        payloadBytes,
        metadataBytes,
        ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_PROTOCOL_PAYLOAD,
      )
    ) {
      return ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_PROTOCOL_RESULT;
    }
    return classifyOrchestrationEventProjectMembershipRoute(
      sqliteOrchestrationRouteInput(eventTypeBytes, payloadBytes, metadataBytes),
    );
  } catch {
    return ORCHESTRATION_EVENT_ROUTE_INVALID;
  }
};

const lowercaseSha256 = /^[0-9a-f]{64}$/u;
const safeInteger = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(value);
  }
  return null;
};

export const sqliteVerificationDeltaDigest = (value: unknown): string | null =>
  typeof value === "string" ? verificationResultDeltaTextDigest(value) : null;

export const sqliteVerificationCompletionDigest = (value: unknown): string | null =>
  typeof value === "string" ? verificationResultCompletionDetailDigest(value) : null;

export const sqliteVerificationEvidenceDigest = (
  previousDigest: unknown,
  fragmentKind: unknown,
  fragmentOrdinal: unknown,
  detailPresent: unknown,
  fullByteLength: unknown,
  fullDigest: unknown,
): string | null => {
  const ordinal = safeInteger(fragmentOrdinal);
  const present = safeInteger(detailPresent);
  const length = fullByteLength === null ? null : safeInteger(fullByteLength);
  if (
    typeof previousDigest !== "string" ||
    !lowercaseSha256.test(previousDigest) ||
    (fragmentKind !== "delta" && fragmentKind !== "completion") ||
    ordinal === null ||
    ordinal < 1 ||
    (present !== 0 && present !== 1) ||
    (fullDigest !== null &&
      (typeof fullDigest !== "string" || !lowercaseSha256.test(fullDigest))) ||
    (present === 0 && (length !== null || fullDigest !== null)) ||
    (present === 1 && (length === null || fullDigest === null)) ||
    (fragmentKind === "delta" && present !== 1)
  ) {
    return null;
  }
  return verificationResultOutputEvidenceDigest({
    previousDigest,
    fragmentKind,
    fragmentOrdinal: ordinal,
    fullByteLength: length,
    fullDigest,
    detailPresent: present === 1,
  });
};
