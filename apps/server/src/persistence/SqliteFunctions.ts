import {
  verificationResultCompletionDetailDigest,
  verificationResultDeltaTextDigest,
  verificationResultOutputEvidenceDigest,
} from "../agentControl/verificationTurn/runtimeEvidence.ts";

export const SQLITE_FATAL_UTF8_FUNCTION = "t3_fatal_utf8";
export const SQLITE_VERIFICATION_DELTA_DIGEST_FUNCTION = "t3_verification_delta_digest";
export const SQLITE_VERIFICATION_COMPLETION_DIGEST_FUNCTION = "t3_verification_completion_digest";
export const SQLITE_VERIFICATION_EVIDENCE_DIGEST_FUNCTION = "t3_verification_evidence_digest";

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
