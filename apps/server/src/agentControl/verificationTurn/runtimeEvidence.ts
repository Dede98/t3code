import * as NodeCrypto from "node:crypto";

const DELTA_TEXT_DIGEST_DOMAIN = "t3/agent-control/verification-result/delta-text/v1";
const COMPLETION_DETAIL_DIGEST_DOMAIN = "t3/agent-control/verification-result/completion-detail/v1";
const OUTPUT_EVIDENCE_GENESIS_DOMAIN =
  "t3/agent-control/verification-result/output-evidence-genesis/v1";
const OUTPUT_EVIDENCE_CHAIN_DOMAIN =
  "t3/agent-control/verification-result/output-evidence-chain/v1";

const updateFramedUtf8 = (hash: NodeCrypto.Hash, label: string, value: string | number): void => {
  const encoded = String(value);
  hash.update(`${label}:${Buffer.byteLength(encoded, "utf8")}:`, "utf8");
  hash.update(encoded, "utf8");
  hash.update(";", "utf8");
};

const digestFullUtf8Text = (domain: string, value: string): string => {
  const hash = NodeCrypto.createHash("sha256");
  updateFramedUtf8(hash, "domain", domain);
  updateFramedUtf8(hash, "utf8ByteLength", Buffer.byteLength(value, "utf8"));
  hash.update(value, "utf8");
  return hash.digest("hex");
};

/** Hashes the complete JS string incrementally without materializing a full byte array. */
export const verificationResultDeltaTextDigest = (value: string): string =>
  digestFullUtf8Text(DELTA_TEXT_DIGEST_DOMAIN, value);

/** Presence is represented outside this digest, so empty and absent remain distinct. */
export const verificationResultCompletionDetailDigest = (value: string): string =>
  digestFullUtf8Text(COMPLETION_DETAIL_DIGEST_DOMAIN, value);

export const VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS = digestFullUtf8Text(
  OUTPUT_EVIDENCE_GENESIS_DOMAIN,
  "",
);

/**
 * Sequence evidence over fragment digests. This is not SHA-256 of concatenated
 * raw output and must never be exposed as such.
 */
export const verificationResultOutputEvidenceDigest = (input: {
  readonly previousDigest: string;
  readonly fragmentKind: "delta" | "completion";
  readonly fragmentOrdinal: number;
  readonly fullByteLength: number | null;
  readonly fullDigest: string | null;
  readonly detailPresent: boolean;
}): string => {
  const hash = NodeCrypto.createHash("sha256");
  updateFramedUtf8(hash, "domain", OUTPUT_EVIDENCE_CHAIN_DOMAIN);
  updateFramedUtf8(hash, "previousDigest", input.previousDigest);
  updateFramedUtf8(hash, "fragmentKind", input.fragmentKind);
  updateFramedUtf8(hash, "fragmentOrdinal", input.fragmentOrdinal);
  updateFramedUtf8(hash, "detailPresent", input.detailPresent ? 1 : 0);
  updateFramedUtf8(hash, "fullByteLength", input.fullByteLength ?? "absent");
  updateFramedUtf8(hash, "fullDigest", input.fullDigest ?? "absent");
  return hash.digest("hex");
};
