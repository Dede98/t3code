import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  prepareVerificationInspectionPages,
  VERIFICATION_INSPECTION_MAX_PAGES,
} from "../../provider/VerificationInspection.ts";
import { sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import type { VerificationCheckManifest } from "./checkEvidence.ts";

class InspectionPageError extends Schema.TaggedError<InspectionPageError>()("InspectionPageError", {
  message: Schema.String,
}) {}

const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const InspectionInventory = Schema.Struct({
  version: Schema.Literal("paged-inspection-v1"),
  baseCommit: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40,64}$/)),
  documentDigest: Digest,
  byteLength: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(4 * 1024 * 1024),
  ),
  pageDigests: Schema.Array(Digest).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(VERIFICATION_INSPECTION_MAX_PAGES),
  ),
});
export type InspectionInventory = typeof InspectionInventory.Type;
const decodeInventory = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Struct({ inspection: InspectionInventory })),
);
export const readInspectionInventory = (resultJson: string) => {
  const decoded = decodeInventory(resultJson);
  return decoded._tag === "Some" ? decoded.value.inspection : null;
};
export const inspectionPageId = (page: number) => `git-diff-page-${page}`;
export const inspectionPageNumber = (checkId: string) => {
  const match = /^git-diff-page-([1-9][0-9]{0,2})$/.exec(checkId);
  const page = match ? Number(match[1]) : null;
  return page !== null && page <= VERIFICATION_INSPECTION_MAX_PAGES ? page : null;
};
export const pagedInspectionBase = (codeDigest: string) =>
  /^review-v2:([a-f0-9]{40,64}):/.exec(codeDigest)?.[1] ?? null;

/** Page bodies are cached controller output, not evidence that a verifier has retrieved them. */
export const prepareInspectionInventory = Effect.fn("prepareInspectionInventory")(function* (
  sql: SqlClient.SqlClient,
  manifest: VerificationCheckManifest,
) {
  const base = pagedInspectionBase(manifest.codeDigest);
  if (!base)
    return yield* Effect.fail(
      new InspectionPageError({ message: "Missing paged inspection authority" }),
    );
  const document = yield* Effect.tryPromise({
    try: () => prepareVerificationInspectionPages(manifest.worktreePath, base),
    catch: (cause) =>
      new InspectionPageError({
        message: cause instanceof Error ? cause.message : "Complete inspection is unavailable",
      }),
  });
  const inspection: InspectionInventory = {
    version: "paged-inspection-v1",
    baseCommit: base,
    documentDigest: document.documentDigest,
    byteLength: document.byteLength,
    pageDigests: document.pageDigests,
  };
  yield* sql.withTransaction(
    Effect.forEach(
      document.pages,
      (content, index) =>
        sql`INSERT INTO agent_control_verification_inspection_pages
      (provider_delivery_id, page_number, manifest_digest, content, content_digest)
      VALUES (${manifest.providerDeliveryId}, ${index + 1}, ${manifest.manifestDigest}, ${content}, ${document.pageDigests[index]!})`,
      { discard: true },
    ),
  );
  return {
    exitCode: 0,
    stdout: `Inspection inventory for base ${base}. Document ${document.documentDigest}: ${document.byteLength} bytes, ${document.pages.length} required pages. Call t3_verification_check with {"check":"git-diff","page":N} for EVERY page 1 through ${document.pages.length}, then review the complete content before reporting a verdict. This inventory alone is not complete review evidence.`,
    stderr: "",
    inspection,
  };
});

export const loadInspectionPage = Effect.fn("loadInspectionPage")(function* (
  sql: SqlClient.SqlClient,
  manifest: VerificationCheckManifest,
  inventory: InspectionInventory,
  page: number,
) {
  const rows = yield* sql<{ content: string; digest: string; manifestDigest: string }>`
    SELECT content, content_digest AS digest, manifest_digest AS "manifestDigest"
    FROM agent_control_verification_inspection_pages
    WHERE provider_delivery_id=${manifest.providerDeliveryId} AND page_number=${page}`;
  const row = rows[0];
  if (
    !row ||
    row.manifestDigest !== manifest.manifestDigest ||
    row.digest !== inventory.pageDigests[page - 1] ||
    sha256Utf8(row.content) !== row.digest
  )
    return yield* Effect.fail(
      new InspectionPageError({ message: "The requested inspection page is missing or corrupt" }),
    );
  return { exitCode: 0, stdout: row.content, stderr: "" };
});

/** Shared server read model: an inventory receipt alone must never display as a passed inspection. */
export const inspectionProgress = Effect.fn("inspectionProgress")(function* (
  sql: SqlClient.SqlClient,
  deliveryId: string,
  manifestDigest: string,
  codeDigest: string,
  resultJson: string,
) {
  const base = pagedInspectionBase(codeDigest);
  if (!base) return null;
  const inventory = readInspectionInventory(resultJson);
  if (!inventory || inventory.baseCommit !== base)
    return {
      status: "unavailable" as const,
      detail: "Complete inspection inventory is unavailable.",
    };
  const rows = yield* sql<{
    checkId: string;
    manifestDigest: string;
    codeDigest: string;
    status: "passed" | "failed" | "unavailable" | "stale";
  }>`
    SELECT check_id AS "checkId", status, manifest_digest AS "manifestDigest", code_digest AS "codeDigest"
    FROM agent_control_verification_check_results
    WHERE provider_delivery_id=${deliveryId} AND check_id LIKE 'git-diff-page-%'`;
  let completed = 0;
  let status: "passed" | "missing" | "unavailable" | "stale" = "passed";
  for (let page = 1; page <= inventory.pageDigests.length; page++) {
    const row = rows.find((row) => row.checkId === inspectionPageId(page));
    if (
      row &&
      (row.manifestDigest !== manifestDigest ||
        row.codeDigest !== codeDigest ||
        row.status === "stale")
    )
      status = "stale";
    else if (row?.status === "passed") completed++;
    else if (row && status !== "stale") status = "unavailable";
    else if (status === "passed") status = "missing";
  }
  return {
    status,
    detail: `Inspection pages: ${completed}/${inventory.pageDigests.length} retrieved with passing evidence. Complete review requires every page.`,
  };
});
