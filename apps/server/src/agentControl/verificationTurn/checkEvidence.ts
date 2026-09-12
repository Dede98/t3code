import { AgentControlRunOnceReadNotifications } from "../runOnce/readNotifications.ts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { AgentControlVerificationChecks, AgentControlProjectPolicy } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { isSqlError } from "effect/unstable/sql/SqlError";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import type { ProviderAdmissionPermit } from "../providerAdmission/model.ts";
import { classifyVerificationCheckResult } from "../../provider/CodexVerificationChecks.ts";

const decodeChecks = Schema.decodeUnknownEffect(AgentControlVerificationChecks);
const decodeChecksJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlVerificationChecks),
);
const decodePolicyJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlProjectPolicy),
);

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const now = Effect.map(DateTime.now, DateTime.formatIso);
export class VerificationCheckError extends Schema.TaggedError<VerificationCheckError>()(
  "VerificationCheckError",
  { cause: Schema.Unknown },
) {}
const failure = (cause: unknown) => new VerificationCheckError({ cause });
const readVerificationSnapshot = async (cwd: string, depth = 0): Promise<string> => {
  if (depth > 32) throw failure("Verification submodule nesting exceeds the snapshot limit.");
  const git = async (args: string[]) =>
    (
      await exec("git", ["--no-optional-locks", ...args], {
        cwd,
        encoding: "buffer",
        maxBuffer: 32 * 1024 * 1024,
        env: { ...process.env, GIT_OPTIONAL_LOCKS: "0" },
      })
    ).stdout;
  const hash = NodeCrypto.createHash("sha256");
  const add = (value: Buffer | string) => {
    hash.update(String(Buffer.byteLength(value)));
    hash.update(":");
    hash.update(value);
  };
  add(await git(["rev-parse", "HEAD"]));
  add(
    await git([
      "diff",
      "HEAD",
      "--binary",
      "--no-ext-diff",
      "--no-textconv",
      "--ignore-submodules=none",
    ]),
  );
  const others = (await git(["ls-files", "--others", "--exclude-standard", "-z"]))
    .toString("utf8")
    .split("\0")
    .filter(Boolean)
    .sort();
  for (const name of others) {
    const path = NodePath.join(cwd, name);
    const stat = await NodeFSP.lstat(path);
    add(name);
    add(String(stat.mode));
    add(stat.isSymbolicLink() ? await NodeFSP.readlink(path) : await NodeFSP.readFile(path));
  }
  const gitlinks = (await git(["ls-files", "--stage", "-z"]))
    .toString("utf8")
    .split("\0")
    .filter((entry) => entry.startsWith("160000 "));
  for (const entry of gitlinks) {
    const name = entry.slice(entry.indexOf("\t") + 1);
    const path = NodePath.join(cwd, name);
    add(name);
    // The superproject records only a commit plus a dirty flag. Bind the
    // actual submodule files so two different dirty trees cannot share proof.
    const initialized = await NodeFSP.lstat(NodePath.join(path, ".git")).then(
      () => true,
      (cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT") return false;
        throw cause;
      },
    );
    add(initialized ? await readVerificationSnapshot(path, depth + 1) : "uninitialized");
  }
  return hash.digest("hex");
};
export const snapshotVerificationCode = (cwd: string) =>
  Effect.tryPromise({ try: () => readVerificationSnapshot(cwd), catch: failure });

export interface VerificationCheckClaim {
  readonly evidence: {
    readonly providerDeliveryId: string;
    readonly handoffId: string;
    readonly fenceToken: number;
    readonly worktreePath: string;
  };
  readonly delivery: { readonly providerTurnId: string | null };
}
export type VerificationChecksCode =
  | "verification-checks-missing"
  | "verification-checks-unavailable"
  | "verification-checks-stale"
  | "verification-checks-failed";
export interface VerificationCheckManifest {
  readonly providerDeliveryId: string;
  readonly handoffId: string;
  readonly fenceToken: number;
  readonly worktreePath: string;
  readonly codeDigest: string;
  readonly checksJson: string;
  readonly manifestDigest: string;
}
const loadManifest = (
  sql: SqlClient.SqlClient,
  deliveryId: string,
) => sql<VerificationCheckManifest>`
  SELECT provider_delivery_id AS "providerDeliveryId", handoff_id AS "handoffId", fence_token AS "fenceToken",
    worktree_path AS "worktreePath", code_digest AS "codeDigest", checks_json AS "checksJson", manifest_digest AS "manifestDigest"
  FROM agent_control_verification_check_manifests WHERE provider_delivery_id=${deliveryId}`;
const manifestDigest = (manifest: Omit<VerificationCheckManifest, "manifestDigest">) =>
  sha256Utf8(canonicalJson(manifest));

export const prepareVerificationCheckManifest = Effect.fn("prepareVerificationCheckManifest")(
  function* (
    sql: SqlClient.SqlClient,
    input: {
      readonly permit: ProviderAdmissionPermit;
      readonly cwd: string;
      readonly checks?: AgentControlVerificationChecks;
    },
  ) {
    const existing = yield* loadManifest(sql, input.permit.providerDeliveryId);
    if (existing.length) {
      const manifest = existing[0]!;
      if (
        manifest.handoffId !== input.permit.handoffId ||
        manifest.fenceToken !== input.permit.stageFenceToken ||
        manifest.worktreePath !== input.cwd
      )
        return yield* Effect.fail(failure("Verification manifest authority changed"));
      return manifest;
    }
    const policies = yield* sql<{
      policy: string;
    }>`SELECT policy_json AS policy FROM agent_control_project_policies WHERE project_id=${input.permit.projectId}`;
    const policy = yield* decodePolicyJson(policies[0]?.policy ?? "{}");
    const checks = yield* decodeChecks(input.checks ?? policy.verificationChecks ?? []);
    const manifest = {
      providerDeliveryId: input.permit.providerDeliveryId,
      handoffId: input.permit.handoffId,
      fenceToken: input.permit.stageFenceToken,
      worktreePath: input.cwd,
      codeDigest: yield* snapshotVerificationCode(input.cwd),
      checksJson: canonicalJson(checks),
    };
    const digest = manifestDigest(manifest);
    yield* sql`INSERT INTO agent_control_verification_check_manifests
    (provider_delivery_id,handoff_id,fence_token,worktree_path,code_digest,checks_json,manifest_digest,created_at)
    VALUES (${manifest.providerDeliveryId},${manifest.handoffId},${manifest.fenceToken},${manifest.worktreePath},${manifest.codeDigest},${manifest.checksJson},${digest},${yield* now})`;
    return { ...manifest, manifestDigest: digest };
  },
);

export interface VerificationCheckCommandResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}
interface CheckResultRow {
  readonly checkId: string;
  readonly providerTurnId: string;
  readonly manifestDigest: string;
  readonly codeDigest: string;
  readonly status: "passed" | "failed" | "unavailable" | "stale";
  readonly resultJson: string;
  readonly resultDigest: string;
}
const ResultSchema = Schema.Struct({
  exitCode: Schema.Int,
  stdout: Schema.String,
  stderr: Schema.String,
});
const decodeResultJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ResultSchema));
const unavailableResult = (message: string): VerificationCheckCommandResult => ({
  exitCode: 125,
  stdout: "",
  stderr: message,
});

/** The start is committed before invoking the command. An ambiguous start is never re-executed. */
export const executeVerificationCheck = Effect.fn("executeVerificationCheck")(function* <
  AuthorizationError,
  ExecutionError,
>(
  sql: SqlClient.SqlClient,
  input: {
    readonly manifest: VerificationCheckManifest;
    readonly checkId: string;
    readonly providerTurnId: string;
    readonly authorize: Effect.Effect<void, AuthorizationError>;
    readonly execute: Effect.Effect<VerificationCheckCommandResult, ExecutionError>;
  },
) {
  const { manifest } = input;
  const checks = yield* decodeChecksJson(manifest.checksJson);
  const check = checks.find((candidate) => candidate.id === input.checkId);
  if (!check) return yield* Effect.fail(failure("Unknown project check"));
  yield* input.authorize.pipe(Effect.mapError(failure));
  const prior =
    yield* sql<CheckResultRow>`SELECT check_id AS "checkId",provider_turn_id AS "providerTurnId",manifest_digest AS "manifestDigest",code_digest AS "codeDigest",status,result_json AS "resultJson",result_digest AS "resultDigest"
    FROM agent_control_verification_check_results WHERE provider_delivery_id=${manifest.providerDeliveryId} AND check_id=${input.checkId}`;
  if (prior[0]) {
    if (
      prior[0].providerTurnId !== input.providerTurnId ||
      prior[0].manifestDigest !== manifest.manifestDigest ||
      sha256Utf8(prior[0].resultJson) !== prior[0].resultDigest
    )
      return unavailableResult("Verification check evidence belongs to another execution.");
    const current = yield* snapshotVerificationCode(manifest.worktreePath).pipe(
      Effect.catch(() => Effect.succeed(null)),
    );
    if (prior[0].status === "stale" || current !== manifest.codeDigest)
      return unavailableResult("Verification code state changed.");
    if (prior[0].status === "unavailable")
      return unavailableResult("Verification check was unavailable.");
    return yield* decodeResultJson(prior[0].resultJson);
  }
  const inserted = yield* sql`INSERT INTO agent_control_verification_check_starts
    (provider_delivery_id,check_id,provider_turn_id,manifest_digest,started_at)
    VALUES (${manifest.providerDeliveryId},${input.checkId},${input.providerTurnId},${manifest.manifestDigest},${yield* now})
    ON CONFLICT(provider_delivery_id,check_id) DO NOTHING RETURNING check_id`;
  if (!inserted.length)
    return unavailableResult(
      "Verification check execution is incomplete; automatic duplicate execution is forbidden.",
    );
  const readNotifications = yield* AgentControlRunOnceReadNotifications;
  yield* readNotifications.publish(manifest.handoffId);
  const before = yield* snapshotVerificationCode(manifest.worktreePath).pipe(
    Effect.catch(() => Effect.succeed(null)),
  );
  const result =
    before !== manifest.codeDigest
      ? unavailableResult("Verification code state changed before the check.")
      : yield* input.execute.pipe(
          Effect.mapError(failure),
          Effect.catch(() =>
            Effect.succeed(unavailableResult("Verification check could not execute.")),
          ),
        );
  const after = yield* snapshotVerificationCode(manifest.worktreePath).pipe(
    Effect.catch(() => Effect.succeed(null)),
  );
  const authorized = yield* input.authorize.pipe(
    Effect.mapError(failure),
    Effect.as(true),
    Effect.catch(() => Effect.succeed(false)),
  );
  const status =
    before !== manifest.codeDigest || after !== manifest.codeDigest
      ? "stale"
      : !authorized
        ? "unavailable"
        : classifyVerificationCheckResult(check, result);
  const resultJson = canonicalJson({ ...result });
  yield* sql`INSERT INTO agent_control_verification_check_results
    (provider_delivery_id,check_id,provider_turn_id,manifest_digest,code_digest,status,result_json,result_digest,completed_at)
    VALUES (${manifest.providerDeliveryId},${input.checkId},${input.providerTurnId},${manifest.manifestDigest},${after ?? "unavailable"},${status},${resultJson},${sha256Utf8(resultJson)},${yield* now})`;
  yield* readNotifications.publish(manifest.handoffId);
  return status === "stale" || !authorized
    ? unavailableResult("Verification code state or authorization changed.")
    : result;
});

export const assessVerificationChecks = Effect.fn("assessVerificationChecks")(function* (
  sql: SqlClient.SqlClient,
  claim: VerificationCheckClaim,
  options: { readonly checkCurrentCode?: boolean } = {},
) {
  const manifests = yield* loadManifest(sql, claim.evidence.providerDeliveryId);
  const manifest = manifests[0];
  const results =
    yield* sql<CheckResultRow>`SELECT check_id AS "checkId",provider_turn_id AS "providerTurnId",manifest_digest AS "manifestDigest",code_digest AS "codeDigest",status,result_json AS "resultJson",result_digest AS "resultDigest"
    FROM agent_control_verification_check_results WHERE provider_delivery_id=${claim.evidence.providerDeliveryId} ORDER BY check_id`;
  const digest = sha256Utf8(
    canonicalJson({
      identity: {
        providerDeliveryId: claim.evidence.providerDeliveryId,
        handoffId: claim.evidence.handoffId,
        fenceToken: claim.evidence.fenceToken,
        worktreePath: claim.evidence.worktreePath,
        providerTurnId: claim.delivery.providerTurnId,
      },
      manifest: manifest ? { ...manifest } : null,
      results: results.map((result) => ({ ...result })),
    }),
  );
  let code: VerificationChecksCode | null = null;
  if (!manifest) return { code: "verification-checks-missing" as const, digest };
  const { manifestDigest: expected, ...document } = manifest;
  if (
    manifest.handoffId !== claim.evidence.handoffId ||
    manifest.fenceToken !== claim.evidence.fenceToken ||
    manifest.worktreePath !== claim.evidence.worktreePath ||
    manifestDigest(document) !== expected
  )
    return { code: "verification-checks-stale" as const, digest };
  const checks = yield* decodeChecksJson(manifest.checksJson);
  const required = checks.filter((check) => check.required);
  if (!required.length) code = "verification-checks-missing";
  for (const check of required) {
    const result = results.find((item) => item.checkId === check.id);
    if (!result) {
      // Incomplete evidence must prevent repair even when another check proved a code failure.
      if (code !== "verification-checks-unavailable") code = "verification-checks-missing";
      continue;
    }
    if (
      result.providerTurnId !== claim.delivery.providerTurnId ||
      result.manifestDigest !== manifest.manifestDigest ||
      result.codeDigest !== manifest.codeDigest ||
      result.status === "stale" ||
      sha256Utf8(result.resultJson) !== result.resultDigest
    ) {
      code = "verification-checks-stale";
      break;
    }
    const output = yield* decodeResultJson(result.resultJson);
    const classified = classifyVerificationCheckResult(check, output);
    if (result.status === "unavailable" || classified === "unavailable")
      code = "verification-checks-unavailable";
    else if (result.status !== "passed" || classified !== "passed")
      code ??= "verification-checks-failed";
  }
  if (options.checkCurrentCode !== false) {
    const current = yield* snapshotVerificationCode(manifest.worktreePath).pipe(
      Effect.catch(() => Effect.succeed(null)),
    );
    if (current !== manifest.codeDigest) code = "verification-checks-stale";
  }
  return { code, digest };
});

const hasSqliteCode = Schema.is(Schema.Struct({ errcode: Schema.Int }));

export class VerificationCheckAssessmentError extends Schema.TaggedError<VerificationCheckAssessmentError>()(
  "VerificationCheckAssessmentError",
  {
    operation: Schema.Literals(["load-seal", "assess-checks", "insert-seal", "compare-seal"]),
    reason: Schema.Literals(["persistence", "invalid-evidence", "evidence-conflict"]),
    sqlReason: Schema.optional(Schema.String),
    sqliteCode: Schema.optional(Schema.Int),
    cause: Schema.optional(Schema.Unknown),
  },
) {}

export const sealVerificationCheckAssessment = Effect.fn("sealVerificationCheckAssessment")(
  function* (
    sql: SqlClient.SqlClient,
    claim: VerificationCheckClaim,
    beforeInsert: Effect.Effect<void> = Effect.void,
  ) {
    const mapFailure =
      (operation: VerificationCheckAssessmentError["operation"]) => (cause: unknown) =>
        new VerificationCheckAssessmentError({
          operation,
          reason: isSqlError(cause) ? "persistence" : "invalid-evidence",
          ...(isSqlError(cause) ? { sqlReason: cause.reason._tag } : {}),
          ...(isSqlError(cause) && hasSqliteCode(cause.reason.cause)
            ? { sqliteCode: cause.reason.cause.errcode }
            : {}),
          cause,
        });
    const loadSeal = sql<{
      providerTurnId: string;
      code: VerificationChecksCode | null;
      digest: string;
    }>`SELECT provider_turn_id AS "providerTurnId",code,digest FROM agent_control_verification_check_assessments WHERE provider_delivery_id=${claim.evidence.providerDeliveryId}`.pipe(
      Effect.mapError(mapFailure("load-seal")),
    );
    const replay = Effect.fn("replayVerificationCheckAssessment")(function* (
      existing: Effect.Success<typeof loadSeal>,
    ) {
      const historical = yield* assessVerificationChecks(sql, claim, {
        checkCurrentCode: false,
      }).pipe(Effect.mapError(mapFailure("assess-checks")));
      if (
        existing.length !== 1 ||
        existing[0]!.providerTurnId !== claim.delivery.providerTurnId ||
        existing[0]!.digest !== historical.digest
      )
        return yield* new VerificationCheckAssessmentError({
          operation: "compare-seal",
          reason: "evidence-conflict",
        });
      return { code: existing[0]!.code, digest: existing[0]!.digest };
    });
    const existing = yield* loadSeal;
    if (existing.length !== 0) return yield* replay(existing);
    const assessment = yield* assessVerificationChecks(sql, claim).pipe(
      Effect.mapError(mapFailure("assess-checks")),
    );
    yield* beforeInsert;
    // Evaluator recovery and stage finalization can assess the same delivery.
    // Only the delivery key may lose the insert; every replay still proves the
    // immutable check evidence and turn identity against the winning seal.
    const inserted =
      yield* sql`INSERT INTO agent_control_verification_check_assessments(provider_delivery_id,provider_turn_id,code,digest,sealed_at)
    VALUES (${claim.evidence.providerDeliveryId},${claim.delivery.providerTurnId ?? ""},${assessment.code},${assessment.digest},${yield* now})
    ON CONFLICT(provider_delivery_id) DO NOTHING RETURNING provider_delivery_id`.pipe(
        Effect.mapError(mapFailure("insert-seal")),
      );
    return inserted.length === 1 ? assessment : yield* replay(yield* loadSeal);
  },
);
