// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import {
  AgentControlEpicAcceptedResult,
  AgentControlEpicRpcError,
  AgentControlRunOnceId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CodexSettings,
  ProviderInstanceId,
  type AgentControlWorktreeReservationState,
  type AgentControlVerificationChecks,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as CodexClient from "effect-codex-app-server/client";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { AgentControlWorktreeController } from "../worktree/Services/AgentControlWorktreeController.ts";
import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import {
  assessVerificationChecks,
  executeVerificationCheck,
  snapshotVerificationCode,
  type VerificationCheckCommandResult,
  type VerificationCheckManifest,
} from "../verificationTurn/checkEvidence.ts";
import {
  AgentControlEpicResultHooks,
  type AgentControlEpicCaptureInput,
  type AgentControlEpicResultHooksShape,
} from "./Services/AgentControlEpicResultHooks.ts";
import { deriveProviderInstanceConfigMap } from "../../provider/Layers/ProviderInstanceRegistryHydration.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { verificationCheckParams } from "../../provider/CodexVerificationChecks.ts";
import { buildCodexInitializeParams } from "../../provider/Layers/CodexProvider.ts";
import { codexAppServerArgs } from "../../provider/Layers/codexLaunchArgs.ts";
import { expandHomePath } from "../../pathExpansion.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
const fail = (message: string) =>
  new AgentControlEpicRpcError({ code: "epic-result-unavailable", message });
const now = Effect.map(DateTime.now, DateTime.formatIso);
const git = (cwd: string, args: readonly string[], env?: NodeJS.ProcessEnv) =>
  Effect.tryPromise({
    try: async () =>
      (
        await execFile("git", [...args], {
          cwd,
          env: { ...process.env, GIT_OPTIONAL_LOCKS: "0", ...env },
          maxBuffer: 32 * 1024 * 1024,
          timeout: 30_000,
          killSignal: "SIGKILL",
        })
      ).stdout.trim(),
    catch: () => fail("The accepted Git result could not be read or retained."),
  }).pipe(
    // An interrupted capture must retain its repository lock until Git exits.
    // Otherwise update-ref/read-tree could mutate after a successor acquired it.
    Effect.uninterruptible,
  );
const decodeAccepted = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlEpicAcceptedResult),
);

const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);
const isEpicRpcError = Schema.is(AgentControlEpicRpcError);

/** Fixed project commands use the same Codex command sandbox as task verification; no model turn. */
export const EpicCheckExecutor = Context.Reference<{
  readonly execute: (input: {
    providerInstanceId: string;
    cwd: string;
    checks: AgentControlVerificationChecks;
    checkId: string;
  }) => Effect.Effect<VerificationCheckCommandResult, AgentControlEpicRpcError>;
}>("t3/agentControl/epic/CheckExecutor", {
  defaultValue: () => ({
    execute: () => Effect.fail(fail("The sandboxed check executor is unavailable.")),
  }),
});

export const EpicCheckExecutorLive = Layer.effect(
  EpicCheckExecutor,
  Effect.gen(function* () {
    const settings = yield* ServerSettingsService;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    return {
      execute: (input) =>
        Effect.scoped(
          Effect.gen(function* () {
            const configured = deriveProviderInstanceConfigMap(yield* settings.getSettings)[
              ProviderInstanceId.make(input.providerInstanceId)
            ];
            if (!configured || configured.driver !== "codex" || configured.enabled === false)
              return yield* fail(
                "The accepted verification provider has no supported command sandbox.",
              );
            const config = yield* decodeCodexSettings(configured.config ?? {});
            const temporary = yield* Effect.acquireRelease(
              Effect.tryPromise(() =>
                NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-epic-check-")),
              ),
              (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
            );
            const env = {
              ...mergeProviderInstanceEnvironment(configured.environment),
              ...(config.homePath ? { CODEX_HOME: expandHomePath(config.homePath) } : {}),
            };
            const spawn = yield* resolveSpawnCommand(
              config.binaryPath,
              codexAppServerArgs(config.launchArgs),
              { env, extendEnv: true },
            );
            const child = yield* spawner.spawn(
              ChildProcess.make(spawn.command, spawn.args, {
                cwd: temporary,
                env,
                extendEnv: true,
                forceKillAfter: "2 seconds",
                shell: spawn.shell,
              }),
            );
            const context = yield* Layer.build(CodexClient.layerChildProcess(child));
            const client = yield* CodexClient.CodexAppServerClient.pipe(Effect.provide(context));
            yield* client.request("initialize", buildCodexInitializeParams());
            yield* client.notify("initialized", undefined);
            const params = yield* verificationCheckParams(
              { check: input.checkId },
              input.cwd,
              input.checks,
              temporary,
            );
            return yield* client.request("command/exec", params);
          }),
        ).pipe(
          Effect.mapError(() =>
            fail("The configured check could not execute in its read-only sandbox."),
          ),
        ),
    };
  }),
);

interface CaptureIntent {
  inputJson: string;
  sourceHead: string;
  commitSha: string;
  treeSha: string;
  codeDigest: string;
  manifestDigest: string;
  createdAt: string;
}

export const makeEpicResults = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const worktrees = yield* AgentControlWorktreeController;
  const executor = yield* EpicCheckExecutor;
  const owned = <A, E, R>(
    input: AgentControlEpicCaptureInput,
    callback: (state: AgentControlWorktreeReservationState) => Effect.Effect<A, E, R>,
  ) => {
    if (!worktrees.useAcceptedWorktree)
      return Effect.fail(fail("Accepted worktree validation is unavailable."));
    return worktrees.useAcceptedWorktree(
      {
        projectId: input.projectId,
        taskId: AgentControlTaskId.make(input.taskId),
        childRunId: AgentControlRunOnceId.make(input.childRunId),
        reservationId: AgentControlWorktreeReservationId.make(input.reservationId),
        taskFinalizationEvidenceId: input.taskFinalizationEvidenceId,
      },
      callback,
    );
  };
  const readIntent = (childRunId: string) =>
    sql<CaptureIntent>`SELECT input_json AS "inputJson",source_head AS "sourceHead",commit_sha AS "commitSha",tree_sha AS "treeSha",code_digest AS "codeDigest",manifest_digest AS "manifestDigest",created_at AS "createdAt" FROM agent_control_epic_capture_intents WHERE child_run_id=${childRunId}`;
  const verification = (input: AgentControlEpicCaptureInput) => sql<{
    providerDeliveryId: string;
    providerTurnId: string;
    providerInstanceId: string;
    handoffId: string;
    fenceToken: number;
    finalizedAt: string;
  }>`
    SELECT stage.provider_delivery_id AS "providerDeliveryId",stage.provider_turn_id AS "providerTurnId",stage.provider_instance_id AS "providerInstanceId",stage.handoff_id AS "handoffId",stage.fence_token AS "fenceToken",task.finalized_at AS "finalizedAt"
    FROM agent_control_task_verification_finalization_evidence task
    JOIN agent_control_verification_finalization_evidence stage ON stage.finalization_evidence_id=task.verification_evidence_id
    WHERE task.task_finalization_evidence_id=${input.taskFinalizationEvidenceId} AND task.task_id=${input.taskId} AND task.project_id=${input.projectId} AND task.verification_outcome='succeeded'`;

  const capture: AgentControlEpicResultHooksShape["capture"] = (input) =>
    owned(input, (state) =>
      Effect.gen(function* () {
        const cwd = state.internalWorktreePath;
        const inputJson = canonicalJson({ ...input });
        let intent = (yield* readIntent(input.childRunId))[0];
        if (intent && intent.inputJson !== inputJson)
          return yield* fail("The result capture belongs to another Epic or base.");
        const prior = yield* sql<{
          resultJson: string;
          resultDigest: string;
        }>`SELECT result_json AS "resultJson",result_digest AS "resultDigest" FROM agent_control_epic_capture_results WHERE child_run_id=${input.childRunId}`;
        if (prior[0]) {
          if (!intent || sha256Utf8(prior[0].resultJson) !== prior[0].resultDigest)
            return yield* fail("Accepted result evidence is inconsistent.");
          const result = yield* decodeAccepted(prior[0].resultJson);
          if (
            result.commitSha !== intent.commitSha ||
            (yield* git(cwd, ["rev-parse", `${result.commitSha}^{tree}`])) !== result.treeSha
          )
            return yield* fail("Accepted result commit is unavailable.");
          return result;
        }
        if (!intent) {
          const proof = (yield* verification(input))[0];
          if (!proof) return yield* fail("Successful child verification evidence is missing.");
          const assessment = yield* assessVerificationChecks(sql, {
            evidence: { ...proof, worktreePath: cwd },
            delivery: { providerTurnId: proof.providerTurnId },
          });
          const seals = yield* sql<{
            digest: string;
            code: string | null;
            codeDigest: string;
          }>`SELECT assessment.digest,assessment.code,manifest.code_digest AS "codeDigest"
              FROM agent_control_verification_check_assessments assessment
              JOIN agent_control_verification_check_manifests manifest
                ON manifest.provider_delivery_id=assessment.provider_delivery_id
              WHERE assessment.provider_delivery_id=${proof.providerDeliveryId}`;
          if (
            assessment.code !== null ||
            seals.length !== 1 ||
            seals[0]?.code !== null ||
            seals[0].digest !== assessment.digest
          )
            return yield* fail(
              "The child worktree no longer matches its accepted mandatory checks.",
            );
          const digest = yield* snapshotVerificationCode(cwd);
          if (digest !== seals[0].codeDigest)
            return yield* fail("The child changed after its mandatory checks were assessed.");
          const sourceHead = yield* git(cwd, ["rev-parse", "HEAD"]);
          const parent = input.previousCommitSha ?? state.baseCommitSha;
          if (parent !== state.baseCommitSha)
            return yield* fail("The child was not built on the preceding accepted result.");
          const submodules = yield* git(cwd, ["submodule", "status", "--recursive"]);
          if (submodules)
            return yield* fail("Epic result capture does not yet support submodule worktrees.");
          const temp = yield* Effect.acquireRelease(
            Effect.tryPromise(() =>
              NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-epic-index-")),
            ),
            (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
          );
          const indexEnv = { GIT_INDEX_FILE: NodePath.join(temp, "index") };
          yield* git(cwd, ["read-tree", "HEAD"], indexEnv);
          yield* git(cwd, ["add", "--all", "--", "."], indexEnv);
          const treeSha = yield* git(cwd, ["write-tree"], indexEnv);
          const commitSha = yield* git(
            cwd,
            [
              "commit-tree",
              treeSha,
              "-p",
              parent,
              "-m",
              `T3Auto Epic ${input.epicRunId}: ${input.taskId}\n\nChild run: ${input.childRunId}\nVerification: ${input.taskFinalizationEvidenceId}`,
            ],
            {
              GIT_AUTHOR_NAME: "T3Auto",
              GIT_AUTHOR_EMAIL: "t3auto@localhost",
              GIT_COMMITTER_NAME: "T3Auto",
              GIT_COMMITTER_EMAIL: "t3auto@localhost",
              GIT_AUTHOR_DATE: proof.finalizedAt,
              GIT_COMMITTER_DATE: proof.finalizedAt,
            },
          );
          if ((yield* snapshotVerificationCode(cwd)) !== digest)
            return yield* fail("The child changed while its result was captured.");
          yield* sql`INSERT INTO agent_control_epic_capture_intents (child_run_id,epic_run_id,project_id,input_json,source_head,commit_sha,tree_sha,code_digest,manifest_digest,created_at) VALUES (${input.childRunId},${input.epicRunId},${input.projectId},${inputJson},${sourceHead},${commitSha},${treeSha},${digest},${assessment.digest},${proof.finalizedAt})`;
          intent = {
            inputJson,
            sourceHead,
            commitSha,
            treeSha,
            codeDigest: digest,
            manifestDigest: assessment.digest,
            createdAt: proof.finalizedAt,
          };
        }
        const currentHead = yield* git(cwd, ["rev-parse", "HEAD"]);
        if (currentHead === intent.sourceHead) {
          if ((yield* snapshotVerificationCode(cwd)) !== intent.codeDigest)
            return yield* fail("The child changed before its accepted commit was published.");
          yield* git(cwd, [
            "update-ref",
            `refs/heads/${state.branchName}`,
            intent.commitSha,
            intent.sourceHead,
          ]);
        } else if (currentHead !== intent.commitSha)
          return yield* fail("The child branch changed during result capture.");
        // Only the index changes; source files and ignored dependencies are retained.
        yield* git(cwd, ["read-tree", intent.commitSha]);
        const dirty = yield* git(cwd, ["status", "--porcelain", "--untracked-files=all"]);
        if (dirty) return yield* fail("The captured commit no longer matches the child files.");
        const result = {
          commitSha: intent.commitSha,
          treeSha: intent.treeSha,
          codeDigest: intent.codeDigest,
          evidenceId: `epic-capture:${input.childRunId}`,
        };
        const resultJson = canonicalJson(result);
        yield* sql`INSERT INTO agent_control_epic_capture_results(child_run_id,result_json,result_digest,accepted_at) VALUES (${input.childRunId},${resultJson},${sha256Utf8(resultJson)},${yield* now}) ON CONFLICT(child_run_id) DO NOTHING`;
        return result;
      }),
    ).pipe(
      Effect.scoped,
      Effect.mapError((cause) =>
        isEpicRpcError(cause) ? cause : fail("The verified child result could not be accepted."),
      ),
    );

  const verify: AgentControlEpicResultHooksShape["verify"] = (input) => {
    const member = input.lastAccepted;
    if (
      !member.taskId ||
      !member.childRunId ||
      !member.reservationId ||
      !member.taskFinalizationEvidenceId ||
      member.accepted?.commitSha !== input.commitSha
    )
      return Effect.fail(fail("The final accepted result is incomplete."));
    const captureInput = {
      epicRunId: input.epicRunId,
      projectId: input.projectId,
      taskId: member.taskId,
      childRunId: member.childRunId,
      reservationId: member.reservationId,
      taskFinalizationEvidenceId: member.taskFinalizationEvidenceId,
      previousCommitSha: member.baseCommitSha,
    };
    return owned(captureInput, (state) =>
      Effect.gen(function* () {
        const evidenceId = `epic-final:${input.epicRunId}:${input.attempt}`;
        const cwd = state.internalWorktreePath;
        const proof = (yield* verification(captureInput))[0];
        if (!proof) return yield* fail("The final verification provider evidence is missing.");
        const authorize = Effect.gen(function* () {
          if (
            (yield* git(cwd, ["rev-parse", "HEAD"])) !== input.commitSha ||
            (yield* git(cwd, ["status", "--porcelain", "--untracked-files=all"]))
          )
            return yield* fail("The common result changed during final verification.");
        });
        yield* authorize;
        const documents = yield* sql<{
          codeDigest: string;
          checksJson: string;
          manifestDigest: string;
        }>`SELECT code_digest AS "codeDigest",checks_json AS "checksJson",manifest_digest AS "manifestDigest" FROM agent_control_verification_check_manifests WHERE provider_delivery_id=${evidenceId}`;
        const document = {
          providerDeliveryId: evidenceId,
          handoffId: evidenceId,
          fenceToken: input.attempt,
          worktreePath: cwd,
          codeDigest: yield* snapshotVerificationCode(cwd),
          checksJson: canonicalJson(input.checks),
        };
        const manifest: VerificationCheckManifest = {
          ...document,
          manifestDigest: sha256Utf8(canonicalJson(document)),
        };
        if (
          documents[0] &&
          (documents[0].manifestDigest !== manifest.manifestDigest ||
            documents[0].checksJson !== manifest.checksJson)
        )
          return yield* fail(
            "The final verification attempt belongs to a different result or check configuration.",
          );
        yield* sql`INSERT INTO agent_control_verification_check_manifests(provider_delivery_id,handoff_id,fence_token,worktree_path,code_digest,checks_json,manifest_digest,created_at) VALUES (${evidenceId},${evidenceId},${input.attempt},${cwd},${manifest.codeDigest},${manifest.checksJson},${manifest.manifestDigest},${yield* now}) ON CONFLICT(provider_delivery_id) DO NOTHING`;
        for (const check of input.checks) {
          yield* executeVerificationCheck(sql, {
            manifest,
            checkId: check.id,
            providerTurnId: evidenceId,
            authorize,
            execute: executor.execute({
              providerInstanceId: proof.providerInstanceId,
              cwd,
              checks: input.checks,
              checkId: check.id,
            }),
          });
        }
        const assessment = yield* assessVerificationChecks(sql, {
          evidence: {
            providerDeliveryId: evidenceId,
            handoffId: evidenceId,
            fenceToken: input.attempt,
            worktreePath: cwd,
          },
          delivery: { providerTurnId: evidenceId },
        });
        const rows = yield* sql<{
          checkId: string;
          status: "passed" | "failed" | "unavailable" | "stale";
          resultJson: string;
          completedAt: string;
        }>`SELECT check_id AS "checkId",status,result_json AS "resultJson",completed_at AS "completedAt" FROM agent_control_verification_check_results WHERE provider_delivery_id=${evidenceId}`;
        const resultSchema = Schema.Struct({
          exitCode: Schema.Int,
          stdout: Schema.String,
          stderr: Schema.String,
        });
        const checks = yield* Effect.forEach(input.checks, (check) =>
          Effect.gen(function* () {
            const row = rows.find((row) => row.checkId === check.id);
            const result = row
              ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(resultSchema))(
                  row.resultJson,
                )
              : null;
            return {
              ...check,
              status: row?.status ?? ("missing" as const),
              exitCode: result?.exitCode ?? null,
              output: result ? [result.stdout, result.stderr].filter(Boolean).join("\n") : null,
              completedAt: row?.completedAt ?? null,
            };
          }),
        );
        return {
          status:
            assessment.code === null
              ? ("passed" as const)
              : assessment.code === "verification-checks-failed"
                ? ("failed" as const)
                : ("blocked" as const),
          commitSha: input.commitSha,
          evidenceId,
          detail: assessment.code ?? "All required checks passed on the common result.",
          checks,
        };
      }),
    ).pipe(
      Effect.scoped,
      Effect.mapError((cause) =>
        isEpicRpcError(cause) ? cause : fail("Final Epic checks could not be completed."),
      ),
    );
  };
  return { capture, verify } satisfies AgentControlEpicResultHooksShape;
});
export const EpicResultsLive = Layer.effect(AgentControlEpicResultHooks, makeEpicResults);
