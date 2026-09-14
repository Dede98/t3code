// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as NodeSqlite from "node:sqlite";
import { describe, expect, it, vi } from "@effect/vitest";
import {
  AgentControlTaskId,
  AgentControlWorktreeReservationState,
  ProjectId,
  type AgentControlVerificationChecks,
  type AgentControlEpicAcceptedResult,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import Migration076 from "../../persistence/Migrations/076_AgentControlVerificationChecks.ts";
import Migration084 from "../../persistence/Migrations/084_AgentControlEpicResults.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import {
  executeVerificationCheck,
  sealVerificationCheckAssessment,
  snapshotVerificationCode,
  type VerificationCheckCommandResult,
} from "../verificationTurn/checkEvidence.ts";
import * as CheckEvidence from "../verificationTurn/checkEvidence.ts";
import { AgentControlWorktreeController } from "../worktree/Services/AgentControlWorktreeController.ts";
import { EpicCheckExecutor, makeEpicResults } from "./results.ts";
import type {
  AgentControlEpicCaptureInput,
  AgentControlEpicVerifyInput,
} from "./Services/AgentControlEpicResultHooks.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const io = <A>(operation: () => Promise<A>) => Effect.tryPromise(operation);
const git = (cwd: string, args: string[]) =>
  io(async () => (await exec("git", args, { cwd })).stdout.trim());
const at = "2026-09-14T08:00:00.000Z";
const checks: AgentControlVerificationChecks = [
  {
    id: "tests",
    command: "node",
    args: ["--test"],
    cwd: ".",
    required: true,
    timeoutMs: 10000,
    allowTemporaryFiles: false,
    resultFormat: "node-test",
  },
];
const success: VerificationCheckCommandResult = {
  exitCode: 0,
  stdout: "# pass 1\n# fail 0\n# cancelled 0\n",
  stderr: "",
};
const failure: VerificationCheckCommandResult = {
  exitCode: 1,
  stdout: "  code: 'ERR_ASSERTION'\n# pass 0\n# fail 1\n# cancelled 0\n",
  stderr: "combined result failed",
};
const captureInput: AgentControlEpicCaptureInput = {
  epicRunId: "epic-1",
  projectId: ProjectId.make("project"),
  taskId: "task",
  childRunId: "child",
  reservationId: "reservation",
  previousCommitSha: null,
  taskFinalizationEvidenceId: "task-proof",
};

const repository = Effect.acquireRelease(
  io(async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-epic-result-test-"));
    const cwd = NodePath.join(root, "repo");
    await NodeFSP.mkdir(cwd);
    await exec("git", ["init", "--quiet", "--initial-branch=task"], { cwd });
    await NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "base\n");
    await exec("git", ["add", "source.txt"], { cwd });
    await exec(
      "git",
      ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"],
      { cwd },
    );
    const base = (await exec("git", ["rev-parse", "HEAD"], { cwd })).stdout.trim();
    await NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "accepted A\n");
    return { root, cwd, base, database: NodePath.join(root, "state.sqlite") };
  }),
  (repo) => io(() => NodeFSP.rm(repo.root, { recursive: true, force: true })).pipe(Effect.orDie),
);
type Repo = Effect.Success<typeof repository>;

const decodeReservation = Schema.decodeUnknownSync(AgentControlWorktreeReservationState);
const reservation = (repo: Repo) =>
  decodeReservation({
    schemaVersion: 1,
    reservationId: "reservation",
    projectId: "project",
    taskId: "task",
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint: "source",
    stageRunId: "stage",
    attemptId: "attempt",
    leaseId: "lease",
    fenceToken: 1,
    repository: {
      repositoryNodeId: "repository",
      nameWithOwner: "owner/repo",
      canonicalKey: "github:owner/repo",
      remoteName: "origin",
      remoteUrl: "https://github.com/owner/repo.git",
      defaultRemoteRef: "refs/remotes/origin/main",
      commonDirDevice: 1,
      commonDirInode: 1,
    },
    repositoryWorkspace: repo.cwd,
    repositoryCommonDir: NodePath.join(repo.cwd, ".git"),
    baseRef: "refs/heads/task",
    baseCommitSha: repo.base,
    branchName: "task",
    internalWorktreePath: repo.cwd,
    targetGenerationId: "target",
    worktreeRootDevice: 1,
    worktreeRootInode: 1,
    worktreeParentDevice: 1,
    worktreeParentInode: 1,
    materializationPhase: "ownership-marked",
    gitCreatedDevice: 1,
    gitCreatedInode: 1,
    gitCreatedGitDir: NodePath.join(repo.cwd, ".git"),
    markedOwnershipFingerprint: "owned",
    headCommitSha: repo.base,
    ownershipFingerprint: "owned",
    verifiedAt: at,
    reservedAt: at,
    createdAt: at,
    updatedAt: at,
    revision: 1,
    sequence: 1,
    status: "ready",
    attentionCode: null,
  });
// This fixture supplies an already-authorized reservation and retains repository
// serialization while testing capture against real Git and persisted check evidence.
const worktrees = (repo: Repo) => {
  const lock = Semaphore.makeUnsafe(1);
  return Layer.mock(AgentControlWorktreeController)({
    useAcceptedWorktree: (_input, callback) =>
      lock.withPermit(Effect.scoped(callback(reservation(repo)))),
  });
};
const initialize = (repo: Repo) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Migration076;
    yield* Migration084;
    yield* sql`CREATE TABLE agent_control_task_verification_finalization_evidence (task_finalization_evidence_id TEXT,task_id TEXT,project_id TEXT,verification_outcome TEXT,verification_evidence_id TEXT,finalized_at TEXT)`;
    yield* sql`CREATE TABLE agent_control_verification_finalization_evidence (finalization_evidence_id TEXT,provider_delivery_id TEXT,provider_turn_id TEXT,provider_instance_id TEXT,handoff_id TEXT,fence_token INTEGER)`;
    // Fixture terminal proof rows replace the authority seam only; check manifests,
    // executions, seals and capture evidence below use the production writer.
    yield* Effect.sync(() => {
      const database = new NodeSqlite.DatabaseSync(repo.database);
      try {
        database
          .prepare(
            "INSERT INTO agent_control_task_verification_finalization_evidence VALUES ('task-proof','task','project','succeeded','stage-proof',?)",
          )
          .run(at);
        database.exec(
          "INSERT INTO agent_control_verification_finalization_evidence VALUES ('stage-proof','child-check','child-turn','codex','child-handoff',1)",
        );
      } finally {
        database.close();
      }
    });
    const document = {
      providerDeliveryId: "child-check",
      handoffId: "child-handoff",
      fenceToken: 1,
      worktreePath: repo.cwd,
      codeDigest: yield* snapshotVerificationCode(repo.cwd),
      checksJson: canonicalJson(checks),
    };
    const manifest = { ...document, manifestDigest: sha256Utf8(canonicalJson(document)) };
    yield* sql`INSERT INTO agent_control_verification_check_manifests VALUES (${manifest.providerDeliveryId},${manifest.handoffId},${manifest.fenceToken},${manifest.worktreePath},${manifest.codeDigest},${manifest.checksJson},${manifest.manifestDigest},${at})`;
    yield* executeVerificationCheck(sql, {
      manifest,
      checkId: "tests",
      providerTurnId: "child-turn",
      authorize: Effect.void,
      execute: Effect.succeed(success),
    });
    yield* sealVerificationCheckAssessment(sql, {
      evidence: manifest,
      delivery: { providerTurnId: "child-turn" },
    });
  });
const session = <A, E, R>(
  repo: Repo,
  effect: Effect.Effect<A, E, R | SqlClient.SqlClient | AgentControlWorktreeController>,
) =>
  effect.pipe(
    Effect.provide(
      Layer.merge(NodeSqliteClient.layer({ filename: repo.database }), worktrees(repo)),
    ),
  );
const testWithRepo = <A, E>(
  body: (
    repo: Repo,
  ) => Effect.Effect<A, E, SqlClient.SqlClient | AgentControlWorktreeController | Scope.Scope>,
) =>
  Effect.gen(function* () {
    const repo = yield* repository;
    return yield* session(
      repo,
      Effect.gen(function* () {
        yield* initialize(repo);
        return yield* body(repo);
      }),
    );
  });
const finalInput = (accepted: AgentControlEpicAcceptedResult): AgentControlEpicVerifyInput => ({
  epicRunId: "epic-1",
  projectId: ProjectId.make("project"),
  commitSha: accepted.commitSha,
  checks,
  attempt: 1,
  lastAccepted: {
    issueNodeId: "issue",
    issueNumber: 2,
    taskId: AgentControlTaskId.make("task"),
    childRunId: "child",
    status: "accepted",
    baseCommitSha: null,
    reservationId: "reservation",
    taskFinalizationEvidenceId: "task-proof",
    accepted,
  },
});

describe("Epic accepted results and common verification", () => {
  it.effect(
    "drains an in-flight Git mutation before releasing capture ownership on interruption",
    () =>
      testWithRepo((repo) =>
        Effect.gen(function* () {
          const script = NodePath.join(repo.root, "gate.cjs");
          const enteredPath = NodePath.join(repo.root, "git-entered");
          const releasePath = NodePath.join(repo.root, "git-release");
          yield* io(() =>
            NodeFSP.writeFile(
              script,
              `
const fs = require('node:fs');
const path = require('node:path');
const input = fs.readFileSync(0);
if (!process.env.GIT_INDEX_FILE) process.stdout.write(input);
else {
  const root = path.dirname(__filename);
  const release = path.join(root, 'git-release');
  const finish = () => { watcher.close(); process.stdout.write(input); };
  const watcher = fs.watch(root, (_, file) => { if (file === 'git-release') finish(); });
  fs.writeFileSync(path.join(root, 'git-entered'), 'entered');
  if (fs.existsSync(release)) finish();
}
`,
            ),
          );
          yield* io(() =>
            NodeFSP.writeFile(
              NodePath.join(repo.cwd, ".git", "info", "attributes"),
              "source.txt filter=epic-gate\n",
            ),
          );
          const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
          yield* git(repo.cwd, [
            "config",
            "filter.epic-gate.clean",
            `${quote(process.execPath)} ${quote(script)}`,
          ]);
          const entered = Promise.withResolvers<void>();
          const watcher = yield* Effect.acquireRelease(
            Effect.sync(() =>
              NodeFS.watch(repo.root, (_event, file) => {
                if (file === NodePath.basename(enteredPath)) entered.resolve();
              }),
            ),
            (watcher) => Effect.sync(() => watcher.close()),
          );
          const hooks = yield* makeEpicResults;
          const capture = yield* hooks.capture(captureInput).pipe(Effect.forkChild);
          yield* Effect.promise(() => entered.promise);
          const interrupt = yield* Fiber.interrupt(capture).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          const stillDraining = interrupt.pollUnsafe() === undefined;
          yield* io(() => NodeFSP.writeFile(releasePath, "release"));
          yield* Fiber.join(interrupt);
          expect(stillDraining).toBe(true);
          watcher.close();
          expect(yield* git(repo.cwd, ["rev-parse", "HEAD"])).toBe(repo.base);
          const sql = yield* SqlClient.SqlClient;
          expect((yield* sql`SELECT * FROM agent_control_epic_capture_intents`).length).toBe(0);
        }),
      ),
  );
  it.effect("rejects a modification between successful assessment and capture snapshot", () =>
    testWithRepo((repo) =>
      Effect.gen(function* () {
        const originalAssess = CheckEvidence.assessVerificationChecks;
        const spy = vi
          .spyOn(CheckEvidence, "assessVerificationChecks")
          .mockImplementation((...args) =>
            originalAssess(...args).pipe(
              Effect.tap(() =>
                io(() =>
                  NodeFSP.writeFile(
                    NodePath.join(repo.cwd, "source.txt"),
                    "changed after assessment\n",
                  ),
                ).pipe(Effect.orDie),
              ),
            ),
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
        const hooks = yield* makeEpicResults;
        const error = yield* hooks.capture(captureInput).pipe(Effect.flip);
        expect(error.message).toContain("after its mandatory checks were assessed");
        expect(yield* git(repo.cwd, ["rev-parse", "HEAD"])).toBe(repo.base);
        const sql = yield* SqlClient.SqlClient;
        expect((yield* sql`SELECT * FROM agent_control_epic_capture_intents`).length).toBe(0);
      }),
    ),
  );
  it.effect(
    "captures once under repeated concurrent requests and the next worktree receives A's files",
    () =>
      testWithRepo((repo) =>
        Effect.gen(function* () {
          const hooks = yield* makeEpicResults;
          const [first, repeated] = yield* Effect.all(
            [hooks.capture(captureInput), hooks.capture(captureInput)],
            { concurrency: 2 },
          );
          expect(repeated).toEqual(first);
          expect(yield* git(repo.cwd, ["rev-list", "--count", "HEAD"])).toBe("2");
          expect(yield* git(repo.cwd, ["rev-parse", "HEAD^1"])).toBe(repo.base);
          const next = NodePath.join(repo.root, "next-task");
          yield* git(repo.cwd, ["worktree", "add", "--detach", next, first.commitSha]);
          expect(yield* io(() => NodeFSP.readFile(NodePath.join(next, "source.txt"), "utf8"))).toBe(
            "accepted A\n",
          );
          const sql = yield* SqlClient.SqlClient;
          expect((yield* sql`SELECT * FROM agent_control_epic_capture_results`).length).toBe(1);
          expect((yield* sql`SELECT * FROM agent_control_epic_capture_intents`).length).toBe(1);
        }),
      ),
  );

  it.effect(
    "recovers after publishing the commit but before storing the result using a reopened database",
    () =>
      Effect.gen(function* () {
        const repo = yield* repository;
        const committed = yield* session(
          repo,
          Effect.gen(function* () {
            yield* initialize(repo);
            const sql = yield* SqlClient.SqlClient;
            yield* sql`CREATE TRIGGER simulate_crash BEFORE INSERT ON agent_control_epic_capture_results BEGIN SELECT RAISE(ABORT,'simulated crash'); END`;
            const hooks = yield* makeEpicResults;
            yield* hooks.capture(captureInput).pipe(Effect.flip);
            expect((yield* sql`SELECT * FROM agent_control_epic_capture_intents`).length).toBe(1);
            expect((yield* sql`SELECT * FROM agent_control_epic_capture_results`).length).toBe(0);
            yield* sql`DROP TRIGGER simulate_crash`;
            return yield* git(repo.cwd, ["rev-parse", "HEAD"]);
          }),
        );
        yield* session(
          repo,
          Effect.gen(function* () {
            const hooks = yield* makeEpicResults;
            const result = yield* hooks.capture(captureInput);
            expect(result.commitSha).toBe(committed);
            expect(yield* git(repo.cwd, ["rev-list", "--count", "HEAD"])).toBe("2");
            expect(yield* hooks.capture(captureInput)).toEqual(result);
          }),
        );
      }),
  );

  it.effect("rejects stale verified files without accepting a commit", () =>
    testWithRepo((repo) =>
      Effect.gen(function* () {
        yield* io(() =>
          NodeFSP.writeFile(NodePath.join(repo.cwd, "source.txt"), "changed after green check\n"),
        );
        const hooks = yield* makeEpicResults;
        const error = yield* hooks.capture(captureInput).pipe(Effect.flip);
        expect(error.message).toContain("mandatory checks");
        expect(yield* git(repo.cwd, ["rev-parse", "HEAD"])).toBe(repo.base);
        const sql = yield* SqlClient.SqlClient;
        expect((yield* sql`SELECT * FROM agent_control_epic_capture_results`).length).toBe(0);
      }),
    ),
  );

  it.effect("rejects a previous base that the accepted child did not build on", () =>
    testWithRepo((repo) =>
      Effect.gen(function* () {
        const hooks = yield* makeEpicResults;
        const error = yield* hooks
          .capture({ ...captureInput, previousCommitSha: "another-base" })
          .pipe(Effect.flip);
        expect(error.message).toContain("preceding accepted result");
        expect(yield* git(repo.cwd, ["rev-parse", "HEAD"])).toBe(repo.base);
      }),
    ),
  );

  it.effect(
    "green child checks cannot override a failing common check; repeats retain the same proof",
    () =>
      testWithRepo(() =>
        Effect.gen(function* () {
          let executions = 0;
          const hooks = yield* makeEpicResults.pipe(
            Effect.provideService(EpicCheckExecutor, {
              execute: () =>
                Effect.sync(() => {
                  executions += 1;
                  return failure;
                }),
            }),
          );
          const accepted = yield* hooks.capture(captureInput);
          const input = finalInput(accepted);
          const final = yield* hooks.verify(input);
          expect(final.status).toBe("failed");
          expect(final.commitSha).toBe(accepted.commitSha);
          expect(final.checks[0]?.status).toBe("failed");
          expect(yield* hooks.verify(input)).toEqual(final);
          expect(executions).toBe(1);
          const sql = yield* SqlClient.SqlClient;
          expect(
            (yield* sql`SELECT code FROM agent_control_verification_check_assessments WHERE provider_delivery_id='child-check'`)[0]
              ?.code,
          ).toBe(null);
        }),
      ),
  );

  it.effect(
    "does not rerun an ambiguous final check after its durable start and interruption",
    () =>
      testWithRepo(() =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          let executions = 0;
          const hooks = yield* makeEpicResults.pipe(
            Effect.provideService(EpicCheckExecutor, {
              execute: () =>
                Effect.gen(function* () {
                  executions += 1;
                  yield* Deferred.succeed(entered, undefined);
                  return yield* Effect.never;
                }),
            }),
          );
          const accepted = yield* hooks.capture(captureInput);
          const input = finalInput(accepted);
          const fiber = yield* hooks.verify(input).pipe(Effect.forkChild);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(fiber);
          const recovered = yield* makeEpicResults.pipe(
            Effect.provideService(EpicCheckExecutor, {
              execute: () =>
                Effect.sync(() => {
                  executions += 1;
                  return success;
                }),
            }),
          );
          const final = yield* recovered.verify(input);
          expect(final.status).toBe("blocked");
          expect(final.checks[0]?.status).toBe("missing");
          expect(executions).toBe(1);
          const sql = yield* SqlClient.SqlClient;
          expect(
            (yield* sql`SELECT * FROM agent_control_verification_check_starts WHERE provider_delivery_id=${final.evidenceId}`)
              .length,
          ).toBe(1);
        }),
      ),
  );

  it.effect("requires a configured mandatory check and binds success to the captured commit", () =>
    testWithRepo((repo) =>
      Effect.gen(function* () {
        let executions = 0;
        const hooks = yield* makeEpicResults.pipe(
          Effect.provideService(EpicCheckExecutor, {
            execute: () =>
              Effect.sync(() => {
                executions += 1;
                return success;
              }),
          }),
        );
        const accepted = yield* hooks.capture(captureInput);
        const input = finalInput(accepted);
        const noChecks = yield* hooks.verify({ ...input, checks: [] });
        expect(noChecks.status).toBe("blocked");
        const passed = yield* hooks.verify({ ...input, attempt: 2 });
        expect(passed.status).toBe("passed");
        expect(passed.commitSha).toBe(accepted.commitSha);
        expect(executions).toBe(1);
        yield* io(() =>
          NodeFSP.writeFile(NodePath.join(repo.cwd, "source.txt"), "modified after final check\n"),
        );
        const error = yield* hooks.verify({ ...input, attempt: 2 }).pipe(Effect.flip);
        expect(error.message).toContain("common result changed");
        expect(executions).toBe(1);
      }),
    ),
  );
});
