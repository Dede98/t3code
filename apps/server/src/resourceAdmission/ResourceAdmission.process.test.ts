// @effect-diagnostics nodeBuiltinImport:off - integration observes an actual managed process group.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, type AgentControlVerificationChecks } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import type { ProviderAdmissionPermit } from "../agentControl/providerAdmission/model.ts";
import {
  executeVerificationCheck,
  prepareVerificationCheckManifest,
  VerificationCheckError,
} from "../agentControl/verificationTurn/checkEvidence.ts";
import Migration076 from "../persistence/Migrations/076_AgentControlVerificationChecks.ts";
import Migration088 from "../persistence/Migrations/088_SharedProviderResourceAdmission.ts";
import * as NodeSqliteClient from "../persistence/NodeSqliteClient.ts";
import { runVerificationSandboxCheck } from "../provider/VerificationSandbox.ts";
import { makeMemoryHostBudgetLedger } from "./HostBudgetLedger.ts";
import { make, ResourceAdmission } from "./ResourceAdmission.ts";
import { ResourcePressure } from "./ResourcePressure.ts";
import { defaultResourceAdmissionSettings, type ResourceAdmissionRequest } from "./model.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

const fixture = Effect.acquireRelease(
  Effect.tryPromise({
    try: async () => {
      const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-resource-process-"));
      const worktree = NodePath.join(root, "worktree");
      const temporary = NodePath.join(root, "temporary");
      await NodeFSP.mkdir(worktree);
      await NodeFSP.mkdir(temporary);
      await NodeFSP.writeFile(
        NodePath.join(worktree, "check.cjs"),
        `
          const fs = require("node:fs");
          const path = require("node:path");
          const { spawn } = require("node:child_process");
          const child = spawn(process.execPath, ["-e", "process.stdin.resume()"], {
            stdio: ["pipe", "ignore", "ignore"]
          });
          const pending = path.join(process.env.TMPDIR, "processes.pending.json");
          const ready = path.join(process.env.TMPDIR, "processes.json");
          fs.writeFileSync(pending, JSON.stringify({ rootPid: process.pid, childPid: child.pid }));
          fs.renameSync(pending, ready);
          process.stdin.resume();
        `,
      );
      await execFile("git", ["init", "--quiet"], { cwd: worktree });
      await execFile("git", ["add", "check.cjs"], { cwd: worktree });
      await execFile(
        "git",
        [
          "-c",
          "user.name=Test",
          "-c",
          "user.email=test@example.invalid",
          "commit",
          "-qm",
          "fixture",
        ],
        { cwd: worktree },
      );
      return { root, worktree, temporary };
    },
    catch: (cause) => new VerificationCheckError({ cause }),
  }),
  ({ root }) => Effect.promise(() => NodeFSP.rm(root, { recursive: true, force: true })),
);

interface ProcessTreeReceipt {
  readonly rootPid: number;
  readonly childPid: number;
  readonly directory: string;
}

const awaitProcessTreeReceipt = (temporary: string) =>
  Effect.callback<ProcessTreeReceipt, VerificationCheckError>((resume) => {
    let settled = false;
    const inspect = async () => {
      try {
        for (const entry of await NodeFSP.readdir(temporary, { withFileTypes: true })) {
          if (!entry.isDirectory() || !entry.name.startsWith("check-")) continue;
          const marker = NodePath.join(temporary, entry.name, "processes.json");
          const content = await NodeFSP.readFile(marker, "utf8").catch(
            (cause: NodeJS.ErrnoException) => {
              if (cause.code === "ENOENT") return null;
              throw cause;
            },
          );
          if (content === null || settled) continue;
          const receipt = JSON.parse(content) as Omit<ProcessTreeReceipt, "directory">;
          if (!Number.isSafeInteger(receipt.rootPid) || !Number.isSafeInteger(receipt.childPid)) {
            throw new Error("verification process receipt contains invalid pids");
          }
          settled = true;
          watcher.close();
          resume(Effect.succeed({ ...receipt, directory: NodePath.join(temporary, entry.name) }));
          return;
        }
      } catch (cause) {
        if (settled) return;
        settled = true;
        watcher.close();
        resume(Effect.fail(new VerificationCheckError({ cause })));
      }
    };
    const watcher = NodeFS.watch(temporary, { recursive: true }, () => void inspect());
    void inspect();
    return Effect.sync(() => {
      settled = true;
      watcher.close();
    });
  });

const awaitDirectoryRemoval = (path: string) =>
  Effect.callback<void, VerificationCheckError>((resume) => {
    let settled = false;
    const inspect = async () => {
      try {
        await NodeFSP.access(path);
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") {
          if (settled) return;
          settled = true;
          watcher.close();
          resume(Effect.fail(new VerificationCheckError({ cause })));
          return;
        }
        if (settled) return;
        settled = true;
        watcher.close();
        resume(Effect.void);
      }
    };
    const watcher = NodeFS.watch(NodePath.dirname(path), () => void inspect());
    void inspect();
    return Effect.sync(() => {
      settled = true;
      watcher.close();
    });
  });

const localCheck = (requestId: string): ResourceAdmissionRequest => ({
  requestId,
  kind: "localCheck",
  priority: "background",
  ownerId: `owner-${requestId}`,
  ownerFenceToken: 1,
  executionKey: `check-process-${requestId}`,
});

const permit: ProviderAdmissionPermit = {
  admissionId: "admission-process",
  admissionMarkerId: "marker",
  admissionMarkerFingerprint: "fingerprint",
  stage: "verification",
  projectId: "project",
  taskId: "task",
  stageRunId: "stage-process",
  attemptId: "attempt",
  handoffId: "handoff-process",
  providerDeliveryId: "verification-process",
  threadId: "thread",
  providerInstanceId: ProviderInstanceId.make("codex"),
  stageLeaseId: "lease",
  stageLeaseHolderId: "controller",
  stageFenceToken: 1,
  admissionOwnerId: "owner",
  admissionLeaseExpiresAt: "2099-01-01T00:00:00.000Z",
  providerFenceToken: 1,
  modelSelectionJson: "{}",
  modelSelectionFingerprint: "model",
  usageEvidenceFingerprint: "usage",
};

const checks: AgentControlVerificationChecks = [
  {
    id: "process-tree",
    command: process.execPath,
    args: ["check.cjs"],
    cwd: ".",
    required: true,
    timeoutMs: 10_000,
    allowTemporaryFiles: true,
    resultFormat: "exit-code",
  },
];

it.effect.skipIf(HostProcessPlatform.defaultValue() !== "darwin")(
  "cancels a productive verification process tree and grants its local slot once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* fixture;
        const ledger = yield* makeMemoryHostBudgetLedger();
        const admission = yield* make({
          ledger,
          settings: { ...defaultResourceAdmissionSettings, localCheckMaxConcurrent: 1 },
        }).pipe(
          Effect.provideService(
            ResourcePressure,
            ResourcePressure.of({
              sample: Effect.succeed({
                sampledAtMs: 1,
                telemetry: "available",
                cpuUtilization: 0.1,
                availableMemoryBytes: 8 * 1024 * 1024 * 1024,
                gpu: { status: "unavailable" },
              }),
              awaitChange: () => Effect.never,
            }),
          ),
        );
        let releaseCount = 0;
        let waiterGrantCount = 0;
        const observedAdmission = ResourceAdmission.of({
          ...admission,
          observeActivity: (authority, activity) =>
            admission.observeActivity(authority, activity).pipe(
              Effect.tap((update) =>
                Effect.sync(() => {
                  if (
                    authority.reservationId === "verification:verification-process:process-tree" &&
                    activity === "inactive" &&
                    update.result
                  ) {
                    releaseCount += 1;
                  }
                  waiterGrantCount += update.newlyAdmitted.filter(
                    (grant) => grant.requestId === "waiting-check",
                  ).length;
                }),
              ),
            ),
        });
        const database = NodeSqliteClient.layerMemory();
        yield* Effect.gen(function* () {
          yield* Migration076;
          yield* Migration088;
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE agent_control_project_policies (project_id TEXT PRIMARY KEY, policy_json TEXT NOT NULL)`;
          yield* sql`INSERT INTO agent_control_project_policies VALUES ('project', '{}')`;
          const manifest = yield* prepareVerificationCheckManifest(sql, {
            permit,
            cwd: test.worktree,
            checks,
          });
          const execution = yield* executeVerificationCheck(sql, {
            manifest,
            checkId: "process-tree",
            providerTurnId: "turn-process",
            authorize: Effect.void,
            execute: Effect.tryPromise({
              try: (signal) =>
                runVerificationSandboxCheck({
                  check: checks[0]!,
                  worktreePath: test.worktree,
                  temporaryDirectory: test.temporary,
                  signal,
                }),
              catch: (cause) => new VerificationCheckError({ cause }),
            }),
          }).pipe(Effect.forkChild({ startImmediately: true }));

          const processes = yield* awaitProcessTreeReceipt(test.temporary);
          assert.doesNotThrow(() => process.kill(processes.rootPid, 0));
          assert.doesNotThrow(() => process.kill(processes.childPid, 0));

          const waiting = localCheck("waiting-check");
          assert.equal((yield* admission.request(waiting)).result._tag, "Waiting");
          const waitingFiber = yield* admission
            .acquire(waiting)
            .pipe(Effect.forkChild({ startImmediately: true }));
          const cleanupFiber = yield* awaitDirectoryRemoval(processes.directory).pipe(
            Effect.forkChild({ startImmediately: true }),
          );

          yield* Fiber.interrupt(execution);
          yield* Fiber.join(cleanupFiber);
          assert.throws(() => process.kill(processes.rootPid, 0));
          assert.throws(() => process.kill(processes.childPid, 0));
          const admitted = yield* Fiber.join(waitingFiber);
          assert.equal(admitted._tag, "Admitted");
          if (admitted._tag !== "Admitted") return yield* Effect.die("expected waiter admission");

          assert.equal(releaseCount, 1);
          assert.equal(waiterGrantCount, 1);
          assert.deepEqual(yield* admission.refresh, []);
          yield* admission.release(admitted.authority);
        }).pipe(
          Effect.provide(database),
          Effect.provideService(ResourceAdmission, observedAdmission),
        );
      }),
    ),
);
