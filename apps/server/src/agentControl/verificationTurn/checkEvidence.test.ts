// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, type AgentControlVerificationChecks } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import Migration076 from "../../persistence/Migrations/076_AgentControlVerificationChecks.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import type { ProviderAdmissionPermit } from "../providerAdmission/model.ts";
import {
  assessVerificationChecks,
  executeVerificationCheck,
  VerificationCheckError,
  prepareVerificationCheckManifest,
  sealVerificationCheckAssessment,
  type VerificationCheckClaim,
  type VerificationCheckCommandResult,
  type VerificationCheckManifest,
} from "./checkEvidence.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const io = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new VerificationCheckError({ cause }) });
const repository = Effect.acquireRelease(
  io(async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3auto-check-evidence-"));
    const cwd = NodePath.join(root, "repo");
    await NodeFSP.mkdir(cwd);
    await NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "original\n");
    await exec("git", ["init", "--quiet"], { cwd });
    await exec("git", ["add", "source.txt"], { cwd });
    await exec(
      "git",
      ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"],
      { cwd },
    );
    return {
      root,
      cwd,
      database: NodePath.join(root, "evidence.sqlite"),
      counter: NodePath.join(root, "count.txt"),
    };
  }),
  ({ root }) => io(() => NodeFSP.rm(root, { recursive: true, force: true })).pipe(Effect.orDie),
);
const checks: AgentControlVerificationChecks = [
  {
    id: "scoped-test",
    command: process.execPath,
    args: ["--test", "scoped.test.cjs"],
    cwd: ".",
    required: true,
    timeoutMs: 10_000,
    allowTemporaryFiles: true,
    resultFormat: "node-test",
  },
];
const permit = (delivery = "verification-1", fence = 3): ProviderAdmissionPermit => ({
  admissionId: `admission-${delivery}`,
  admissionMarkerId: "marker",
  admissionMarkerFingerprint: "fingerprint",
  stage: "verification",
  projectId: "project",
  taskId: "task",
  stageRunId: `stage-${delivery}`,
  attemptId: "attempt",
  handoffId: `handoff-${delivery}`,
  providerDeliveryId: delivery,
  threadId: "thread",
  providerInstanceId: ProviderInstanceId.make("codex"),
  stageLeaseId: "lease",
  stageLeaseHolderId: "controller",
  stageFenceToken: fence,
  admissionOwnerId: "owner",
  admissionLeaseExpiresAt: "2099-01-01T00:00:00.000Z",
  providerFenceToken: 1,
  modelSelectionJson: "{}",
  modelSelectionFingerprint: "model",
  usageEvidenceFingerprint: "usage",
});
const claim = (manifest: VerificationCheckManifest, turn = "turn-1"): VerificationCheckClaim => ({
  evidence: manifest,
  delivery: { providerTurnId: turn },
});
const initialize = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* Migration076;
  yield* sql`CREATE TABLE agent_control_project_policies (project_id TEXT PRIMARY KEY, policy_json TEXT NOT NULL)`;
  yield* sql`INSERT INTO agent_control_project_policies VALUES ('project', '{}')`;
});
const success: VerificationCheckCommandResult = {
  exitCode: 0,
  stdout: "# pass 1\n# fail 0\n# cancelled 0\n",
  stderr: "",
};
const execute = (sql: SqlClient.SqlClient, manifest: VerificationCheckManifest, result = success) =>
  executeVerificationCheck(sql, {
    manifest,
    checkId: "scoped-test",
    providerTurnId: "turn-1",
    authorize: Effect.void,
    execute: Effect.succeed(result),
  });

it.effect(
  "reopens persisted results and a sealed assessment without executing the command twice",
  () =>
    Effect.gen(function* () {
      const repo = yield* repository;
      const command = io(async () => {
        const result = await exec(
          process.execPath,
          ["-e", "require('node:fs').appendFileSync(process.argv[1], 'executed\\n')", repo.counter],
          { cwd: repo.cwd },
        );
        return { exitCode: 0, stdout: success.stdout + result.stdout, stderr: result.stderr };
      });
      const first = yield* Effect.gen(function* () {
        yield* initialize;
        const sql = yield* SqlClient.SqlClient;
        const manifest = yield* prepareVerificationCheckManifest(sql, {
          permit: permit(),
          cwd: repo.cwd,
          checks,
        });
        yield* executeVerificationCheck(sql, {
          manifest,
          checkId: "scoped-test",
          providerTurnId: "turn-1",
          authorize: Effect.void,
          execute: command,
        });
        return manifest;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: repo.database })));
      const sealed = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const manifest = yield* prepareVerificationCheckManifest(sql, {
          permit: permit(),
          cwd: repo.cwd,
          checks: [],
        });
        assert.deepStrictEqual(manifest, first);
        assert.equal(
          (yield* executeVerificationCheck(sql, {
            manifest,
            checkId: "scoped-test",
            providerTurnId: "turn-1",
            authorize: Effect.void,
            execute: command,
          })).exitCode,
          0,
        );
        const assessment = yield* sealVerificationCheckAssessment(sql, claim(manifest));
        assert.isNull(assessment.code);
        return assessment;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: repo.database })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* sealVerificationCheckAssessment(sql, claim(first)), sealed);
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sealVerificationCheckAssessment(
                sql,
                claim({ ...first, fenceToken: first.fenceToken + 1 }),
              ),
            ),
          ),
        );
        for (const mutation of [
          sql`UPDATE agent_control_verification_check_manifests SET code_digest = 'forged'`,
          sql`DELETE FROM agent_control_verification_check_starts`,
          sql`UPDATE agent_control_verification_check_results SET status = 'passed'`,
          sql`DELETE FROM agent_control_verification_check_assessments`,
        ]) {
          assert.isTrue(Exit.isFailure(yield* Effect.exit(mutation)));
        }
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count FROM agent_control_verification_check_results`,
          [{ count: 1 }],
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: repo.database })));
      assert.equal(yield* io(() => NodeFSP.readFile(repo.counter, "utf8")), "executed\n");
    }).pipe(Effect.scoped),
);

it.effect("never retries a persisted start whose completion was lost at restart", () =>
  Effect.gen(function* () {
    const repo = yield* repository;
    const manifest = yield* Effect.gen(function* () {
      yield* initialize;
      const sql = yield* SqlClient.SqlClient;
      const manifest = yield* prepareVerificationCheckManifest(sql, {
        permit: permit(),
        cwd: repo.cwd,
        checks,
      });
      yield* sql`INSERT INTO agent_control_verification_check_starts VALUES (${manifest.providerDeliveryId}, 'scoped-test', 'turn-1', ${manifest.manifestDigest}, '2026-09-10T00:00:00Z')`;
      return manifest;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: repo.database })));
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      let executions = 0;
      const result = yield* executeVerificationCheck(sql, {
        manifest,
        checkId: "scoped-test",
        providerTurnId: "turn-1",
        authorize: Effect.void,
        execute: Effect.sync(() => {
          executions += 1;
          return success;
        }),
      });
      assert.equal(executions, 0);
      assert.equal(result.exitCode, 125);
      assert.equal(
        (yield* assessVerificationChecks(sql, claim(manifest))).code,
        "verification-checks-missing",
      );
      assert.deepStrictEqual(
        yield* sql`SELECT count(*) AS count FROM agent_control_verification_check_results`,
        [{ count: 0 }],
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: repo.database })));
  }).pipe(Effect.scoped),
);

it.effect(
  "requires every mandatory check and distinguishes assertion failures from unavailable runners",
  () =>
    Effect.gen(function* () {
      const repo = yield* repository;
      yield* Effect.gen(function* () {
        yield* initialize;
        const sql = yield* SqlClient.SqlClient;
        const cases = [
          ["missing", undefined, "verification-checks-missing"],
          [
            "failed",
            { exitCode: 1, stdout: "  code: 'ERR_ASSERTION'\n# fail 1\n", stderr: "" },
            "verification-checks-failed",
          ],
          [
            "dependencies",
            { exitCode: 1, stdout: "", stderr: "ERR_MODULE_NOT_FOUND" },
            "verification-checks-unavailable",
          ],
          [
            "timeout",
            { exitCode: 124, stdout: "", stderr: "timeout" },
            "verification-checks-unavailable",
          ],
          [
            "runner",
            { exitCode: 1, stdout: "", stderr: "No test files found" },
            "verification-checks-unavailable",
          ],
          ["passed", success, null],
        ] as const;
        for (const [id, result, code] of cases) {
          const manifest = yield* prepareVerificationCheckManifest(sql, {
            permit: permit(id),
            cwd: repo.cwd,
            checks,
          });
          if (result) yield* execute(sql, manifest, result);
          assert.equal((yield* assessVerificationChecks(sql, claim(manifest))).code, code);
        }
        const multiple = yield* prepareVerificationCheckManifest(sql, {
          permit: permit("multiple"),
          cwd: repo.cwd,
          checks: [...checks, { ...checks[0]!, id: "second-test" }],
        });
        yield* execute(sql, multiple);
        assert.equal(
          (yield* assessVerificationChecks(sql, claim(multiple))).code,
          "verification-checks-missing",
        );
        const optional = yield* prepareVerificationCheckManifest(sql, {
          permit: permit("optional"),
          cwd: repo.cwd,
          checks: checks.map((check) => ({ ...check, required: false })),
        });
        yield* execute(sql, optional);
        assert.equal(
          (yield* assessVerificationChecks(sql, claim(optional))).code,
          "verification-checks-missing",
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory()));
    }).pipe(Effect.scoped),
);

it.effect(
  "prioritizes incomplete or invalid evidence over code failures regardless of check order",
  () =>
    Effect.gen(function* () {
      const repo = yield* repository;
      yield* Effect.gen(function* () {
        yield* initialize;
        const sql = yield* SqlClient.SqlClient;
        const pairs = [
          ["failed", "missing", "verification-checks-missing"],
          ["failed", "unavailable", "verification-checks-unavailable"],
          ["failed", "stale", "verification-checks-stale"],
          ["missing", "unavailable", "verification-checks-unavailable"],
          ["missing", "stale", "verification-checks-stale"],
          ["unavailable", "stale", "verification-checks-stale"],
        ] as const;
        for (const [first, second, expected] of pairs) {
          for (const statuses of [
            [first, second],
            [second, first],
          ]) {
            const manifest = yield* prepareVerificationCheckManifest(sql, {
              permit: permit(statuses.join("-")),
              cwd: repo.cwd,
              checks: statuses.map((status) => ({ ...checks[0]!, id: status })),
            });
            for (const status of statuses) {
              if (status === "missing") continue;
              yield* executeVerificationCheck(sql, {
                manifest,
                checkId: status,
                providerTurnId: status === "stale" ? "foreign-turn" : "turn-1",
                authorize: Effect.void,
                execute: Effect.succeed(
                  status === "failed"
                    ? { exitCode: 1, stdout: "  code: 'ERR_ASSERTION'\n# fail 1\n", stderr: "" }
                    : status === "unavailable"
                      ? { exitCode: 124, stdout: "", stderr: "timeout" }
                      : success,
                ),
              });
            }
            assert.equal(
              (yield* assessVerificationChecks(sql, claim(manifest))).code,
              expected,
              statuses.join(" then "),
            );
          }
        }
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory()));
    }).pipe(Effect.scoped),
);

it.effect("rejects foreign turns, handoffs, fences and pre-repair deliveries", () =>
  Effect.gen(function* () {
    const repo = yield* repository;
    yield* Effect.gen(function* () {
      yield* initialize;
      const sql = yield* SqlClient.SqlClient;
      const manifest = yield* prepareVerificationCheckManifest(sql, {
        permit: permit(),
        cwd: repo.cwd,
        checks,
      });
      yield* execute(sql, manifest);
      assert.equal(
        (yield* assessVerificationChecks(sql, claim(manifest, "foreign-turn"))).code,
        "verification-checks-stale",
      );
      for (const evidence of [
        { ...manifest, handoffId: "foreign-handoff" },
        { ...manifest, fenceToken: 5 },
      ]) {
        assert.equal(
          (yield* assessVerificationChecks(sql, {
            evidence,
            delivery: { providerTurnId: "turn-1" },
          })).code,
          "verification-checks-stale",
        );
      }
      const repaired = yield* prepareVerificationCheckManifest(sql, {
        permit: permit("after-repair", 5),
        cwd: repo.cwd,
        checks,
      });
      assert.equal(
        (yield* assessVerificationChecks(sql, claim(repaired))).code,
        "verification-checks-missing",
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            prepareVerificationCheckManifest(sql, {
              permit: permit("verification-1", 5),
              cwd: repo.cwd,
              checks,
            }),
          ),
        ),
      );
      let executions = 0;
      const replay = yield* executeVerificationCheck(sql, {
        manifest,
        checkId: "scoped-test",
        providerTurnId: "foreign-turn",
        authorize: Effect.void,
        execute: Effect.sync(() => {
          executions += 1;
          return success;
        }),
      });
      assert.equal(replay.exitCode, 125);
      assert.equal(executions, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory()));
  }).pipe(Effect.scoped),
);

it.effect("invalidates results when tracked or untracked code changes after execution", () =>
  Effect.gen(function* () {
    const repo = yield* repository;
    yield* Effect.gen(function* () {
      yield* initialize;
      const sql = yield* SqlClient.SqlClient;
      const manifest = yield* prepareVerificationCheckManifest(sql, {
        permit: permit(),
        cwd: repo.cwd,
        checks,
      });
      yield* execute(sql, manifest);
      yield* io(() => NodeFSP.writeFile(NodePath.join(repo.cwd, "source.txt"), "repaired\n"));
      assert.equal(
        (yield* assessVerificationChecks(sql, claim(manifest))).code,
        "verification-checks-stale",
      );
      yield* io(() => NodeFSP.writeFile(NodePath.join(repo.cwd, "source.txt"), "original\n"));
      yield* io(() => NodeFSP.writeFile(NodePath.join(repo.cwd, "untracked.txt"), "new code\n"));
      assert.equal(
        (yield* assessVerificationChecks(sql, claim(manifest))).code,
        "verification-checks-stale",
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory()));
  }).pipe(Effect.scoped),
);

it.effect(
  "rejects unauthorized check IDs and records code or authorization changes during a check",
  () =>
    Effect.gen(function* () {
      const repo = yield* repository;
      yield* Effect.gen(function* () {
        yield* initialize;
        const sql = yield* SqlClient.SqlClient;
        const manifest = yield* prepareVerificationCheckManifest(sql, {
          permit: permit(),
          cwd: repo.cwd,
          checks,
        });
        let executions = 0;
        const command = Effect.sync(() => {
          executions += 1;
          return success;
        });
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              executeVerificationCheck(sql, {
                manifest,
                checkId: "arbitrary-shell",
                providerTurnId: "turn-1",
                authorize: Effect.void,
                execute: command,
              }),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              executeVerificationCheck(sql, {
                manifest,
                checkId: "scoped-test",
                providerTurnId: "turn-1",
                authorize: Effect.fail(new VerificationCheckError({ cause: "fenced" })),
                execute: command,
              }),
            ),
          ),
        );
        assert.equal(executions, 0);
        let authorizations = 0;
        const authorization = Effect.suspend(() =>
          ++authorizations === 1
            ? Effect.void
            : Effect.fail(new VerificationCheckError({ cause: "lease lost" })),
        );
        assert.equal(
          (yield* executeVerificationCheck(sql, {
            manifest,
            checkId: "scoped-test",
            providerTurnId: "turn-1",
            authorize: authorization,
            execute: command,
          })).exitCode,
          125,
        );
        assert.equal(
          (yield* assessVerificationChecks(sql, claim(manifest))).code,
          "verification-checks-unavailable",
        );
        assert.equal((yield* execute(sql, manifest)).exitCode, 125);
        const changed = yield* prepareVerificationCheckManifest(sql, {
          permit: permit("changed"),
          cwd: repo.cwd,
          checks,
        });
        assert.equal(
          (yield* executeVerificationCheck(sql, {
            manifest: changed,
            checkId: "scoped-test",
            providerTurnId: "turn-1",
            authorize: Effect.void,
            execute: io(async () => {
              await NodeFSP.writeFile(
                NodePath.join(repo.cwd, "source.txt"),
                "changed during test\n",
              );
              return success;
            }),
          })).exitCode,
          125,
        );
        assert.equal(
          (yield* assessVerificationChecks(sql, claim(changed))).code,
          "verification-checks-stale",
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory()));
    }).pipe(Effect.scoped),
);
