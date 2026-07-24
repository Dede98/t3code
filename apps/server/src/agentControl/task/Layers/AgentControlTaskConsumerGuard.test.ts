import {
  type AgentControlGithubIssueSnapshot,
  AgentControlTaskId,
  type AgentControlTaskState,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlTaskConsumerGuard } from "../Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { layer } from "./AgentControlTaskConsumerGuard.ts";

const projectId = ProjectId.make("task-consumer-guard");
const at = "2026-07-23T00:00:00.000Z";
const task = (sequence: number): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId: AgentControlTaskId.make("task-consumer-guard-task"),
  source: {
    projectId,
    repositoryNodeId: "repository-node",
    issueNodeId: "issue-node",
    issueNumber: 1,
    issueUrl: "https://example.test/issues/1",
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: at,
  githubIntakeSequence: sequence,
  sourceSnapshot: {
    repositoryNodeId: "repository-node",
    issueNodeId: "issue-node",
    number: 1,
    url: "https://example.test/issues/1",
    state: "open",
    title: "Untrusted title",
    body: null,
    contentTrust: "untrusted-external",
    updatedAt: at,
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  },
  createdAt: at,
  updatedAt: at,
  revision: 1,
  sequence: 1,
});
const issue: AgentControlGithubIssueSnapshot = {
  ...task(5).sourceSnapshot,
  timelineEvents: [],
} as const;

const makeGuard = (input?: {
  readonly available?: boolean;
  readonly mode?: "manual" | "observe" | "paused";
  readonly sourceSequence?: number | null;
  readonly watermarkStatus?: "reconciling" | "completed" | "recovery-required" | null;
  readonly targetSequence?: number;
  readonly lastCompletedSequence?: number;
  readonly tasks?: ReadonlyArray<AgentControlTaskState | "corrupt">;
  readonly issues?: ReadonlyArray<AgentControlGithubIssueSnapshot>;
  readonly getCorrupt?: boolean;
}) =>
  AgentControlTaskConsumerGuard.pipe(
    Effect.provide(layer),
    Effect.provideService(AgentControlProjectAvailability, {
      ensureAvailable: () =>
        input?.available === false
          ? Effect.fail({
              _tag: "AgentControlProjectUnavailableError",
              projectId,
              reason: "deleted",
            } as never)
          : Effect.void,
    }),
    Effect.provideService(AgentControlProjectStateRepository, {
      get: () =>
        Effect.succeed(
          Option.some({
            schemaVersion: 1,
            projectId,
            mode: input?.mode ?? "observe",
            pausedFromMode: null,
            revision: 1,
            sequence: 1,
            updatedAt: at,
          }),
        ),
      save: () => Effect.die("unused"),
      listPersisted: Effect.die("unused"),
      deleteAll: Effect.die("unused"),
    }),
    Effect.provideService(AgentControlGithubStateRepository, {
      get: () => Effect.die("unused"),
      save: () => Effect.die("unused"),
      replaceIssues: () => Effect.die("unused"),
      listIssues: () => Effect.die("unused"),
      getCompletedSnapshot: () => {
        const sourceSequence = input?.sourceSequence === undefined ? 5 : input.sourceSequence;
        const issues = input?.issues ?? [issue];
        return Effect.succeed(
          sourceSequence === null
            ? Option.none()
            : Option.some({
                sourcePrecondition: {
                  schemaVersion: 1,
                  projectId,
                  githubIntakeSequence: sourceSequence,
                  githubProjectionRevision: 3,
                  githubConfigRevision: 2,
                  repositoryNodeId: "repository-node",
                  pollStatus: "success",
                  expectedIssueCount: issues.length,
                },
                issues,
              }),
        );
      },
      matchesCompletedSnapshot: () => Effect.die("unused"),
      deleteProject: () => Effect.die("unused"),
      deleteAll: Effect.die("unused"),
    }),
    Effect.provideService(AgentControlTaskReconcileStateRepository, {
      get: () => {
        const status = input?.watermarkStatus === undefined ? "completed" : input.watermarkStatus;
        return Effect.succeed(
          status === null
            ? Option.none()
            : Option.some({
                schemaVersion: 1,
                projectId,
                targetSequence: input?.targetSequence ?? 5,
                lastCompletedSequence: input?.lastCompletedSequence ?? 5,
                revision: 1,
                status,
                updatedAt: at,
              }),
        );
      },
      begin: () => Effect.die("unused"),
      markRecoveryRequired: () => Effect.die("unused"),
      complete: () => Effect.die("unused"),
    }),
    Effect.provideService(AgentControlTaskStateRepository, {
      get: (taskId) => {
        if (input?.getCorrupt === true) {
          return Effect.fail({ _tag: "AgentControlPersistenceDecodeError" } as never);
        }
        const entry = (input?.tasks ?? [task(5)]).find(
          (candidate) => candidate !== "corrupt" && candidate.taskId === taskId,
        );
        return Effect.succeed(
          entry === undefined || entry === "corrupt" ? Option.none() : Option.some(entry),
        );
      },
      save: () => Effect.die("unused"),
      listProject: () =>
        Effect.succeed(
          (input?.tasks ?? [task(5)]).map((entry) =>
            entry === "corrupt"
              ? { _tag: "Corrupt" as const, taskId: null, projectId }
              : { _tag: "Valid" as const, state: entry },
          ),
        ),
      listAll: Effect.die("unused"),
      findByIdentity: () => Effect.die("unused"),
      findBySourceNumber: () => Effect.die("unused"),
      deleteAll: Effect.die("unused"),
    }),
  );

const sqlite = it.layer(NodeSqliteClient.layerMemory());

sqlite("AgentControl task consumer guard", (it) => {
  it.effect("accepts only a completed, exact current project and task sequence", () =>
    Effect.gen(function* () {
      const guard = yield* makeGuard();
      const current = yield* guard.useTaskConsumable(projectId, task(5).taskId, (_task, gate) =>
        Effect.succeed(gate),
      );
      assert.isTrue(current.sequenceCurrent);
      assert.equal(current.currentSourceSequence, 5);
    }),
  );

  it.effect("future consumer guard rejects every inactive or stale correctness boundary", () =>
    Effect.gen(function* () {
      const cases = [
        { expected: "project-unavailable", input: { available: false } },
        { expected: "mode-inactive", input: { mode: "manual" as const } },
        { expected: "mode-inactive", input: { mode: "paused" as const } },
        { expected: "source-snapshot-unavailable", input: { sourceSequence: null } },
        { expected: "watermark-missing", input: { watermarkStatus: null } },
        {
          expected: "watermark-sequence-mismatch",
          input: { targetSequence: 5, lastCompletedSequence: 4 },
        },
        {
          expected: "watermark-not-completed",
          input: { watermarkStatus: "recovery-required" as const },
        },
        {
          expected: "task-sequence-mismatch",
          input: { sourceSequence: 6 },
        },
        {
          expected: "task-projection-corrupt",
          input: { tasks: ["corrupt" as const] },
        },
      ] as const;

      for (const testCase of cases) {
        const guard = yield* makeGuard(testCase.input);
        const result = yield* Effect.result(
          guard.useTaskConsumable(projectId, task(5).taskId, () => Effect.void),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.reason, testCase.expected);
      }

      const stale = task(4);
      const staleGuard = yield* makeGuard({ tasks: [stale] });
      const taskMismatch = yield* Effect.result(
        staleGuard.useTaskConsumable(projectId, stale.taskId, () => Effect.void),
      );
      assert.equal(taskMismatch._tag, "Failure");
      if (taskMismatch._tag === "Failure") {
        assert.equal(taskMismatch.failure.reason, "task-sequence-mismatch");
      }
    }),
  );

  it.effect("loads the concrete task canonically and rejects every task-local mismatch", () =>
    Effect.gen(function* () {
      const otherProject = ProjectId.make("task-consumer-guard-other");
      const cases = [
        {
          expected: "task-missing",
          taskId: AgentControlTaskId.make("invented-task"),
          input: {},
        },
        {
          expected: "task-project-mismatch",
          taskId: task(5).taskId,
          input: {
            tasks: [{ ...task(5), source: { ...task(5).source, projectId: otherProject } }],
          },
        },
        {
          expected: "task-status-inactive",
          taskId: task(5).taskId,
          input: { tasks: [{ ...task(5), status: "running" as const }] },
        },
        {
          expected: "task-source-ineligible",
          taskId: task(5).taskId,
          input: { tasks: [{ ...task(5), sourceGate: "paused" as const }] },
        },
        {
          expected: "task-source-mismatch",
          taskId: task(5).taskId,
          input: {
            tasks: [
              {
                ...task(5),
                source: { ...task(5).source, issueNodeId: "not-in-snapshot" },
                sourceSnapshot: {
                  ...task(5).sourceSnapshot,
                  issueNodeId: "not-in-snapshot",
                },
              },
            ],
          },
        },
        {
          expected: "task-source-mismatch",
          taskId: task(5).taskId,
          input: {
            tasks: [
              {
                ...task(5),
                sourceSnapshot: { ...task(5).sourceSnapshot, title: "stale title" },
              },
            ],
          },
        },
        {
          expected: "task-projection-corrupt",
          taskId: task(5).taskId,
          input: { getCorrupt: true },
        },
      ] as const;

      for (const testCase of cases) {
        const guard = yield* makeGuard(testCase.input);
        const result = yield* Effect.result(
          guard.useTaskConsumable(projectId, testCase.taskId, () => Effect.void),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.reason, testCase.expected);
      }
    }),
  );

  it.effect(
    "keeps project inspection current with zero tasks and composes a claim in an outer transaction",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const emptyGuard = yield* makeGuard({ tasks: [] });
        const empty = yield* emptyGuard.inspectProject(projectId);
        assert.isTrue(empty.sequenceCurrent);
        const missing = yield* Effect.result(
          emptyGuard.useTaskConsumable(projectId, task(5).taskId, () => Effect.void),
        );
        assert.equal(missing._tag, "Failure");
        if (missing._tag === "Failure") assert.equal(missing.failure.reason, "task-missing");

        yield* sql`CREATE TABLE task_claim_probe (task_id TEXT PRIMARY KEY)`;
        const guard = yield* makeGuard({
          issues: [
            issue,
            {
              ...issue,
              issueNodeId: "ineligible-issue",
              number: 2,
              url: "https://example.test/issues/2",
              ready: false,
              eligible: false,
              eligibilityReason: "ready-inactive",
            },
          ],
        });
        yield* sql.withTransaction(
          guard.useTaskConsumable(
            projectId,
            task(5).taskId,
            (canonicalTask) =>
              sql`INSERT INTO task_claim_probe (task_id) VALUES (${canonicalTask.taskId})`,
          ),
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM task_claim_probe
          `)[0]?.count,
          1,
        );
      }),
  );

  it.effect("interrupts an unjoined child before the guarded transaction can commit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_escape (task_id TEXT PRIMARY KEY)`;
      const release = yield* Deferred.make<void>();
      const childStarted = yield* Deferred.make<void>();
      const childFinalized = yield* Deferred.make<void>();
      const guard = yield* makeGuard();

      yield* guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
        Deferred.await(release).pipe(
          Effect.andThen(
            sql`INSERT INTO task_claim_escape (task_id) VALUES (${canonicalTask.taskId})`,
          ),
          Effect.ensuring(Deferred.succeed(childFinalized, undefined).pipe(Effect.ignore)),
          Effect.forkChild({ startImmediately: true }),
          Effect.tap(() => Deferred.succeed(childStarted, undefined)),
          Effect.asVoid,
        ),
      );
      yield* Deferred.await(childStarted);
      assert.isTrue(yield* Deferred.isDone(childFinalized));
      yield* Deferred.succeed(release, undefined);
      yield* Effect.yieldNow;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_escape
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("commits a child claim that the callback explicitly joins", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_joined (task_id TEXT PRIMARY KEY)`;
      const guard = yield* makeGuard();

      yield* guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
        Effect.gen(function* () {
          const claim = yield* sql`
            INSERT INTO task_claim_joined (task_id) VALUES (${canonicalTask.taskId})
          `.pipe(Effect.forkChild({ startImmediately: true }));
          yield* Fiber.join(claim);
        }),
      );

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_joined
        `)[0]?.count,
        1,
      );
    }),
  );

  it.effect("terminates attached descendant fibers before committing", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_descendant (task_id TEXT PRIMARY KEY)`;
      const release = yield* Deferred.make<void>();
      const descendantStarted = yield* Deferred.make<void>();
      const descendantFinalized = yield* Deferred.make<void>();
      const guard = yield* makeGuard();

      yield* guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
        Effect.gen(function* () {
          yield* Deferred.await(release).pipe(
            Effect.andThen(
              sql`INSERT INTO task_claim_descendant (task_id) VALUES (${canonicalTask.taskId})`,
            ),
            Effect.ensuring(Deferred.succeed(descendantFinalized, undefined).pipe(Effect.ignore)),
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Deferred.succeed(descendantStarted, undefined);
          return yield* Effect.never;
        }).pipe(Effect.forkChild({ startImmediately: true }), Effect.asVoid),
      );
      yield* Deferred.await(descendantStarted);
      assert.isTrue(yield* Deferred.isDone(descendantFinalized));
      yield* Deferred.succeed(release, undefined);
      yield* Effect.yieldNow;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_descendant
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("rolls back a joined child write when the callback fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_rollback (task_id TEXT PRIMARY KEY)`;
      const guard = yield* makeGuard();

      const result = yield* Effect.result(
        guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
          Effect.gen(function* () {
            const claim = yield* sql`
              INSERT INTO task_claim_rollback (task_id) VALUES (${canonicalTask.taskId})
            `.pipe(Effect.forkChild({ startImmediately: true }));
            yield* Fiber.join(claim);
            return yield* Effect.fail("callback-failed");
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_rollback
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("interrupts callback descendants before an interrupted transaction ends", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_interrupted (task_id TEXT PRIMARY KEY)`;
      const release = yield* Deferred.make<void>();
      const childStarted = yield* Deferred.make<void>();
      const childFinalized = yield* Deferred.make<void>();
      const guard = yield* makeGuard();

      const guarded = yield* guard
        .useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
          Effect.gen(function* () {
            yield* Deferred.await(release).pipe(
              Effect.andThen(
                sql`INSERT INTO task_claim_interrupted (task_id) VALUES (${canonicalTask.taskId})`,
              ),
              Effect.ensuring(Deferred.succeed(childFinalized, undefined).pipe(Effect.ignore)),
              Effect.forkChild({ startImmediately: true }),
            );
            yield* Deferred.succeed(childStarted, undefined);
            return yield* Effect.never;
          }),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(childStarted);
      yield* Fiber.interrupt(guarded);
      assert.isTrue(yield* Deferred.isDone(childFinalized));
      yield* Deferred.succeed(release, undefined);
      yield* Effect.yieldNow;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_interrupted
        `)[0]?.count,
        0,
      );
    }),
  );
});
