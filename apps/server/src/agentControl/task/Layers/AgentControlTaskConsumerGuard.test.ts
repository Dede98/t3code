import { AgentControlTaskId, type AgentControlTaskState, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

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

const makeGuard = (input?: {
  readonly available?: boolean;
  readonly mode?: "manual" | "observe" | "paused";
  readonly sourceSequence?: number | null;
  readonly watermarkStatus?: "reconciling" | "completed" | "recovery-required" | null;
  readonly targetSequence?: number;
  readonly lastCompletedSequence?: number;
  readonly tasks?: ReadonlyArray<AgentControlTaskState | "corrupt">;
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
                  expectedIssueCount: 1,
                },
                issues: [],
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
      get: () => Effect.die("unused"),
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

it.effect("accepts only a completed, exact current project and task sequence", () =>
  Effect.gen(function* () {
    const guard = yield* makeGuard();
    const current = yield* guard.ensureCurrent(projectId, task(5));
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
        expected: "watermark-not-completed",
        input: { sourceSequence: 6 },
      },
      {
        expected: "task-projection-corrupt",
        input: { tasks: ["corrupt" as const] },
      },
    ] as const;

    for (const testCase of cases) {
      const guard = yield* makeGuard(testCase.input);
      const result = yield* Effect.result(guard.ensureCurrent(projectId));
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.reason, testCase.expected);
    }

    const guard = yield* makeGuard();
    const taskMismatch = yield* Effect.result(guard.ensureCurrent(projectId, task(4)));
    assert.equal(taskMismatch._tag, "Failure");
    if (taskMismatch._tag === "Failure") {
      assert.equal(taskMismatch.failure.reason, "task-sequence-mismatch");
    }
  }),
);
