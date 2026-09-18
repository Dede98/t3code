import {
  AgentControlEpicRpcError,
  AgentControlRunOnceId,
  AgentControlTaskId,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type AgentControlEpicFinalVerification,
  type AgentControlEpicHandoffPullRequest,
  AgentControlEpicRuntimeView,
  type AgentControlEpicReviewReworkInput,
  type AgentControlEpicSource,
  type AgentControlProjectState,
  type AgentControlPreflightRuntimeResult,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { GithubIssueTrackerClient } from "../github/Services/GithubIssueTrackerClient.ts";
import { AgentControlGithubStateRepository } from "../github/Services/AgentControlGithubStateRepository.ts";
import { createDefaultGithubIntakeState } from "../github/projector.ts";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { AgentControlPolicyService } from "../AgentControlPolicyService.ts";
import { selectAgentControlRunOnceCandidate } from "../runOnce/selection.ts";
import {
  bindEpicChildRun,
  epicDigest,
  epicJson,
  loadEpicRun,
  loadEpicRunBase,
  saveEpicRun,
  requireEpicIntegrationAuthority,
} from "./authority.ts";
import { createEpicRun, insertEpicRun } from "./runState.ts";
import { EpicHandoffRemote } from "./remote.ts";
import { epicSourceChanges, selectEpicMember } from "./model.ts";
import { makeAgentControlEpic } from "./Layers/AgentControlEpic.ts";
import {
  AgentControlEpicResultHooks,
  type AgentControlEpicReviewVerifyInput,
} from "./Services/AgentControlEpicResultHooks.ts";
import {
  AgentControlEpicReviewRepair,
  type AgentControlEpicReviewRepairInput,
  type AgentControlEpicReviewRepairProgress,
} from "./Services/AgentControlEpicReviewRepair.ts";

const projectId = ProjectId.make("epic-unit-project");
const at = "2026-09-14T08:00:00.000Z";
const repository = { repositoryNodeId: "epic-repository", nameWithOwner: "owner/repo" };
const decodeEpicState = Schema.decodeUnknownSync(
  Schema.fromJsonString(AgentControlEpicRuntimeView),
);
const issue = (number: number) => ({
  ...repository,
  issueNodeId: `issue-${number}`,
  number,
  title: `Task ${number}`,
  url: `https://github.com/owner/repo/issues/${number}`,
  state: "open" as const,
  subIssueCount: 0,
});
const source: AgentControlEpicSource = {
  format: "github-native-sub-issues-v1",
  repository,
  epic: { ...issue(10), subIssueCount: 3 },
  tasks: [
    { issue: issue(3), position: 0, dependencies: [issue(2)] },
    { issue: issue(2), position: 1, dependencies: [] },
    { issue: issue(4), position: 2, dependencies: [] },
  ],
  blockers: [],
  fingerprint: "native-preview-fingerprint",
  inspectedAt: at,
};
const checks = [
  {
    id: "required",
    command: "node",
    args: ["--test"],
    cwd: ".",
    required: true,
    timeoutMs: 1000,
    allowTemporaryFiles: false,
    resultFormat: "exit-code" as const,
  },
];
const initial = (): AgentControlEpicRuntimeView => ({
  epicRunId: "epic-test",
  projectId,
  revision: 1,
  status: "running",
  source,
  checks,
  members: source.tasks.map((task) => ({
    issueNodeId: task.issue.issueNodeId,
    issueNumber: task.issue.number,
    taskId: null,
    childRunId: null,
    status: "pending",
    baseCommitSha: null,
    reservationId: null,
    taskFinalizationEvidenceId: null,
    accepted: null,
  })),
  activeTaskId: null,
  acceptedCommitSha: null,
  blockers: [],
  blockerHistory: [],
  verificationAttempt: 1,
  finalVerification: null,
  finalVerificationHistory: [],
  createdAt: at,
  updatedAt: at,
});
const seedRun = Effect.fn("seedEpicRun")(function* (
  sql: SqlClient.SqlClient,
  state: AgentControlEpicRuntimeView,
) {
  yield* sql`INSERT INTO agent_control_epic_runs(epic_run_id,project_id,revision,state_json,state_digest) VALUES (${state.epicRunId},${projectId},${state.revision},${epicJson(state)},${epicDigest(state)})`;
  yield* sql`INSERT INTO agent_control_epic_history(epic_run_id,revision,state_json,state_digest) VALUES (${state.epicRunId},${state.revision},${epicJson(state)},${epicDigest(state)})`;
  yield* sql`INSERT INTO agent_control_epic_targets(project_id,epic_run_id) VALUES (${projectId},${state.epicRunId})`;
});
const insertTask = (
  sql: SqlClient.SqlClient,
  number: number,
  gate = "eligible",
) => sql`INSERT INTO agent_control_task_states(task_id,project_id,repository_node_id,issue_node_id,issue_number,issue_url,status,source_gate,stage,source_updated_at,github_intake_sequence,state_json,created_at,updated_at,revision,last_event_sequence)
  VALUES (${`task-${number}`},${projectId},${repository.repositoryNodeId},${`issue-${number}`},${number},${issue(number).url},'candidate',${gate},'intake',${at},7,'{}',${at},${at},1,7)`;

describe("Epic dependency policy", () => {
  it("orders explicit dependencies and gives an independent eligible task progress", () => {
    const state = initial();
    assert.equal(selectEpicMember(state)?.issue.number, 2);
    assert.equal(selectEpicMember(state, new Set(["issue-3", "issue-4"]))?.issue.number, 4);
    const accepted = {
      ...state,
      members: state.members.map((member) =>
        member.issueNumber === 2 ? { ...member, status: "accepted" as const } : member,
      ),
    };
    assert.equal(selectEpicMember(accepted)?.issue.number, 3);
  });
  it("does not reinterpret unrelated links or externally closed members as verified work", () => {
    const state = initial();
    const closed = {
      ...state,
      members: state.members.map((member) =>
        member.issueNumber === 2 ? { ...member, status: "external-closed" as const } : member,
      ),
    };
    assert.equal(selectEpicMember(closed)?.issue.number, 3);
    assert.equal(closed.members[1]!.accepted, null);
  });
  it("blocks structural changes and pending closures but preserves accepted closure evidence", () => {
    const state = initial();
    assert.equal(
      epicSourceChanges(state, { ...source, tasks: source.tasks.slice(1) })[0]?.code,
      "scope-changed",
    );
    const current = {
      ...source,
      tasks: source.tasks.map((task) =>
        task.issue.number === 2
          ? { ...task, issue: { ...task.issue, state: "closed" as const } }
          : task,
      ),
    };
    assert.equal(epicSourceChanges(state, current)[0]?.code, "closed-during-run");
    assert.deepEqual(
      epicSourceChanges(
        {
          ...state,
          members: state.members.map((member) =>
            member.issueNumber === 2 ? { ...member, status: "accepted" } : member,
          ),
        },
        current,
      ),
      [],
    );
  });
  it("ignores dependency response order while retaining membership order and edges", () => {
    const frozen = {
      ...source,
      tasks: source.tasks.map((task) =>
        task.issue.number === 3 ? { ...task, dependencies: [issue(2), issue(4)] } : task,
      ),
    };
    const state = { ...initial(), source: frozen };
    const reordered = {
      ...frozen,
      tasks: frozen.tasks.map((task) => ({
        ...task,
        dependencies: task.dependencies.toReversed(),
      })),
    };
    assert.deepEqual(epicSourceChanges(state, reordered), []);
    assert.equal(
      epicSourceChanges(state, { ...reordered, tasks: reordered.tasks.toReversed() })[0]?.code,
      "scope-changed",
    );
    assert.equal(
      epicSourceChanges(state, {
        ...reordered,
        tasks: reordered.tasks.map((task) =>
          task.issue.number === 3 ? { ...task, dependencies: [issue(2)] } : task,
        ),
      })[0]?.code,
      "scope-changed",
    );
  });
  it("keeps old sources compatible while fencing new Epic dependency edges and reopened prerequisites", () => {
    assert.deepEqual(epicSourceChanges(initial(), { ...source, dependencies: [] }), []);
    const frozen = {
      ...source,
      dependencies: [
        { ...issue(90), state: "closed" as const },
        { ...issue(91), state: "closed" as const },
      ],
    };
    const state = { ...initial(), source: frozen };
    assert.deepEqual(
      epicSourceChanges(state, { ...frozen, dependencies: frozen.dependencies.toReversed() }),
      [],
    );
    assert.equal(epicSourceChanges(initial(), frozen)[0]?.code, "scope-changed");
    assert.equal(
      epicSourceChanges(state, { ...frozen, dependencies: [] })[0]?.code,
      "scope-changed",
    );
    assert.deepEqual(
      epicSourceChanges(state, { ...frozen, dependencies: [issue(90), frozen.dependencies[1]!] }),
      [
        {
          code: "prerequisite-reopened",
          issueNumber: 90,
          message: "Epic prerequisite #90 was reopened.",
        },
      ],
    );
    assert.deepEqual(epicSourceChanges(state, frozen), []);
  });
});

describe("Epic persistence and existing selection", () => {
  it.effect(
    "pins a higher-number Epic task, binds one Run Once, survives replay, and fences terminal scope",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 83 });
        yield* insertTask(sql, 1);
        yield* insertTask(sql, 2);
        assert.equal(yield* selectAgentControlRunOnceCandidate(sql, projectId, 7), "task-1");
        const taskId = AgentControlTaskId.make("task-2");
        const state = {
          ...initial(),
          activeTaskId: taskId,
          acceptedCommitSha: "a".repeat(40),
          members: initial().members.map((member) =>
            member.issueNumber === 2
              ? { ...member, taskId, status: "running" as const, baseCommitSha: "a".repeat(40) }
              : member,
          ),
        };
        yield* seedRun(sql, state);
        assert.equal(yield* selectAgentControlRunOnceCandidate(sql, projectId, 7), "task-2");
        const runId = AgentControlRunOnceId.make("child-A");
        yield* sql.withTransaction(bindEpicChildRun(sql, projectId, taskId, runId));
        yield* sql.withTransaction(bindEpicChildRun(sql, projectId, taskId, runId));
        const recovered = yield* loadEpicRun(sql, state.epicRunId);
        assert.equal(recovered?.revision, 2);
        assert.equal(yield* loadEpicRunBase(sql, projectId, taskId, runId), "a".repeat(40));
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql.withTransaction(
                bindEpicChildRun(sql, projectId, taskId, AgentControlRunOnceId.make("duplicate")),
              ),
            ),
          ),
        );
        yield* sql.withTransaction(saveEpicRun(sql, recovered!, { status: "succeeded" }));
        assert.equal(yield* selectAgentControlRunOnceCandidate(sql, projectId, 7), null);
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql`UPDATE agent_control_epic_history SET state_json='{}'`),
          ),
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "binds the first queued child to its fetched SHA and rejects a changed target mapping",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 85 });
        const taskId = AgentControlTaskId.make("task-2");
        const runId = AgentControlRunOnceId.make("queued-child");
        const commitSha = "a".repeat(40);
        const state = {
          ...initial(),
          initialBase: { commitSha, targetBranch: "main" },
          activeTaskId: taskId,
          members: initial().members.map((member) =>
            member.issueNumber === 2
              ? { ...member, taskId, status: "running" as const, baseCommitSha: commitSha }
              : member,
          ),
        };
        yield* seedRun(sql, state);
        yield* sql.withTransaction(bindEpicChildRun(sql, projectId, taskId, runId));
        assert.equal(yield* loadEpicRunBase(sql, projectId, taskId, runId, "main"), commitSha);
        const error = yield* loadEpicRunBase(sql, projectId, taskId, runId, "other").pipe(
          Effect.flip,
        );
        assert.propertyVal(error, "code", "authority-conflict");
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("competing revisions cannot adopt the same result twice or rewrite scope", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 83 });
      const state = initial();
      yield* seedRun(sql, state);
      const results = yield* Effect.all(
        [
          sql.withTransaction(saveEpicRun(sql, state, { acceptedCommitSha: "a".repeat(40) })),
          sql.withTransaction(saveEpicRun(sql, state, { acceptedCommitSha: "b".repeat(40) })),
        ].map(Effect.exit),
        { concurrency: 2 },
      );
      assert.equal(results.filter(Exit.isSuccess).length, 1);
      assert.equal((yield* loadEpicRun(sql, state.epicRunId))?.revision, 2);
      const current = (yield* loadEpicRun(sql, state.epicRunId))!;
      assert.propertyVal(
        yield* requireEpicIntegrationAuthority(state, current).pipe(Effect.flip),
        "code",
        "revision-conflict",
      );
      yield* requireEpicIntegrationAuthority(current, current);
      const stopped = yield* sql.withTransaction(saveEpicRun(sql, current, { status: "stopped" }));
      assert.propertyVal(
        yield* requireEpicIntegrationAuthority(current, stopped).pipe(Effect.flip),
        "code",
        "revision-conflict",
      );
      assert.propertyVal(
        yield* requireEpicIntegrationAuthority(stopped, stopped).pipe(Effect.flip),
        "code",
        "authority-conflict",
      );
      assert.propertyVal(
        yield* requireEpicIntegrationAuthority(state, {
          ...current,
          dependencyPlanDigest: "different-plan",
        }).pipe(Effect.flip),
        "code",
        "authority-conflict",
      );
      assert.propertyVal(
        yield* requireEpicIntegrationAuthority(state, null).pipe(Effect.flip),
        "code",
        "authority-conflict",
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(saveEpicRun(sql, stopped, { source: { ...source, tasks: [] } })),
          ),
        ),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});

const fixture = Effect.fn("epicServiceFixture")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: 83 });
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,created_at,updated_at,scripts_json) VALUES (${projectId},'Epic test','/isolated/epic',${at},${at},'[]')`;
  yield* sql`INSERT INTO agent_control_project_policies(project_id,policy_json,revision,updated_at) VALUES (${projectId},${epicJson({ verificationChecks: checks })},1,${at})`;
  yield* sql`INSERT INTO agent_control_task_reconcile_states(project_id,target_sequence,last_completed_sequence,revision,status,updated_at) VALUES (${projectId},7,7,1,'completed',${at})`;
  for (const number of [2, 3, 4]) yield* insertTask(sql, number);
  let currentSource = source;
  const sources = new Map<number, AgentControlEpicSource>();
  let project: AgentControlProjectState = {
    schemaVersion: 1,
    projectId,
    mode: "observe",
    pausedFromMode: null,
    revision: 1,
    sequence: 1,
    updatedAt: at,
  };
  const dispatch: AgentControlEngine["Service"]["dispatchHuman"] = (input) =>
    Effect.sync(() => {
      project = {
        ...project,
        mode: input.mode,
        revision: project.revision + 1,
        sequence: project.sequence + 1,
      };
      return { state: project, resultSequence: project.sequence, eventCreated: true };
    });
  const engine = AgentControlEngine.of({
    getProjectState: () => Effect.sync(() => project),
    dispatchHuman: dispatch,
    dispatchController: dispatch,
    dispatchSystem: dispatch,
    streamDomainEvents: Stream.never,
  });
  const github = AgentControlGithubStateRepository.of({
    get: () =>
      Effect.succeed(
        Option.some({
          ...createDefaultGithubIntakeState(projectId),
          config: {
            schemaVersion: 1,
            projectId,
            repository,
            settings: {
              trackerKind: "github",
              readyLabel: "agent:ready",
              pausedLabel: "agent:paused",
              trustedLogins: [],
              pollIntervalSeconds: 60,
            },
            revision: 1,
            sequence: 7,
            updatedAt: at,
          },
        }),
      ),
    getCompletedSnapshot: () =>
      Effect.succeed(
        Option.some({
          sourcePrecondition: {
            schemaVersion: 1,
            projectId,
            githubIntakeSequence: 7,
            githubProjectionRevision: 1,
            githubConfigRevision: 1,
            repositoryNodeId: repository.repositoryNodeId,
            pollStatus: "success",
            expectedIssueCount: 3,
          },
          issues: [],
        }),
      ),
    save: () => Effect.void,
    replaceIssues: () => Effect.void,
    listIssues: () => Effect.succeed([]),
    matchesCompletedSnapshot: () => Effect.succeed(true),
    deleteProject: () => Effect.void,
    deleteAll: Effect.void,
  });
  const client = GithubIssueTrackerClient.of({
    resolveRepository: () => Effect.succeed(repository),
    pollIssues: () => Effect.succeed({ repository, issues: [] }),
    inspectEpic: (input) => Effect.sync(() => sources.get(input.epicNumber) ?? currentSource),
  });
  const attempts: number[] = [];
  const reviewRepairCalls: AgentControlEpicReviewRepairInput[] = [];
  const reviewVerificationCalls: AgentControlEpicReviewVerifyInput[] = [];
  let reviewProgress: (
    input: AgentControlEpicReviewRepairInput,
  ) => Effect.Effect<AgentControlEpicReviewRepairProgress, AgentControlEpicRpcError> = () =>
    Effect.fail(
      new AgentControlEpicRpcError({
        code: "unexpected-review-repair",
        message: "No review repair expected in this fixture.",
      }),
    );
  let reviewVerification: (
    input: AgentControlEpicReviewVerifyInput,
  ) => Effect.Effect<AgentControlEpicFinalVerification, AgentControlEpicRpcError> = () =>
    Effect.fail(
      new AgentControlEpicRpcError({
        code: "unexpected-review-verification",
        message: "No review verification expected in this fixture.",
      }),
    );
  let runtime: AgentControlPreflightRuntimeResult = {
    ok: true,
    staticPreflight: { ok: true, roles: [] },
    roles: [],
  };
  const make = () =>
    makeAgentControlEpic.pipe(
      Effect.provideService(AgentControlEngine, engine),
      Effect.provideService(AgentControlPolicyService, {
        getPolicy: () => Effect.die("unexpected getPolicy"),
        setProjectPolicy: () => Effect.die("unexpected setProjectPolicy"),
        clearProjectPolicy: () => Effect.die("unexpected clearProjectPolicy"),
        preflightPolicy: () => Effect.die("unexpected preflightPolicy"),
        preflightRuntime: () => Effect.sync(() => runtime),
      }),
      Effect.provideService(AgentControlGithubStateRepository, github),
      Effect.provideService(GithubIssueTrackerClient, client),
      Effect.provideService(AgentControlEpicReviewRepair, {
        progress: (input) => {
          reviewRepairCalls.push(input);
          return reviewProgress(input);
        },
        cancel: () => Effect.void,
      }),
      Effect.provideService(AgentControlEpicResultHooks, {
        capture: () =>
          Effect.fail(
            new AgentControlEpicRpcError({
              code: "unexpected-capture",
              message: "No capture expected in this fixture.",
            }),
          ),
        verify: (input) =>
          Effect.sync(() => {
            attempts.push(input.attempt);
            return {
              status: "failed" as const,
              commitSha: input.commitSha,
              evidenceId: `final-${input.attempt}`,
              detail: "Shared result check failed.",
              checks: [],
            };
          }),
        verifyReview: (input) => {
          reviewVerificationCalls.push(input);
          return reviewVerification(input);
        },
      }),
    );
  const makeWithHandoffObservation = (observation: AgentControlEpicHandoffPullRequest) =>
    make().pipe(
      Effect.provideService(EpicHandoffRemote, {
        readPullRequest: () => Effect.succeed(observation),
        prepare: () => Effect.die("Review-request preflight must not prepare a handoff"),
        publish: () => Effect.die("Review-request preflight must not publish a handoff"),
      }),
    );
  return {
    sql,
    make,
    attempts,
    reviewRepairCalls,
    reviewVerificationCalls,
    setReviewProgress: (
      next: (
        input: AgentControlEpicReviewRepairInput,
      ) => Effect.Effect<AgentControlEpicReviewRepairProgress, AgentControlEpicRpcError>,
    ) => {
      reviewProgress = next;
    },
    setReviewVerification: (
      next: (
        input: AgentControlEpicReviewVerifyInput,
      ) => Effect.Effect<AgentControlEpicFinalVerification, AgentControlEpicRpcError>,
    ) => {
      reviewVerification = next;
    },
    setRuntime: (next: AgentControlPreflightRuntimeResult) => {
      runtime = next;
    },
    setSource: (next: AgentControlEpicSource) => {
      currentSource = next;
    },
    setSources: (values: readonly AgentControlEpicSource[]) => {
      for (const value of values) sources.set(value.epic.number, value);
    },
    setMode: (mode: AgentControlProjectState["mode"]) => {
      project = { ...project, mode, revision: project.revision + 1 };
    },
    makeWithHandoffObservation,
  };
});

describe("Epic service lifecycle", () => {
  it.effect(
    "blocks Epic start before any child or turn when required verification is unsupported",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        f.setRuntime({
          ok: false,
          staticPreflight: { ok: true, roles: [] },
          roles: [
            {
              role: "verifier",
              accessMode: "restricted",
              strict: true,
              selectedCandidateIndex: null,
              errorCode: "role-runtime-unresolved",
              candidates: [
                {
                  candidateIndex: 0,
                  source: "role-route",
                  providerInstanceId: ProviderInstanceId.make("codex"),
                  model: "test",
                  driverKind: ProviderDriverKind.make("codex"),
                  providerStatus: "ready",
                  authStatus: "authenticated",
                  checkedAt: at,
                  runtimeReady: false,
                  errorCode: "verification-checks-unavailable",
                  verificationCheckError: {
                    checkId: "required",
                    message: "Loopback isolation is unavailable.",
                  },
                },
              ],
            },
          ],
        });
        const service = yield* f.make();
        const preview = yield* service.preview({ projectId, epicNumber: 10 });
        assert.isFalse(preview.canStart);
        assert.deepEqual(preview.blockers, [
          {
            code: "verification-checks-unavailable",
            issueNumber: null,
            message: "Check required: Loopback isolation is unavailable.",
          },
        ]);
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              service.start({
                projectId,
                commandId: CommandId.make("unsupported-epic"),
                expectedRevision: 1,
                epicNumber: 10,
                expectedFingerprint: source.fingerprint,
              }),
            ),
          ),
        );
        assert.deepEqual(yield* f.sql`SELECT epic_run_id FROM agent_control_epic_runs`, []);
        assert.deepEqual(f.attempts, []);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("shows a blocker when selected work loses approval before Run Once admits it", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const service = yield* f.make();
      yield* service.start({
        projectId,
        commandId: CommandId.make("approval-race"),
        expectedRevision: 1,
        epicNumber: 10,
        expectedFingerprint: source.fingerprint,
      });
      yield* service.processProject(projectId);
      yield* f.sql`UPDATE agent_control_task_states SET source_gate='paused' WHERE task_id='task-2'`;
      yield* service.processProject(projectId);
      const state = (yield* service.get(projectId))!;
      assert.equal(state.status, "blocked");
      assert.equal(state.blockers[0]?.code, "task-not-approved");
      assert.equal(yield* selectAgentControlRunOnceCandidate(f.sql, projectId, 7), null);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "persists a resolved external prerequisite on resume and blocks a later reopening",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        f.setMode("armed");
        const frozen = {
          ...source,
          tasks: source.tasks.map((task) =>
            task.issue.number === 2 ? { ...task, dependencies: [issue(99)] } : task,
          ),
        };
        const state = { ...initial(), status: "blocked" as const, source: frozen };
        yield* seedRun(f.sql, state);
        f.setSource({
          ...frozen,
          tasks: frozen.tasks.map((task) => ({
            ...task,
            dependencies: task.dependencies.map((dependency) =>
              dependency.number === 99 ? { ...dependency, state: "closed" as const } : dependency,
            ),
          })),
        });
        const service = yield* f.make();
        yield* service.resume({
          projectId,
          epicRunId: state.epicRunId,
          expectedRevision: 1,
          commandId: CommandId.make("resolved-prerequisite"),
        });
        yield* service.processProject(projectId);
        const current = (yield* service.get(projectId))!;
        assert.equal(current.activeTaskId, "task-2");
        assert.equal(current.externalPrerequisites?.[0]?.issueNumber, 99);
        assert.equal(current.source.tasks[1]?.dependencies[0]?.state, "open");
        f.setSource(frozen);
        yield* service.processProject(projectId);
        assert.equal((yield* service.get(projectId))?.blockers[0]?.code, "prerequisite-reopened");
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "recovers an uncommitted activation but never overrides a later human off command",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const state = initial();
        yield* seedRun(f.sql, state);
        yield* f.sql`INSERT INTO agent_control_epic_mode_intents(command_id,epic_run_id,expected_revision,mode) VALUES ('crashed-activation',${state.epicRunId},1,'armed')`;
        const service = yield* f.make();
        yield* service.processProject(projectId);
        assert.equal((yield* service.get(projectId))?.activeTaskId, "task-2");
        assert.equal(
          (yield* f.sql<{ status: string }>`SELECT status FROM agent_control_epic_mode_intents`)[0]
            ?.status,
          "applied",
        );
        f.setMode("observe");
        yield* f.sql`INSERT INTO agent_control_epic_mode_intents(command_id,epic_run_id,expected_revision,mode) VALUES ('superseded-activation',${state.epicRunId},1,'armed')`;
        const recovered = yield* f.make();
        yield* recovered.processProject(projectId);
        assert.equal(
          (yield* f.sql<{
            status: string;
          }>`SELECT status FROM agent_control_epic_mode_intents WHERE command_id='superseded-activation'`)[0]
            ?.status,
          "superseded",
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("two service instances accept one start command without duplicate runs", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const first = yield* f.make();
      const second = yield* f.make();
      const input = {
        projectId,
        commandId: CommandId.make("same-command"),
        expectedRevision: 1,
        epicNumber: 10,
        expectedFingerprint: source.fingerprint,
      };
      const started = yield* Effect.all([first.start(input), second.start(input)], {
        concurrency: 2,
      });
      assert.equal(started[0].epicRunId, started[1].epicRunId);
      assert.equal(
        (yield* f.sql<{ count: number }>`SELECT COUNT(*) AS count FROM agent_control_epic_runs`)[0]
          ?.count,
        1,
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("runs an independent approved child while another dependency is blocked", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.sql`UPDATE agent_control_task_states SET source_gate='paused' WHERE task_id='task-2'`;
      const service = yield* f.make();
      const preview = yield* service.preview({ projectId, epicNumber: 10 });
      assert.isTrue(preview.canStart);
      assert.equal(preview.blockers[0]?.code, "task-not-approved");
      yield* service.start({
        projectId,
        commandId: CommandId.make("independent"),
        expectedRevision: 1,
        epicNumber: 10,
        expectedFingerprint: source.fingerprint,
      });
      yield* service.processProject(projectId);
      assert.equal((yield* service.get(projectId))?.activeTaskId, "task-4");
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("rejects incomplete intake and keeps missing members visible", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const service = yield* f.make();
      yield* f.sql`UPDATE agent_control_task_reconcile_states SET target_sequence=8,status='reconciling' WHERE project_id=${projectId}`;
      const incomplete = yield* service.preview({ projectId, epicNumber: 10 });
      assert.isFalse(incomplete.canStart);
      assert.equal(incomplete.blockers[0]?.code, "intake-incomplete");
      yield* f.sql`UPDATE agent_control_task_reconcile_states SET target_sequence=7,status='completed' WHERE project_id=${projectId}`;
      yield* f.sql`DELETE FROM agent_control_task_states WHERE task_id='task-2'`;
      const missing = yield* service.preview({ projectId, epicNumber: 10 });
      assert.isTrue(missing.canStart);
      assert.equal(missing.blockers[0]?.code, "missing-issue");
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "pauses queued work without terminalizing it and re-arms the same run after restart",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* runMigrations({ toMigrationInclusive: 85 });
        const service = yield* f.make();
        const state = yield* service.start({
          projectId,
          commandId: CommandId.make("queued-pause-start"),
          expectedRevision: 1,
          epicNumber: 10,
          expectedFingerprint: source.fingerprint,
        });
        const nextSource = {
          ...source,
          epic: { ...source.epic, issueNodeId: "next-epic", number: 11 },
          tasks: [{ issue: issue(5), position: 0, dependencies: [] }],
          fingerprint: "next-preview",
        };
        f.setSource(nextSource);
        yield* service.changeQueue({
          projectId,
          commandId: CommandId.make("queued-pause-approve"),
          expectedRevision: 0,
          action: { kind: "approve", epicNumber: 11, expectedFingerprint: nextSource.fingerprint },
        });
        f.setSource(source);
        const request = {
          projectId,
          epicRunId: state.epicRunId,
          expectedRevision: state.revision,
          commandId: CommandId.make("queued-pause"),
        };
        const paused = yield* service.stop(request);
        assert.equal(paused.status, "running");
        assert.equal((yield* service.stop(request)).revision, paused.revision);
        yield* service.processProject(projectId);
        assert.isNull((yield* service.get(projectId))?.activeTaskId);
        f.setMode("armed");
        const restarted = yield* f.make();
        yield* restarted.processProject(projectId);
        const continued = yield* restarted.get(projectId);
        assert.equal(continued?.epicRunId, state.epicRunId);
        assert.equal(continued?.activeTaskId, "task-2");
        assert.lengthOf(yield* f.sql`SELECT epic_run_id FROM agent_control_epic_runs`, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("clears only stopped scope with automation off and retains all prior history", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const service = yield* f.make();
      let state = yield* service.start({
        projectId,
        commandId: CommandId.make("start-clear"),
        expectedRevision: 1,
        epicNumber: 10,
        expectedFingerprint: source.fingerprint,
      });
      state = yield* service.stop({
        projectId,
        epicRunId: state.epicRunId,
        expectedRevision: state.revision,
        commandId: CommandId.make("stop-clear"),
      });
      f.setMode("armed");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            service.clear({
              projectId,
              epicRunId: state.epicRunId,
              expectedRevision: state.revision,
              commandId: CommandId.make("cannot-clear-armed"),
            }),
          ),
        ),
      );
      assert.equal((yield* service.get(projectId))?.epicRunId, state.epicRunId);
      f.setMode("observe");
      yield* service.clear({
        projectId,
        epicRunId: state.epicRunId,
        expectedRevision: state.revision,
        commandId: CommandId.make("clear"),
      });
      assert.equal(yield* service.get(projectId), null);
      assert.equal((yield* loadEpicRun(f.sql, state.epicRunId))?.status, "stopped");
      assert.equal(yield* selectAgentControlRunOnceCandidate(f.sql, projectId, 7), "task-2");
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "starts once across retries and resumes the same run after reconstructing the service",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const service = yield* f.make();
        const input = {
          projectId,
          commandId: CommandId.make("start-epic"),
          expectedRevision: 1,
          epicNumber: 10,
          expectedFingerprint: source.fingerprint,
        };
        const started = yield* service.start(input);
        assert.equal((yield* service.start(input)).epicRunId, started.epicRunId);
        yield* service.processProject(projectId);
        assert.equal((yield* service.get(projectId))?.activeTaskId, "task-2");
        const reconstructed = yield* f.make();
        yield* reconstructed.processProject(projectId);
        assert.equal((yield* reconstructed.get(projectId))?.epicRunId, started.epicRunId);
        assert.equal(
          (yield* f.sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM agent_control_epic_runs`)[0]?.count,
          1,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("keeps automation off and blocks changed membership after explicit resume", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const service = yield* f.make();
      yield* service.start({
        projectId,
        commandId: CommandId.make("start-epic"),
        expectedRevision: 1,
        epicNumber: 10,
        expectedFingerprint: source.fingerprint,
      });
      f.setMode("observe");
      yield* service.processProject(projectId);
      let state = (yield* service.get(projectId))!;
      assert.equal(state.activeTaskId, null);
      f.setSource({ ...source, tasks: source.tasks.slice(1) });
      yield* service.resume({
        projectId,
        epicRunId: state.epicRunId,
        expectedRevision: state.revision,
        commandId: CommandId.make("resume-epic"),
      });
      yield* service.processProject(projectId);
      state = (yield* service.get(projectId))!;
      assert.equal(state.status, "blocked");
      assert.equal(state.blockers[0]?.code, "scope-changed");
      assert.equal(state.source.tasks.length, 3);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "does not declare success for green children with failed shared checks and preserves retry evidence",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        f.setMode("armed");
        const state = {
          ...initial(),
          acceptedCommitSha: "a".repeat(40),
          members: initial().members.map((member) => ({
            ...member,
            status: "accepted" as const,
            taskId: AgentControlTaskId.make(`task-${member.issueNumber}`),
            childRunId: `run-${member.issueNumber}`,
            reservationId: `reservation-${member.issueNumber}`,
            accepted: {
              commitSha: "a".repeat(40),
              treeSha: "b".repeat(40),
              evidenceId: `accepted-${member.issueNumber}`,
              codeDigest: "digest",
            },
          })),
        };
        yield* seedRun(f.sql, state);
        const service = yield* f.make();
        yield* service.processProject(projectId);
        let current = (yield* service.get(projectId))!;
        assert.equal(current.status, "blocked");
        assert.equal(current.finalVerification?.status, "failed");
        const recovered = yield* f.make();
        yield* recovered.processProject(projectId);
        assert.deepEqual(f.attempts, [1]);
        yield* recovered.resume({
          projectId,
          epicRunId: current.epicRunId,
          expectedRevision: current.revision,
          commandId: CommandId.make("retry-final"),
        });
        yield* recovered.processProject(projectId);
        current = (yield* recovered.get(projectId))!;
        assert.equal(current.status, "blocked");
        assert.deepEqual(f.attempts, [1, 2]);
        assert.equal(current.finalVerificationHistory.length, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});

const reviewedCommitSha = "a".repeat(40);
const repairedCommitSha = "b".repeat(40);
const reviewBaseCommitSha = "c".repeat(40);
const secondRepairedCommitSha = "e".repeat(40);
const reviewedVerification = {
  status: "passed" as const,
  commitSha: reviewedCommitSha,
  evidenceId: "reviewed-final-proof",
  detail: "The original combined result passed.",
  checks: [],
};
const reviewFinding = {
  findingId: "sidebar-keyboard-navigation",
  summary: "Keyboard navigation skips nested sidebar items.",
  correctionCriteria: "Nested items participate in the same ordered keyboard traversal.",
  acceptanceCriteria: "The focused behavior test reaches every visible nested item in order.",
};
const reviewAttempt = (status: "running" | "succeeded" = "running") => ({
  attempt: 1,
  providerInstanceId: "repair-provider",
  model: "repair-model",
  threadId: "review-repair-thread",
  status,
  startedAt: at,
  completedAt: status === "succeeded" ? at : null,
  error: null,
});
const reviewedPullRequest: AgentControlEpicHandoffPullRequest = {
  number: 42,
  url: "https://github.com/owner/repo/pull/42",
  state: "open",
  isDraft: true,
  headSha: reviewedCommitSha,
  baseBranch: "main",
  mergeCommitSha: null,
};
const succeededReviewState = (): AgentControlEpicRuntimeView => ({
  ...initial(),
  status: "succeeded",
  initialBase: { commitSha: reviewBaseCommitSha, targetBranch: "main" },
  acceptedCommitSha: reviewedCommitSha,
  members: initial().members.map((member) => ({
    ...member,
    taskId: AgentControlTaskId.make(`task-${member.issueNumber}`),
    childRunId: `reviewed-run-${member.issueNumber}`,
    status: "accepted" as const,
    baseCommitSha: reviewBaseCommitSha,
    reservationId: `reviewed-reservation-${member.issueNumber}`,
    taskFinalizationEvidenceId: `reviewed-finalization-${member.issueNumber}`,
    accepted: {
      commitSha: reviewedCommitSha,
      treeSha: "d".repeat(40),
      codeDigest: `reviewed-code-${member.issueNumber}`,
      evidenceId: `reviewed-result-${member.issueNumber}`,
    },
  })),
  finalVerification: reviewedVerification,
  finalVerificationHistory: [reviewedVerification],
  handoff: {
    intentId: "reviewed-handoff",
    status: "published",
    repository,
    targetBranch: "main",
    baseCommitSha: reviewBaseCommitSha,
    commitSha: reviewedCommitSha,
    branchName: "t3auto/epic-10-reviewed",
    verificationEvidenceId: reviewedVerification.evidenceId,
    requestedAt: at,
    updatedAt: at,
    pullRequest: null,
    error: null,
  },
});
const reviewRequest = (
  state: AgentControlEpicRuntimeView,
  overrides: Partial<AgentControlEpicReviewReworkInput> = {},
): AgentControlEpicReviewReworkInput => ({
  projectId,
  epicRunId: state.epicRunId,
  commandId: CommandId.make("review-rework-command"),
  expectedRevision: state.revision,
  reviewedCommitSha: reviewedCommitSha,
  reviewedVerificationEvidenceId: reviewedVerification.evidenceId,
  findings: [reviewFinding],
  idempotencyKey: "independent-review-1",
  ...overrides,
});

describe("Epic review rework", () => {
  for (const status of ["failed", "blocked", "publishing"] as const)
    it.effect(`rejects feedback for an unconfirmed ${status} handoff without a saved PR`, () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* runMigrations({ toMigrationInclusive: 94 });
        const original = succeededReviewState();
        const unconfirmed = {
          ...original,
          handoff: {
            ...original.handoff!,
            status,
            pullRequest: null,
            error:
              status === "publishing"
                ? null
                : { code: `handoff-${status}`, message: "The handoff was not confirmed." },
          },
        };
        f.setMode("armed");
        yield* seedRun(f.sql, unconfirmed);
        const service = yield* f.make();
        assert.propertyVal(
          yield* service.requestReviewRework(reviewRequest(unconfirmed)).pipe(Effect.flip),
          "code",
          status === "publishing" ? "review-publication-unsettled" : "review-handoff-unconfirmed",
        );
        assert.deepEqual(
          yield* f.sql`SELECT count(*) AS count FROM agent_control_epic_review_requests`,
          [{ count: 0 }],
        );
        assert.lengthOf(f.reviewRepairCalls, 0);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  for (const status of ["publishing", "update-required"] as const)
    it.effect(`rejects feedback while the retained handoff is ${status}`, () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* runMigrations({ toMigrationInclusive: 94 });
        const original = succeededReviewState();
        const unsettled = {
          ...original,
          handoff: { ...original.handoff!, status, pullRequest: reviewedPullRequest },
        };
        f.setMode("armed");
        yield* seedRun(f.sql, unsettled);
        const service = yield* f.makeWithHandoffObservation(reviewedPullRequest);
        assert.propertyVal(
          yield* service.requestReviewRework(reviewRequest(unsettled)).pipe(Effect.flip),
          "code",
          "review-publication-unsettled",
        );
        assert.deepEqual(
          yield* f.sql`SELECT count(*) AS count FROM agent_control_epic_review_requests`,
          [{ count: 0 }],
        );
        assert.lengthOf(f.reviewRepairCalls, 0);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  for (const condition of ["closed", "merged", "not-draft", "foreign-head"] as const)
    it.effect(
      `rejects feedback before persistence when the retained pull request is ${condition}`,
      () =>
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* runMigrations({ toMigrationInclusive: 94 });
          const original = succeededReviewState();
          const withPullRequest = {
            ...original,
            handoff: { ...original.handoff!, pullRequest: reviewedPullRequest },
          };
          const observation: AgentControlEpicHandoffPullRequest = {
            ...reviewedPullRequest,
            ...(condition === "closed" ? { state: "closed" as const } : {}),
            ...(condition === "merged"
              ? { state: "merged" as const, isDraft: false, mergeCommitSha: "f".repeat(40) }
              : {}),
            ...(condition === "not-draft" ? { isDraft: false } : {}),
            ...(condition === "foreign-head" ? { headSha: "f".repeat(40) } : {}),
          };
          f.setMode("armed");
          yield* seedRun(f.sql, withPullRequest);
          const service = yield* f.makeWithHandoffObservation(observation);
          assert.propertyVal(
            yield* service.requestReviewRework(reviewRequest(withPullRequest)).pipe(Effect.flip),
            "code",
            {
              closed: "review-pr-closed",
              merged: "review-pr-merged",
              "not-draft": "review-pr-not-draft",
              "foreign-head": "review-pr-head-changed",
            }[condition],
          );
          assert.deepEqual(
            yield* f.sql`SELECT count(*) AS count FROM agent_control_epic_review_requests`,
            [{ count: 0 }],
          );
          assert.lengthOf(f.reviewRepairCalls, 0);
        }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  it.effect(
    "moves durable feedback through repair and fresh verification to a new handoff while retaining history",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* runMigrations({ toMigrationInclusive: 94 });
        const original = succeededReviewState();
        f.setMode("armed");
        yield* seedRun(f.sql, original);
        let progress = 0;
        f.setReviewProgress((input) =>
          Effect.gen(function* () {
            yield* input.authorize;
            progress++;
            return progress === 1
              ? { kind: "repairing" as const, attempts: [reviewAttempt()] }
              : {
                  kind: "candidate" as const,
                  attempts: [reviewAttempt("succeeded")],
                  commitSha: repairedCommitSha,
                };
          }),
        );
        f.setReviewVerification((input) =>
          input.authorize.pipe(
            Effect.as({
              status: "passed" as const,
              commitSha: input.commitSha,
              evidenceId: "review-rework-final-proof",
              detail: "The repaired combined result passed.",
              checks: [],
            }),
          ),
        );
        const service = yield* f.make();
        const accepted = yield* service.requestReviewRework(reviewRequest(original));
        assert.equal(accepted.reviewReworks?.[0]?.status, "accepted");
        assert.equal(accepted.acceptedCommitSha, reviewedCommitSha);

        yield* service.processProject(projectId);
        assert.equal((yield* service.get(projectId))?.reviewReworks?.[0]?.status, "repairing");
        yield* service.processProject(projectId);
        assert.equal((yield* service.get(projectId))?.reviewReworks?.[0]?.status, "verifying");
        yield* service.processProject(projectId);

        const completed = (yield* service.get(projectId))!;
        assert.equal(completed.status, "succeeded");
        assert.equal(completed.acceptedCommitSha, repairedCommitSha);
        assert.equal(completed.finalVerification?.commitSha, repairedCommitSha);
        assert.equal(completed.finalVerification?.evidenceId, "review-rework-final-proof");
        assert.deepEqual(
          completed.finalVerificationHistory.map((proof) => proof.evidenceId),
          [reviewedVerification.evidenceId, "review-rework-final-proof"],
        );
        assert.equal(completed.reviewReworks?.[0]?.status, "succeeded");
        assert.equal(completed.reviewReworks?.[0]?.previousAcceptedCommitSha, reviewedCommitSha);
        assert.equal(completed.handoff?.status, "update-required");
        assert.equal(completed.handoff?.commitSha, repairedCommitSha);
        assert.equal(completed.handoff?.verificationEvidenceId, "review-rework-final-proof");
        assert.equal(completed.handoffHistory?.[0]?.handoff.commitSha, reviewedCommitSha);
        assert.equal(
          completed.handoffHistory?.[0]?.supersededByReviewRequestId,
          completed.reviewReworks?.[0]?.requestId,
        );
        assert.lengthOf(f.reviewRepairCalls, 2);
        assert.lengthOf(f.reviewVerificationCalls, 1);
        assert.deepEqual(
          yield* f.sql`SELECT reviewed_commit_sha,reviewed_verification_evidence_id
            FROM agent_control_epic_review_requests`,
          [
            {
              reviewed_commit_sha: reviewedCommitSha,
              reviewed_verification_evidence_id: reviewedVerification.evidenceId,
            },
          ],
        );
        const oldest = yield* f.sql<{ stateJson: string }>`SELECT state_json AS "stateJson"
          FROM agent_control_epic_history WHERE epic_run_id=${original.epicRunId} AND revision=1`;
        assert.deepEqual(decodeEpicState(oldest[0]!.stateJson), original);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "replays command and idempotency identities without duplicate repair or verification",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* runMigrations({ toMigrationInclusive: 94 });
        const original = succeededReviewState();
        f.setMode("armed");
        yield* seedRun(f.sql, original);
        f.setReviewProgress((input) =>
          input.authorize.pipe(
            Effect.as({
              kind: "candidate" as const,
              attempts: [reviewAttempt("succeeded")],
              commitSha: repairedCommitSha,
            }),
          ),
        );
        f.setReviewVerification((input) =>
          input.authorize.pipe(
            Effect.as({
              status: "passed" as const,
              commitSha: input.commitSha,
              evidenceId: "idempotent-review-proof",
              detail: "Passed once.",
              checks: [],
            }),
          ),
        );
        const service = yield* f.make();
        const input = reviewRequest(original);
        const accepted = yield* service.requestReviewRework(input);
        assert.equal((yield* service.requestReviewRework(input)).revision, accepted.revision);
        assert.equal(
          (yield* service.requestReviewRework({
            ...input,
            commandId: CommandId.make("review-rework-retry-command"),
          })).revision,
          accepted.revision,
        );
        assert.propertyVal(
          yield* service
            .requestReviewRework({
              ...input,
              findings: [{ ...reviewFinding, acceptanceCriteria: "Different requested scope." }],
            })
            .pipe(Effect.flip),
          "code",
          "command-conflict",
        );
        assert.propertyVal(
          yield* service
            .requestReviewRework({
              ...input,
              commandId: CommandId.make("review-rework-conflicting-key-command"),
              findings: [{ ...reviewFinding, acceptanceCriteria: "Different requested scope." }],
            })
            .pipe(Effect.flip),
          "code",
          "idempotency-conflict",
        );
        yield* service.processProject(projectId);
        yield* service.processProject(projectId);
        yield* service.processProject(projectId);
        assert.lengthOf(f.reviewRepairCalls, 1);
        assert.lengthOf(f.reviewVerificationCalls, 1);
        assert.deepEqual(
          yield* f.sql`SELECT count(*) AS count FROM agent_control_epic_review_requests`,
          [{ count: 1 }],
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "verifies a second reviewed repair from the retained original member evidence after the intermediate handoff is published",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* runMigrations({ toMigrationInclusive: 94 });
        const original = succeededReviewState();
        f.setMode("armed");
        yield* seedRun(f.sql, original);
        f.setReviewProgress((input) =>
          input.authorize.pipe(
            Effect.as({
              kind: "candidate" as const,
              attempts: [reviewAttempt("succeeded")],
              commitSha:
                input.rework.reviewedCommitSha === reviewedCommitSha
                  ? repairedCommitSha
                  : secondRepairedCommitSha,
            }),
          ),
        );
        f.setReviewVerification((input) =>
          input.authorize.pipe(
            Effect.as({
              status: "passed" as const,
              commitSha: input.commitSha,
              evidenceId:
                input.commitSha === repairedCommitSha
                  ? "first-review-repair-proof"
                  : "second-review-repair-proof",
              detail: "The sequential reviewed repair passed.",
              checks: [],
            }),
          ),
        );
        const service = yield* f.make();
        yield* service.requestReviewRework(reviewRequest(original));
        yield* service.processProject(projectId);
        yield* service.processProject(projectId);
        const first = (yield* service.get(projectId))!;
        assert.equal(first.acceptedCommitSha, repairedCommitSha);
        assert.equal(first.handoff?.status, "update-required");

        const published = yield* f.sql.withTransaction(
          saveEpicRun(f.sql, first, {
            handoff: { ...first.handoff!, status: "published" },
          }),
        );
        yield* service.requestReviewRework(
          reviewRequest(published, {
            commandId: CommandId.make("second-review-rework-command"),
            expectedRevision: published.revision,
            reviewedCommitSha: repairedCommitSha,
            reviewedVerificationEvidenceId: "first-review-repair-proof",
            idempotencyKey: "independent-review-2",
            findings: [
              {
                ...reviewFinding,
                findingId: "sidebar-focus-return",
                summary: "Focus is not restored after the nested item closes.",
              },
            ],
          }),
        );
        yield* service.processProject(projectId);
        yield* service.processProject(projectId);

        const completed = (yield* service.get(projectId))!;
        assert.equal(completed.acceptedCommitSha, secondRepairedCommitSha);
        assert.equal(completed.finalVerification?.evidenceId, "second-review-repair-proof");
        assert.deepEqual(
          completed.finalVerificationHistory.map((verification) => verification.evidenceId),
          [
            reviewedVerification.evidenceId,
            "first-review-repair-proof",
            "second-review-repair-proof",
          ],
        );
        assert.deepEqual(
          completed.reviewReworks?.map((rework) => [
            rework.previousAcceptedCommitSha,
            rework.candidateCommitSha,
            rework.status,
          ]),
          [
            [reviewedCommitSha, repairedCommitSha, "succeeded"],
            [repairedCommitSha, secondRepairedCommitSha, "succeeded"],
          ],
        );
        assert.lengthOf(completed.handoffHistory ?? [], 2);
        assert.equal(completed.handoffHistory?.[0]?.handoff.commitSha, reviewedCommitSha);
        assert.equal(completed.handoffHistory?.[1]?.handoff.commitSha, repairedCommitSha);
        assert.lengthOf(f.reviewVerificationCalls, 2);
        assert.equal(f.reviewVerificationCalls[1]?.previousCommitSha, repairedCommitSha);
        assert.equal(
          f.reviewVerificationCalls[1]?.firstAccepted.accepted?.commitSha,
          reviewedCommitSha,
        );
        assert.equal(
          f.reviewVerificationCalls[1]?.lastAccepted.accepted?.commitSha,
          reviewedCommitSha,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("rejects stale reviewed commits and evidence plus a competing active request", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 94 });
      const original = succeededReviewState();
      f.setMode("armed");
      yield* seedRun(f.sql, original);
      const service = yield* f.make();
      assert.propertyVal(
        yield* service
          .requestReviewRework(reviewRequest(original, { reviewedCommitSha: "e".repeat(40) }))
          .pipe(Effect.flip),
        "code",
        "review-revision-stale",
      );
      assert.propertyVal(
        yield* service
          .requestReviewRework(
            reviewRequest(original, { reviewedVerificationEvidenceId: "older-proof" }),
          )
          .pipe(Effect.flip),
        "code",
        "review-revision-stale",
      );
      const accepted = yield* service.requestReviewRework(reviewRequest(original));
      assert.propertyVal(
        yield* service
          .requestReviewRework(
            reviewRequest(original, {
              commandId: CommandId.make("competing-review-command"),
              expectedRevision: accepted.revision,
              idempotencyKey: "independent-review-2",
            }),
          )
          .pipe(Effect.flip),
        "code",
        "review-rework-unavailable",
      );
      assert.lengthOf(f.reviewRepairCalls, 0);
      assert.deepEqual(
        yield* f.sql`SELECT count(*) AS count FROM agent_control_epic_review_requests`,
        [{ count: 1 }],
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("accepts only one of two concurrent review requests across service instances", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 94 });
      const original = succeededReviewState();
      f.setMode("armed");
      yield* seedRun(f.sql, original);
      const first = yield* f.make();
      const second = yield* f.make();
      const outcomes = yield* Effect.all(
        [
          first.requestReviewRework(
            reviewRequest(original, {
              commandId: CommandId.make("concurrent-review-a"),
              idempotencyKey: "concurrent-review-key-a",
            }),
          ),
          second.requestReviewRework(
            reviewRequest(original, {
              commandId: CommandId.make("concurrent-review-b"),
              idempotencyKey: "concurrent-review-key-b",
            }),
          ),
        ].map(Effect.exit),
        { concurrency: 2 },
      );
      assert.equal(outcomes.filter(Exit.isSuccess).length, 1);
      assert.equal(outcomes.filter(Exit.isFailure).length, 1);
      assert.deepEqual(
        yield* f.sql`SELECT count(*) AS count FROM agent_control_epic_review_requests`,
        [{ count: 1 }],
      );
      const current = (yield* first.get(projectId))!;
      assert.equal(current.reviewReworks?.length, 1);
      assert.equal(current.reviewReworks?.[0]?.status, "accepted");
      assert.lengthOf(f.reviewRepairCalls, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("keeps the reviewed authority and handoff current when fresh verification fails", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 94 });
      const original = succeededReviewState();
      f.setMode("armed");
      yield* seedRun(f.sql, original);
      f.setReviewProgress((input) =>
        input.authorize.pipe(
          Effect.as({
            kind: "candidate" as const,
            attempts: [reviewAttempt("succeeded")],
            commitSha: repairedCommitSha,
          }),
        ),
      );
      f.setReviewVerification((input) =>
        input.authorize.pipe(
          Effect.as({
            status: "failed" as const,
            commitSha: input.commitSha,
            evidenceId: "failed-review-proof",
            detail: "The repaired combined result failed its required check.",
            checks: [],
          }),
        ),
      );
      const service = yield* f.make();
      yield* service.requestReviewRework(reviewRequest(original));
      yield* service.processProject(projectId);
      yield* service.processProject(projectId);
      const blocked = (yield* service.get(projectId))!;
      assert.equal(blocked.status, "blocked");
      assert.equal(blocked.activeReviewReworkId, null);
      assert.equal(blocked.acceptedCommitSha, reviewedCommitSha);
      assert.deepEqual(blocked.finalVerification, reviewedVerification);
      assert.equal(blocked.finalVerificationHistory.at(-1)?.evidenceId, "failed-review-proof");
      assert.equal(blocked.handoff?.commitSha, reviewedCommitSha);
      assert.equal(blocked.handoff?.verificationEvidenceId, reviewedVerification.evidenceId);
      assert.equal(blocked.reviewReworks?.[0]?.candidateCommitSha, repairedCommitSha);
      assert.equal(blocked.reviewReworks?.[0]?.status, "blocked");
      assert.equal(blocked.blockers[0]?.code, "review-verification-failed");
      assert.propertyVal(
        yield* service
          .resume({
            projectId,
            epicRunId: blocked.epicRunId,
            expectedRevision: blocked.revision,
            commandId: CommandId.make("resume-terminal-review-rework"),
          })
          .pipe(Effect.flip),
        "code",
        "review-rework-terminal",
      );
      assert.deepEqual(yield* service.get(projectId), blocked);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("fences a late repair result after the active review request is stopped", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 94 });
      const original = succeededReviewState();
      f.setMode("armed");
      yield* seedRun(f.sql, original);
      const progressStarted = yield* Deferred.make<void>();
      const releaseProgress = yield* Deferred.make<void>();
      f.setReviewProgress((input) =>
        Effect.gen(function* () {
          yield* input.authorize;
          yield* Deferred.succeed(progressStarted, undefined);
          yield* Deferred.await(releaseProgress);
          yield* input.authorize;
          return {
            kind: "candidate" as const,
            attempts: [reviewAttempt("succeeded")],
            commitSha: repairedCommitSha,
          };
        }),
      );
      const service = yield* f.make();
      const accepted = yield* service.requestReviewRework(reviewRequest(original));
      const processing = yield* service.processProject(projectId).pipe(Effect.forkChild);
      yield* Deferred.await(progressStarted);
      // A reconstructed service has an independent in-memory lock but must still
      // fence the late worker through the durable Epic revision.
      const stopper = yield* f.make();
      const stopped = yield* stopper.stop({
        projectId,
        epicRunId: original.epicRunId,
        expectedRevision: accepted.revision,
        commandId: CommandId.make("stop-review-rework"),
      });
      assert.equal(stopped.status, "stopped");
      yield* Deferred.succeed(releaseProgress, undefined);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(Fiber.join(processing))));
      const current = (yield* service.get(projectId))!;
      assert.equal(current.status, "stopped");
      assert.equal(current.activeReviewReworkId, null);
      assert.equal(current.reviewReworks?.[0]?.status, "stopped");
      assert.equal(current.reviewReworks?.[0]?.candidateCommitSha, null);
      assert.equal(current.acceptedCommitSha, reviewedCommitSha);
      assert.deepEqual(current.finalVerification, reviewedVerification);
      assert.lengthOf(f.reviewVerificationCalls, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("blocks a late repair result after Armed authority is revoked", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 94 });
      const original = succeededReviewState();
      f.setMode("armed");
      yield* seedRun(f.sql, original);
      const progressStarted = yield* Deferred.make<void>();
      const releaseProgress = yield* Deferred.make<void>();
      f.setReviewProgress((input) =>
        Effect.gen(function* () {
          yield* input.authorize;
          yield* Deferred.succeed(progressStarted, undefined);
          yield* Deferred.await(releaseProgress);
          yield* input.authorize;
          return {
            kind: "candidate" as const,
            attempts: [reviewAttempt("succeeded")],
            commitSha: repairedCommitSha,
          };
        }),
      );
      const service = yield* f.make();
      yield* service.requestReviewRework(reviewRequest(original));
      const processing = yield* service.processProject(projectId).pipe(Effect.forkChild);
      yield* Deferred.await(progressStarted);
      f.setMode("observe");
      yield* Deferred.succeed(releaseProgress, undefined);
      yield* Fiber.join(processing);

      const blocked = (yield* service.get(projectId))!;
      assert.equal(blocked.status, "blocked");
      assert.equal(blocked.activeReviewReworkId, null);
      assert.equal(blocked.reviewReworks?.[0]?.status, "blocked");
      assert.equal(blocked.reviewReworks?.[0]?.candidateCommitSha, null);
      assert.equal(blocked.reviewReworks?.[0]?.blocker?.code, "review-authority-revoked");
      assert.equal(blocked.acceptedCommitSha, reviewedCommitSha);
      assert.deepEqual(blocked.finalVerification, reviewedVerification);
      assert.equal(blocked.handoff?.commitSha, reviewedCommitSha);
      assert.lengthOf(f.reviewVerificationCalls, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("recovers accepted, repairing, and verifying review work across service restarts", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 94 });
      const original = succeededReviewState();
      f.setMode("armed");
      yield* seedRun(f.sql, original);
      let candidateReady = false;
      f.setReviewProgress((input) =>
        input.authorize.pipe(
          Effect.as(
            candidateReady
              ? {
                  kind: "candidate" as const,
                  attempts: [reviewAttempt("succeeded")],
                  commitSha: repairedCommitSha,
                }
              : { kind: "repairing" as const, attempts: [reviewAttempt()] },
          ),
        ),
      );
      f.setReviewVerification((input) =>
        input.authorize.pipe(
          Effect.as({
            status: "passed" as const,
            commitSha: input.commitSha,
            evidenceId: "recovered-review-proof",
            detail: "Recovered verification passed.",
            checks: [],
          }),
        ),
      );
      const accepting = yield* f.make();
      yield* accepting.requestReviewRework(reviewRequest(original));

      const repairStarter = yield* f.make();
      yield* repairStarter.processProject(projectId);
      const repairing = (yield* repairStarter.get(projectId))!;
      assert.equal(repairing.reviewReworks?.[0]?.status, "repairing");
      const repairingRevision = repairing.revision;

      const repairRecovery = yield* f.make();
      yield* repairRecovery.processProject(projectId);
      assert.equal((yield* repairRecovery.get(projectId))?.revision, repairingRevision);
      candidateReady = true;
      const candidateRecovery = yield* f.make();
      yield* candidateRecovery.processProject(projectId);
      assert.equal(
        (yield* candidateRecovery.get(projectId))?.reviewReworks?.[0]?.status,
        "verifying",
      );

      const verificationRecovery = yield* f.make();
      yield* verificationRecovery.processProject(projectId);
      assert.equal((yield* verificationRecovery.get(projectId))?.status, "succeeded");
      yield* verificationRecovery.processProject(projectId);
      assert.lengthOf(f.reviewRepairCalls, 3);
      assert.lengthOf(f.reviewVerificationCalls, 1);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});

const reviewedPlan = {
  version: 1 as const,
  sourceFingerprint: source.fingerprint,
  rationale: "Independent documents for 2 and 4; task 3 consumes both.",
  tasks: [
    { issueNodeId: "issue-2", dependsOn: [] },
    { issueNodeId: "issue-4", dependsOn: [] },
    { issueNodeId: "issue-3", dependsOn: ["issue-2", "issue-4"] },
  ],
};
const plannedState = (parallelism: number): AgentControlEpicRuntimeView => ({
  ...initial(),
  parallelism,
  dependencyPlan: reviewedPlan,
  dependencyPlanDigest: epicDigest(reviewedPlan),
  initialBase: { commitSha: "initial-head", targetBranch: "main" },
});

describe("Epic parallel selection and recovery", () => {
  for (const limit of [1, 2])
    it.effect(`reserves deterministic independent members up to limit ${limit}`, () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        f.setMode("armed");
        yield* seedRun(f.sql, plannedState(limit));
        const service = yield* f.make();
        yield* service.processProject(projectId);
        const state = (yield* service.get(projectId))!;
        assert.deepEqual(
          state.members
            .filter((member) => member.status === "running")
            .map((member) => member.issueNumber),
          limit === 1 ? [2] : [2, 4],
        );
        assert.equal(
          state.members.find((member) => member.issueNumber === 3)?.waitReason,
          "dependencies",
        );
        const restarted = yield* f.make();
        yield* restarted.processProject(projectId);
        assert.deepEqual((yield* restarted.get(projectId))?.members, state.members);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  it.effect("keeps independent B running after A fails and blocks C", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.setMode("armed");
      const state = plannedState(2);
      yield* seedRun(f.sql, {
        ...state,
        members: state.members.map((member) =>
          member.issueNumber === 2 ? { ...member, status: "failed" } : member,
        ),
      });
      const service = yield* f.make();
      yield* service.processProject(projectId);
      const current = (yield* service.get(projectId))!;
      assert.equal(current.status, "running");
      assert.equal(current.members.find((member) => member.issueNumber === 4)?.status, "running");
      assert.equal(current.members.find((member) => member.issueNumber === 3)?.status, "pending");
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("pause prevents new reservations and plan edits cannot rewrite issued authority", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* seedRun(f.sql, plannedState(2));
      const service = yield* f.make();
      yield* service.processProject(projectId);
      const state = (yield* service.get(projectId))!;
      assert.isTrue(state.members.every((member) => member.status === "pending"));
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            saveEpicRun(f.sql, state, {
              dependencyPlan: { ...reviewedPlan, rationale: "replacement" },
            }),
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(saveEpicRun(f.sql, state, { parallelism: 4 }))),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});

describe("Epic integrated dependency progress", () => {
  const accepted = (id: string) => ({
    commitSha: `commit-${id}`,
    treeSha: `tree-${id}`,
    codeDigest: `code-${id}`,
    evidenceId: `accepted-${id}`,
  });
  const proof = (commitSha: string) => ({
    status: "passed" as const,
    commitSha,
    evidenceId: `verified-${commitSha}`,
    detail: "Current integration checks passed",
    checks: [],
  });

  it.effect(
    "resumes only captured integration failures with fresh attempt authority and clean task status",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        f.setMode("armed");
        const planned = plannedState(2);
        const state: AgentControlEpicRuntimeView = {
          ...planned,
          status: "blocked",
          blockers: [
            { code: "integration-check-failed", issueNumber: 2, message: "combined check failed" },
          ],
          members: planned.members.map((member) =>
            member.issueNumber === 2
              ? {
                  ...member,
                  taskId: AgentControlTaskId.make("task-2"),
                  childRunId: "execution-a",
                  status: "failed",
                  captured: accepted("a"),
                  reservationId: "reservation-a",
                  taskFinalizationEvidenceId: "task-proof-a",
                  waitReason: "blocker",
                  blocker: "combined check failed",
                }
              : member.issueNumber === 4
                ? {
                    ...member,
                    status: "failed",
                    waitReason: "blocker",
                    blocker: "implementation failed",
                  }
                : member,
          ),
        };
        yield* seedRun(f.sql, state);
        const service = yield* f.make();
        const resumed = yield* service.resume({
          projectId,
          epicRunId: state.epicRunId,
          expectedRevision: state.revision,
          commandId: CommandId.make("resume-captured"),
        });
        const a = resumed.members.find((member) => member.issueNumber === 2)!;
        const b = resumed.members.find((member) => member.issueNumber === 4)!;
        assert.equal(resumed.verificationAttempt, 2);
        assert.deepEqual(resumed.blockers, []);
        assert.equal(a.status, "running");
        assert.equal(a.waitReason, "integration");
        assert.isUndefined(a.blocker);
        assert.equal(a.childRunId, "execution-a");
        assert.equal(a.reservationId, "reservation-a");
        assert.deepEqual(a.captured, accepted("a"));
        assert.equal(b.status, "failed");
        assert.equal(b.blocker, "implementation failed");
        const reopened = yield* f.make();
        assert.deepEqual((yield* reopened.get(projectId))?.members, resumed.members);
        assert.deepEqual(
          (yield* reopened.resume({
            projectId,
            epicRunId: state.epicRunId,
            expectedRevision: state.revision,
            commandId: CommandId.make("resume-captured"),
          })).members,
          resumed.members,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  for (const currentProof of [false, true])
    it.effect(
      `releases C only when both captured predecessors have accepted integration and ${currentProof ? "current" : "stale"} head proof`,
      () =>
        Effect.gen(function* () {
          const f = yield* fixture();
          f.setMode("armed");
          const planned = plannedState(2);
          const state: AgentControlEpicRuntimeView = {
            ...planned,
            acceptedCommitSha: "current-integration",
            integrationVerification: proof(
              currentProof ? "current-integration" : "old-integration",
            ),
            members: planned.members.map((member) =>
              member.issueNumber === 3
                ? member
                : {
                    ...member,
                    status: "accepted",
                    taskId: AgentControlTaskId.make(`task-${member.issueNumber}`),
                    childRunId: `execution-${member.issueNumber}`,
                    accepted: accepted(String(member.issueNumber)),
                    captured: accepted(`local-${member.issueNumber}`),
                  },
            ),
          };
          yield* seedRun(f.sql, state);
          const service = yield* f.make();
          yield* service.processProject(projectId);
          const after = (yield* service.get(projectId))!;
          const c = after.members.find((member) => member.issueNumber === 3)!;
          assert.equal(c.status, currentProof ? "running" : "pending");
          assert.equal(c.waitReason, currentProof ? "capacity" : "dependencies");
          assert.equal(c.baseCommitSha, currentProof ? "current-integration" : null);
          assert.isNull(c.childRunId);
          assert.isTrue(
            after.members
              .filter((member) => member.issueNumber !== 3)
              .every((member) => member.status === "accepted"),
          );
          const recovered = yield* f.make();
          yield* recovered.processProject(projectId);
          assert.deepEqual((yield* recovered.get(projectId))?.members, after.members);
        }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  it.effect(
    "does not release a successor for captured local results or closed issue metadata",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        f.setMode("armed");
        f.setSource({
          ...source,
          tasks: source.tasks.map((task) =>
            task.issue.number === 4 ? { ...task, issue: { ...task.issue, state: "closed" } } : task,
          ),
        });
        const planned = plannedState(2);
        yield* seedRun(f.sql, {
          ...planned,
          acceptedCommitSha: "current-integration",
          integrationVerification: proof("current-integration"),
          members: planned.members.map((member) =>
            member.issueNumber === 3
              ? member
              : {
                  ...member,
                  status: member.issueNumber === 2 ? "accepted" : "failed",
                  accepted: member.issueNumber === 2 ? accepted("a") : null,
                  captured: accepted(String(member.issueNumber)),
                },
          ),
        });
        const service = yield* f.make();
        yield* service.processProject(projectId);
        const after = (yield* service.get(projectId))!;
        assert.equal(after.members.find((member) => member.issueNumber === 3)?.status, "pending");
        assert.equal(
          after.members.find((member) => member.issueNumber === 3)?.waitReason,
          "dependencies",
        );
        assert.equal(after.status, "blocked");
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});

for (const reason of ["closed", "dependency", "approval"] as const)
  it.effect(`blocks an active child before accepting its result after ${reason} changes`, () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.setMode("armed");
      const planned = plannedState(2);
      yield* seedRun(f.sql, {
        ...planned,
        members: planned.members.map((member) =>
          member.issueNumber === 2
            ? {
                ...member,
                status: "running",
                taskId: AgentControlTaskId.make("task-2"),
                childRunId: "active-child",
                reservationId: "retained-worktree",
              }
            : member,
        ),
      });
      if (reason === "approval")
        yield* f.sql`UPDATE agent_control_task_states SET source_gate='not-ready' WHERE task_id='task-2'`;
      else
        f.setSource({
          ...source,
          tasks: source.tasks.map((task) =>
            task.issue.number !== 2
              ? task
              : reason === "closed"
                ? {
                    ...task,
                    issue: { ...task.issue, state: "closed" },
                  }
                : { ...task, dependencies: [issue(90)] },
          ),
        });
      const service = yield* f.make();
      yield* service.processProject(projectId);
      const blocked = (yield* service.get(projectId))!;
      assert.equal(blocked.status, "blocked");
      assert.equal(
        blocked.blockers[0]?.code,
        reason === "approval"
          ? "task-not-approved"
          : reason === "closed"
            ? "closed-during-run"
            : "scope-changed",
      );
      assert.equal(
        blocked.members.find((member) => member.issueNumber === 2)?.reservationId,
        "retained-worktree",
      );
      const restarted = yield* f.make();
      yield* restarted.processProject(projectId);
      assert.deepEqual(yield* restarted.get(projectId), blocked);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

it.effect("waits for in-progress intake reconciliation without blocking an active Epic", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.setMode("armed");
    const planned = plannedState(2);
    const state: AgentControlEpicRuntimeView = {
      ...planned,
      members: planned.members.map((member) =>
        member.issueNumber === 2
          ? {
              ...member,
              status: "running",
              taskId: AgentControlTaskId.make("task-2"),
              childRunId: "active-child",
            }
          : member,
      ),
    };
    yield* seedRun(f.sql, state);
    yield* f.sql`UPDATE agent_control_task_reconcile_states SET target_sequence=30 WHERE project_id=${projectId}`;
    const service = yield* f.make();
    yield* service.processProject(projectId);
    assert.deepEqual(yield* service.get(projectId), state);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "keeps independent work running across dependency review waits and restarts before starting on the proved merged base",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 93 });
      f.setMode("armed");
      const firstSource: AgentControlEpicSource = {
        ...source,
        tasks: [{ issue: issue(2), position: 0, dependencies: [] }],
        fingerprint: "first-source",
      };
      const secondSource: AgentControlEpicSource = {
        ...source,
        epic: issue(11),
        fingerprint: "second-source",
        tasks: [
          { issue: issue(3), position: 0, dependencies: [issue(2)] },
          { issue: issue(4), position: 1, dependencies: [] },
        ],
      };
      f.setSources([firstSource, secondSource]);
      const projectDependencyPlan = {
        version: 1 as const,
        rationale: "Task 3 consumes reviewed task 2; task 4 is independent.",
        epics: [firstSource, secondSource].map((value) => ({
          issueNodeId: value.epic.issueNodeId,
          sourceFingerprint: value.fingerprint,
        })),
        tasks: [
          { issueNodeId: "issue-2", dependsOn: [] },
          { issueNodeId: "issue-3", dependsOn: ["issue-2"] },
          { issueNodeId: "issue-4", dependsOn: [] },
        ],
      };
      const create = (value: AgentControlEpicSource) =>
        createEpicRun({
          projectId,
          commandId: value.fingerprint,
          source: value,
          checks: [],
          parallelism: 2,
          initialBase: { commitSha: "a".repeat(40), targetBranch: "main" },
          projectDependencyPlan,
          dependencyPlan: {
            version: 1,
            sourceFingerprint: value.fingerprint,
            rationale: "Reviewed task graph.",
            tasks: projectDependencyPlan.tasks.filter((node) =>
              value.tasks.some((task) => task.issue.issueNodeId === node.issueNodeId),
            ),
          },
        });
      const first = yield* create(firstSource);
      const second = yield* create(secondSource);
      yield* insertEpicRun(f.sql, first);
      yield* insertEpicRun(f.sql, second);
      const result = {
        commitSha: "b".repeat(40),
        treeSha: "c".repeat(40),
        codeDigest: "digest",
        evidenceId: "captured",
      };
      const reviewed = yield* f.sql.withTransaction(
        saveEpicRun(f.sql, first, {
          status: "succeeded",
          acceptedCommitSha: result.commitSha,
          members: first.members.map((member) => ({
            ...member,
            status: "accepted",
            accepted: result,
          })),
          finalVerification: {
            status: "passed",
            commitSha: result.commitSha,
            evidenceId: "verification",
            detail: "Passed",
            checks: [],
          },
          handoff: {
            intentId: "handoff",
            status: "published",
            repository,
            targetBranch: "main",
            baseCommitSha: "a".repeat(40),
            commitSha: result.commitSha,
            branchName: "review",
            verificationEvidenceId: "verification",
            requestedAt: at,
            updatedAt: at,
            error: null,
            pullRequest: {
              number: 30,
              url: "https://github.com/owner/repo/pull/30",
              state: "open",
              isDraft: true,
              headSha: result.commitSha,
              baseBranch: "main",
              mergeCommitSha: null,
            },
          },
        }),
      );
      let refreshes = 0;
      const make = () =>
        f.make().pipe(
          Effect.provideService(EpicHandoffRemote, {
            refreshQueueBase: (input) =>
              Effect.sync(() => {
                assert.equal(input.previousHandoff?.pullRequest?.state, "merged");
                refreshes++;
                return { commitSha: "d".repeat(40), targetBranch: "main" };
              }),
            readPullRequest: () => Effect.die("unused"),
            prepare: () => Effect.die("unused"),
            publish: () => Effect.die("unused"),
          }),
        );
      yield* (yield* make()).processProject(projectId);
      const waiting = (yield* loadEpicRun(f.sql, second.epicRunId))!;
      assert.equal(waiting.members[0]?.status, "pending");
      assert.equal(waiting.members[0]?.waitReason, "dependencies");
      assert.include(waiting.members[0]?.blocker, "reviewed merge");
      assert.equal(waiting.members[1]?.status, "running");
      assert.equal(refreshes, 0);
      yield* (yield* make()).processProject(projectId);
      assert.deepEqual(yield* loadEpicRun(f.sql, second.epicRunId), waiting);
      yield* f.sql.withTransaction(
        saveEpicRun(f.sql, reviewed, {
          handoff: {
            ...reviewed.handoff!,
            pullRequest: {
              ...reviewed.handoff!.pullRequest!,
              state: "merged",
              mergeCommitSha: "e".repeat(40),
            },
          },
        }),
      );
      yield* (yield* make()).processProject(projectId);
      const resumed = (yield* loadEpicRun(f.sql, second.epicRunId))!;
      assert.equal(resumed.epicRunId, waiting.epicRunId);
      assert.equal(resumed.members[0]?.status, "running");
      assert.equal(resumed.members[0]?.baseCommitSha, "d".repeat(40));
      assert.equal(resumed.members[1]?.baseCommitSha, "a".repeat(40));
      assert.equal(resumed.members[0]?.blocker, undefined);
      yield* (yield* make()).processProject(projectId);
      assert.deepEqual(yield* loadEpicRun(f.sql, second.epicRunId), resumed);
      assert.equal(refreshes, 1);
      const completed = (yield* loadEpicRun(f.sql, first.epicRunId))!;
      const stopMerged = yield* Effect.result(
        (yield* make()).stop({
          projectId,
          epicRunId: first.epicRunId,
          expectedRevision: completed.revision,
          commandId: CommandId.make("keep-merged-proof"),
        }),
      );
      assert.equal(stopMerged._tag, "Failure");
      if (stopMerged._tag === "Failure") assert.equal(stopMerged.failure.code, "epic-terminal");
      assert.deepEqual(yield* loadEpicRun(f.sql, first.epicRunId), completed);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "retains a run-specific authority failure while independently approved Epics still select work",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* runMigrations({ toMigrationInclusive: 93 });
      f.setMode("armed");
      const sources = [2, 4].map((number) => ({
        ...source,
        epic: issue(number + 10),
        fingerprint: `scope-${number}`,
        tasks: [{ issue: issue(number), position: 0, dependencies: [] }],
      }));
      f.setSources(sources);
      const projectDependencyPlan = {
        version: 1 as const,
        rationale: "Independent reviewed scopes",
        epics: sources.map((value) => ({
          issueNodeId: value.epic.issueNodeId,
          sourceFingerprint: value.fingerprint,
        })),
        tasks: [2, 4].map((number) => ({ issueNodeId: `issue-${number}`, dependsOn: [] })),
      };
      const runs: AgentControlEpicRuntimeView[] = [];
      for (const [index, value] of sources.entries()) {
        const run = yield* createEpicRun({
          projectId,
          commandId: value.fingerprint,
          source: value,
          checks: [],
          projectDependencyPlan,
          initialBase: { commitSha: "a".repeat(40), targetBranch: "main" },
          dependencyPlan: {
            version: 1,
            rationale: "Reviewed scope",
            sourceFingerprint: value.fingerprint,
            tasks: [{ issueNodeId: value.tasks[0]!.issue.issueNodeId, dependsOn: [] }],
          },
        });
        const persisted =
          index === 0 ? { ...run, dependencyPlanDigest: "divergent-authority" } : run;
        yield* insertEpicRun(f.sql, persisted);
        runs.push(persisted);
      }
      const service = yield* f.make();
      yield* service.processProject(projectId);
      const failed = (yield* loadEpicRun(f.sql, runs[0]!.epicRunId))!;
      const independent = (yield* loadEpicRun(f.sql, runs[1]!.epicRunId))!;
      assert.equal(failed.status, "blocked");
      assert.equal(failed.blockers[0]?.code, "authority-conflict");
      assert.equal(independent.members[0]?.status, "running");
      assert.equal(independent.members[0]?.taskId, "task-4");
      yield* (yield* f.make()).processProject(projectId);
      assert.deepEqual(yield* loadEpicRun(f.sql, runs[1]!.epicRunId), independent);
      const stopped = yield* service.stop({
        projectId,
        epicRunId: failed.epicRunId,
        expectedRevision: failed.revision,
        commandId: CommandId.make("stop-failed-owner"),
      });
      assert.equal(stopped.status, "stopped");
      assert.deepEqual(yield* loadEpicRun(f.sql, runs[1]!.epicRunId), independent);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
