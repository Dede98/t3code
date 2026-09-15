import {
  AgentControlEpicRpcError,
  AgentControlRunOnceId,
  AgentControlTaskId,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicSource,
  type AgentControlProjectState,
  type AgentControlPreflightRuntimeResult,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
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
} from "./authority.ts";
import { epicSourceChanges, selectEpicMember } from "./model.ts";
import { makeAgentControlEpic } from "./Layers/AgentControlEpic.ts";
import { AgentControlEpicResultHooks } from "./Services/AgentControlEpicResultHooks.ts";

const projectId = ProjectId.make("epic-unit-project");
const at = "2026-09-14T08:00:00.000Z";
const repository = { repositoryNodeId: "epic-repository", nameWithOwner: "owner/repo" };
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
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(saveEpicRun(sql, current, { source: { ...source, tasks: [] } })),
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
    inspectEpic: () => Effect.sync(() => currentSource),
  });
  const attempts: number[] = [];
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
      }),
    );
  return {
    sql,
    make,
    attempts,
    setRuntime: (next: AgentControlPreflightRuntimeResult) => {
      runtime = next;
    },
    setSource: (next: AgentControlEpicSource) => {
      currentSource = next;
    },
    setMode: (mode: AgentControlProjectState["mode"]) => {
      project = { ...project, mode, revision: project.revision + 1 };
    },
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
