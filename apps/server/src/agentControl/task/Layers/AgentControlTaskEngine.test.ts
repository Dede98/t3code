import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { deriveAgentControlTaskId } from "../identity.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const event = {
  eventId: EventId.make("task-engine-publication-original"),
  type: "agentControl.task.created",
  aggregateKind: "task",
  aggregateId: AgentControlTaskId.make("task-engine-publication-task"),
  sequence: 1,
  streamVersion: 1,
  occurredAt: "2026-08-30T12:00:00.000Z",
  commandId: CommandId.make("task-engine-publication-command"),
  causationEventId: null,
  correlationId: CommandId.make("task-engine-publication-command"),
  authority: "controller",
  payload: {
    taskId: AgentControlTaskId.make("task-engine-publication-task"),
    source: {
      projectId: ProjectId.make("task-engine-publication-project"),
      repositoryNodeId: "task-engine-publication-repository",
      issueNodeId: "task-engine-publication-issue",
      issueNumber: 1,
      issueUrl: "https://example.invalid/task-engine-publication/1",
    },
    status: "candidate",
    sourceGate: "eligible",
    stage: "intake",
    sourceUpdatedAt: "2026-08-30T12:00:00.000Z",
    githubIntakeSequence: 1,
    sourceSnapshot: {
      repositoryNodeId: "task-engine-publication-repository",
      issueNodeId: "task-engine-publication-issue",
      number: 1,
      url: "https://example.invalid/task-engine-publication/1",
      state: "open",
      title: "Task Engine publication",
      body: null,
      contentTrust: "untrusted-external",
      updatedAt: "2026-08-30T12:00:00.000Z",
      timelineComplete: true,
      ready: true,
      paused: false,
      eligible: true,
      eligibilityReason: "eligible",
    },
    createdAt: "2026-08-30T12:00:00.000Z",
  },
  metadata: { schemaVersion: 1 },
} satisfies AgentControlTaskEvent;

layer("AgentControlTaskEngine committed publication", (it) => {
  it.effect("admits automatic task writes in Armed and rejects manual, run-once, and paused", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlTaskEngine;
      const github = yield* AgentControlGithubStateRepository;
      const projectId = ProjectId.make("task-engine-armed-mode-gate");
      const at = "2026-09-02T08:00:00.000Z";
      const repositoryNodeId = "task-engine-armed-repository";
      const issues = Array.from({ length: 4 }, (_, offset) => {
        const number = offset + 1;
        return {
          repositoryNodeId,
          issueNodeId: `task-engine-armed-issue-${number}`,
          number,
          url: `https://example.invalid/task-engine-armed/${number}`,
          state: "open" as const,
          title: `Armed task ${number}`,
          body: null,
          contentTrust: "untrusted-external" as const,
          updatedAt: at,
          timelineComplete: true,
          timelineEvents: [],
          ready: true,
          paused: false,
          eligible: true,
          eligibilityReason: "eligible" as const,
        };
      });
      yield* sql`
        INSERT INTO main.projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (${projectId}, 'Task Engine Armed', '/tmp/task-engine-armed', NULL, '[]', ${at}, ${at}, NULL)
      `;
      yield* sql`
        INSERT INTO main.agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES (${projectId}, 'armed', NULL, 1, 1, ${at})
      `;
      yield* github.save(
        {
          schemaVersion: 1,
          projectId,
          config: {
            schemaVersion: 1,
            projectId,
            settings: {
              trackerKind: "github",
              readyLabel: "agent:ready",
              pausedLabel: "agent:paused",
              trustedLogins: [],
              pollIntervalSeconds: 60,
            },
            repository: { repositoryNodeId, nameWithOwner: "owner/repo" },
            revision: 1,
            sequence: 1,
            updatedAt: at,
          },
          cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
          pollStatus: {
            status: "success",
            attemptedAt: at,
            completedAt: at,
            errorCode: null,
            issueCount: issues.length,
          },
          revision: 1,
          sequence: 1,
          updatedAt: at,
        },
        0,
      );
      yield* github.replaceIssues(projectId, issues);
      const dispatch = Effect.fn("test.dispatchObservedTask")(function* (
        index: number,
        commandSuffix: string,
      ) {
        const issue = issues[index]!;
        const taskId = yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId,
          issueNodeId: issue.issueNodeId,
        });
        return yield* Effect.result(
          engine.dispatchObservedController({
            type: "agentControl.task.createFromGithubIssue",
            commandId: CommandId.make(`task-engine-armed-${commandSuffix}`),
            taskId,
            projectId,
            expectedRevision: 0,
            sourcePrecondition: {
              schemaVersion: 1,
              projectId,
              githubIntakeSequence: 1,
              githubProjectionRevision: 1,
              githubConfigRevision: 1,
              repositoryNodeId,
              pollStatus: "success",
              expectedIssueCount: issues.length,
            },
            source: {
              projectId,
              repositoryNodeId,
              issueNodeId: issue.issueNodeId,
              issueNumber: issue.number,
              issueUrl: issue.url,
            },
            sourceGate: "eligible",
            sourceUpdatedAt: at,
            githubIntakeSequence: 1,
            sourceSnapshot: {
              repositoryNodeId: issue.repositoryNodeId,
              issueNodeId: issue.issueNodeId,
              number: issue.number,
              url: issue.url,
              state: issue.state,
              title: issue.title,
              body: issue.body,
              contentTrust: issue.contentTrust,
              updatedAt: issue.updatedAt,
              timelineComplete: issue.timelineComplete,
              ready: issue.ready,
              paused: issue.paused,
              eligible: issue.eligible,
              eligibilityReason: issue.eligibilityReason,
            },
          }),
        );
      });

      assert.equal((yield* dispatch(0, "armed"))._tag, "Success");
      for (const [index, mode, pausedFromMode] of [
        [1, "manual", null],
        [2, "run-once", null],
        [3, "paused", "armed"],
      ] as const) {
        yield* sql`
          UPDATE main.agent_control_project_states
          SET mode = ${mode}, paused_from_mode = ${pausedFromMode}, revision = revision + 1,
            last_event_sequence = last_event_sequence + 1
          WHERE project_id = ${projectId}
        `;
        const rejected = yield* dispatch(index, mode);
        assert.equal(rejected._tag, "Failure", mode);
        if (rejected._tag === "Failure") {
          assert.equal(rejected.failure.code, "project-mode-inactive", mode);
        }
      }
    }),
  );

  it.effect("publishes each EventId at most once per process runtime", () =>
    Effect.gen(function* () {
      const engine = yield* AgentControlTaskEngine;
      const observed = yield* Ref.make<ReadonlyArray<string>>([]);
      const sentinelSeen = yield* Deferred.make<void>();
      const subscribed = yield* engine.subscribeDomainEvents;
      const listener = yield* Effect.forkChild(
        Stream.runForEach(subscribed, (published) =>
          Ref.update(observed, (current) => [...current, published.eventId]).pipe(
            Effect.andThen(
              published.eventId === "task-engine-publication-sentinel"
                ? Deferred.succeed(sentinelSeen, undefined)
                : Effect.void,
            ),
            Effect.asVoid,
          ),
        ),
      );
      const sentinel = {
        ...event,
        eventId: EventId.make("task-engine-publication-sentinel"),
      };
      const concurrent = {
        ...event,
        eventId: EventId.make("task-engine-publication-concurrent"),
      };

      yield* engine.publishCommitted([event]);
      yield* Effect.all(
        [engine.publishCommitted([concurrent]), engine.publishCommitted([concurrent])],
        { concurrency: "unbounded", discard: true },
      );
      yield* engine.publishCommitted([event, event, sentinel]);
      yield* Deferred.await(sentinelSeen);
      yield* Fiber.interrupt(listener);

      assert.deepStrictEqual(yield* Ref.get(observed), [
        "task-engine-publication-original",
        "task-engine-publication-concurrent",
        "task-engine-publication-sentinel",
      ]);
    }),
  );
});
