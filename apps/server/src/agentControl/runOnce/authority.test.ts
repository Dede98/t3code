import {
  CommandId,
  EventId,
  ProjectId,
  type AgentControlRunOnceActivation,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson, type JsonValue } from "../initialPlanning/eventEvidence.ts";
import { admitRunOnceActivation, writeRunOnceStep } from "./authority.ts";
import { deriveAgentControlRunOnceId, deriveRunOnceCommandId } from "./identity.ts";
import { fingerprintAgentControlRunOnceSource } from "./source.ts";

const at = "2026-08-31T10:00:00.000Z";
const later = "2026-08-31T10:00:01.000Z";
const projectId = ProjectId.make("run-once-authority");

const insertEvent = Effect.fn("insertRunOnceAuthorityEvent")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly eventId: string;
    readonly aggregateKind: string;
    readonly streamVersion: number;
    readonly eventType: string;
    readonly commandId: string;
    readonly authority: string;
    readonly payload: JsonValue;
  },
) {
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO main.agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_authority, payload_json, metadata_json
    ) VALUES (
      ${input.eventId}, ${input.aggregateKind}, ${projectId}, ${input.streamVersion},
      ${input.eventType}, ${at}, ${input.commandId}, NULL, ${input.commandId},
      ${input.authority}, ${canonicalJson(input.payload)}, '{"schemaVersion":1}'
    ) RETURNING sequence
  `;
  return rows[0]!.sequence;
});

const seedActivation = Effect.fn("seedRunOnceActivation")(function* (sql: SqlClient.SqlClient) {
  yield* insertEvent(sql, {
    eventId: "project-observe-event",
    aggregateKind: "project-controller",
    streamVersion: 1,
    eventType: "agentControl.project.mode.changed",
    commandId: "project-observe-command",
    authority: "human",
    payload: {
      projectId,
      previousMode: "manual",
      mode: "observe",
      previousPausedFromMode: null,
      pausedFromMode: null,
      changedAt: at,
    },
  });
  yield* insertEvent(sql, {
    eventId: "github-config-event",
    aggregateKind: "github-intake",
    streamVersion: 1,
    eventType: "agentControl.github.config.set",
    commandId: "github-config-command",
    authority: "human",
    payload: {
      projectId,
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      repository: { repositoryNodeId: "repository-node", nameWithOwner: "owner/repo" },
      configuredAt: at,
    },
  });
  const githubSequence = yield* insertEvent(sql, {
    eventId: "github-success-event",
    aggregateKind: "github-intake",
    streamVersion: 2,
    eventType: "agentControl.github.poll.succeeded",
    commandId: "github-success-command",
    authority: "controller",
    payload: {
      projectId,
      repository: { repositoryNodeId: "repository-node", nameWithOwner: "owner/repo" },
      attemptedAt: at,
      completedAt: at,
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
      issues: [],
    },
  });
  const activationCommandId = CommandId.make("project-run-once-command");
  const activationEventId = EventId.make("project-run-once-event");
  const activationSequence = yield* insertEvent(sql, {
    eventId: activationEventId,
    aggregateKind: "project-controller",
    streamVersion: 2,
    eventType: "agentControl.project.mode.changed",
    commandId: activationCommandId,
    authority: "human",
    payload: {
      projectId,
      previousMode: "observe",
      mode: "run-once",
      previousPausedFromMode: null,
      pausedFromMode: null,
      changedAt: at,
    },
  });
  yield* sql`
    INSERT INTO main.agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, 'run-once', NULL, 2, ${activationSequence}, ${at})
  `;
  yield* sql`
    INSERT INTO main.agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence, revision, status, updated_at
    ) VALUES (${projectId}, ${githubSequence}, ${githubSequence}, 1, 'completed', ${at})
  `;
  const runId = deriveAgentControlRunOnceId({
    projectId,
    activationEventId,
    activationEventSequence: activationSequence,
    activationEventStreamVersion: 2,
    activationCommandId,
  });
  return {
    schemaVersion: 1,
    runId,
    projectId,
    activationEventId,
    activationEventSequence: activationSequence,
    activationEventStreamVersion: 2,
    activationCommandId,
    githubIntakeSequence: githubSequence,
    githubEventId: EventId.make("github-success-event"),
    githubEventSequence: githubSequence,
    githubEventStreamVersion: 2,
    reconcileRevision: 1,
    sourceFingerprint: fingerprintAgentControlRunOnceSource({
      schemaVersion: 1,
      projectId,
      githubIntakeSequence: githubSequence,
      githubProjectionRevision: 2,
      githubConfigRevision: 2,
      repositoryNodeId: "repository-node",
      pollStatus: "success",
      expectedIssueCount: 0,
    }),
    activatedAt: at,
  } satisfies AgentControlRunOnceActivation;
});

const layer = it.layer(NodeSqliteClient.layerMemory());

layer("run-once durable authority", (it) => {
  it.effect("replays identical E/R/M bytes with zero DML and rejects divergence", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 63 });
      const activation = yield* seedActivation(sql);
      assert.deepStrictEqual(
        yield* sql`
        SELECT
          t3_run_once_activation_identity_match(
            ${activation.runId}, ${activation.projectId}, ${activation.activationEventId},
            ${activation.activationEventSequence}, ${activation.activationEventStreamVersion},
            ${activation.activationCommandId}
          ) AS identity,
          t3_run_once_source_fingerprint_match(
            ${activation.sourceFingerprint}, ${activation.projectId},
            ${activation.githubIntakeSequence}, ${activation.githubEventStreamVersion},
            ${activation.githubEventStreamVersion}, 'repository-node', 0
          ) AS source,
          (SELECT json_extract(payload_json, '$.completedAt') = occurred_at
           FROM main.agent_control_events WHERE event_id = ${activation.githubEventId}) AS completed,
          (SELECT last_event_sequence = ${activation.activationEventSequence}
           FROM main.agent_control_project_states WHERE project_id = ${projectId}) AS projected
      `,
        [{ identity: 1, source: 1, completed: 1, projected: 1 }],
      );
      assert.deepStrictEqual(
        yield* sql`
        SELECT
          activation.event_id = ${activation.activationEventId}
            AND activation.aggregate_kind = 'project-controller'
            AND activation.stream_id = ${projectId}
            AND activation.event_type = 'agentControl.project.mode.changed'
            AND activation.actor_authority = 'human'
            AND activation.sequence = ${activation.activationEventSequence}
            AND activation.stream_version = ${activation.activationEventStreamVersion}
            AND activation.command_id = ${activation.activationCommandId}
            AND activation.correlation_id = ${activation.activationCommandId}
            AND activation.causation_event_id IS NULL
            AND json_extract(activation.payload_json, '$.previousMode') = 'observe'
            AND json_extract(activation.payload_json, '$.mode') = 'run-once'
            AND json_extract(activation.payload_json, '$.pausedFromMode') IS NULL AS event_ok,
          github.aggregate_kind = 'github-intake'
            AND github.stream_id = ${projectId}
            AND github.event_type = 'agentControl.github.poll.succeeded'
            AND github.sequence = ${activation.githubEventSequence}
            AND github.stream_version = ${activation.githubEventStreamVersion}
            AND github.sequence < activation.sequence
            AND json_extract(github.payload_json, '$.projectId') = ${projectId} AS github_ok,
          reconcile.status = 'completed'
            AND reconcile.target_sequence = ${activation.githubIntakeSequence}
            AND reconcile.last_completed_sequence = ${activation.githubIntakeSequence}
            AND reconcile.revision = ${activation.reconcileRevision} AS reconcile_ok,
          project.mode = 'run-once' AND project.paused_from_mode IS NULL
            AND project.revision = ${activation.activationEventStreamVersion}
            AND project.last_event_sequence = ${activation.activationEventSequence}
            AND project.updated_at = ${activation.activatedAt} AS project_ok
        FROM main.agent_control_events activation
        JOIN main.agent_control_events github ON github.event_id = ${activation.githubEventId}
        JOIN main.agent_control_task_reconcile_states reconcile
          ON reconcile.project_id = ${projectId}
        JOIN main.agent_control_project_states project ON project.project_id = ${projectId}
        WHERE activation.event_id = ${activation.activationEventId}
      `,
        [{ event_ok: 1, github_ok: 1, reconcile_ok: 1, project_ok: 1 }],
      );
      assert.deepStrictEqual(
        yield* sql`
        SELECT EXISTS (
          SELECT 1
          FROM main.agent_control_events activation
          JOIN main.agent_control_events github
            ON github.event_id = ${activation.githubEventId}
          JOIN main.agent_control_task_reconcile_states reconcile
            ON reconcile.project_id = ${projectId}
          JOIN main.agent_control_project_states project
            ON project.project_id = ${projectId}
          WHERE activation.event_id = ${activation.activationEventId}
            AND activation.aggregate_kind = 'project-controller'
            AND activation.stream_id = ${projectId}
            AND activation.event_type = 'agentControl.project.mode.changed'
            AND activation.actor_authority = 'human'
            AND activation.sequence = ${activation.activationEventSequence}
            AND activation.stream_version = ${activation.activationEventStreamVersion}
            AND activation.command_id = ${activation.activationCommandId}
            AND activation.correlation_id = ${activation.activationCommandId}
            AND activation.causation_event_id IS NULL
            AND json_extract(activation.payload_json, '$.previousMode') = 'observe'
            AND json_extract(activation.payload_json, '$.mode') = 'run-once'
            AND json_extract(activation.payload_json, '$.pausedFromMode') IS NULL
            AND github.aggregate_kind = 'github-intake'
            AND github.stream_id = ${projectId}
            AND github.event_type = 'agentControl.github.poll.succeeded'
            AND github.sequence = ${activation.githubEventSequence}
            AND github.stream_version = ${activation.githubEventStreamVersion}
            AND github.sequence < activation.sequence
            AND ${activation.githubIntakeSequence} = github.sequence
            AND json_extract(github.payload_json, '$.projectId') = ${projectId}
            AND json_extract(github.payload_json, '$.completedAt') = github.occurred_at
            AND t3_run_once_source_fingerprint_match(
              ${activation.sourceFingerprint}, ${projectId}, ${activation.githubIntakeSequence},
              ${activation.githubEventStreamVersion}, ${activation.githubEventStreamVersion},
              json_extract(github.payload_json, '$.repository.repositoryNodeId'),
              json_array_length(json_extract(github.payload_json, '$.issues'))
            ) = 1
            AND t3_run_once_activation_identity_match(
              ${activation.runId}, ${projectId}, ${activation.activationEventId},
              ${activation.activationEventSequence}, ${activation.activationEventStreamVersion},
              ${activation.activationCommandId}
            ) = 1
            AND reconcile.status = 'completed'
            AND reconcile.target_sequence = ${activation.githubIntakeSequence}
            AND reconcile.last_completed_sequence = ${activation.githubIntakeSequence}
            AND reconcile.revision = ${activation.reconcileRevision}
            AND project.mode = 'run-once' AND project.paused_from_mode IS NULL
            AND project.revision = ${activation.activationEventStreamVersion}
            AND project.last_event_sequence = ${activation.activationEventSequence}
            AND project.updated_at = ${activation.activatedAt}
            AND NOT EXISTS (
              SELECT 1 FROM main.agent_control_events later
              WHERE later.aggregate_kind = 'github-intake'
                AND later.stream_id = ${projectId}
                AND later.event_type = 'agentControl.github.poll.succeeded'
                AND later.sequence > ${activation.githubEventSequence}
                AND later.sequence < ${activation.activationEventSequence}
            )
            AND NOT EXISTS (
              SELECT 1 FROM main.agent_control_run_once_states existing
              WHERE existing.project_id = ${projectId} AND existing.status = 'active'
            )
        ) AS ok
      `,
        [{ ok: 1 }],
      );
      assert.deepStrictEqual(yield* admitRunOnceActivation(sql, activation), { replayed: false });
      const initialState = {
        projectId,
        status: "active" as const,
        taskId: null,
        stageRunId: null,
        leaseId: null,
        worktreeReservationId: null,
        controlledThreadReservationId: null,
        terminalTaskEventId: null,
        activationProjectRevision: 2,
        resetProjectRevision: null,
      };
      const step = {
        runId: activation.runId,
        projectId,
        ordinal: 1,
        step: "activation-admitted" as const,
        payload: { schemaVersion: 1, activation: activation as unknown as JsonValue },
        bindings: {},
        state: initialState,
        recordedAt: at,
      };
      const first = yield* writeRunOnceStep(sql, step);
      assert.isFalse(first.replayed);
      assert.match(first.publicationId, /^run-once-publication-/u);
      const before = (yield* sql<{
        readonly changes: number;
      }>`SELECT total_changes() AS changes`)[0]!.changes;
      assert.deepStrictEqual(yield* admitRunOnceActivation(sql, activation), { replayed: true });
      assert.isTrue((yield* writeRunOnceStep(sql, step)).replayed);
      const after = (yield* sql<{
        readonly changes: number;
      }>`SELECT total_changes() AS changes`)[0]!.changes;
      assert.equal(after, before);

      const divergent = yield* Effect.result(writeRunOnceStep(sql, { ...step, recordedAt: later }));
      assert.equal(divergent._tag, "Failure");
      if (divergent._tag === "Failure") {
        assert.equal(divergent.failure.reason, "identity-mismatch");
      }

      const noEligible = yield* writeRunOnceStep(sql, {
        ...step,
        ordinal: 2,
        step: "no-eligible-task",
        payload: { schemaVersion: 1, outcome: "no-eligible-task" },
      });
      assert.isFalse(noEligible.replayed);
      const modeCommandId = deriveRunOnceCommandId(activation.runId, 3, "mode-reset");
      const modeEventId = EventId.make("run-once-mode-reset-event");
      const modeSequence = yield* insertEvent(sql, {
        eventId: modeEventId,
        aggregateKind: "project-controller",
        streamVersion: 3,
        eventType: "agentControl.project.mode.changed",
        commandId: modeCommandId,
        authority: "system",
        payload: {
          projectId,
          previousMode: "run-once",
          mode: "observe",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      });
      yield* sql`
        UPDATE main.agent_control_project_states
        SET mode = 'observe', revision = 3, last_event_sequence = ${modeSequence}
        WHERE project_id = ${projectId}
      `;
      yield* writeRunOnceStep(sql, {
        ...step,
        ordinal: 3,
        step: "mode-reset",
        payload: { schemaVersion: 1, projectRevision: 3 },
        bindings: {
          modeEventId,
          modeEventSequence: modeSequence,
          modeEventStreamVersion: 3,
        },
        state: { ...initialState, resetProjectRevision: 3 },
      });
      yield* writeRunOnceStep(sql, {
        ...step,
        ordinal: 4,
        step: "completed",
        payload: { schemaVersion: 1, status: "no-eligible-task" },
        state: { ...initialState, status: "no-eligible-task", resetProjectRevision: 3 },
      });
      assert.deepStrictEqual(
        yield* sql`SELECT status, last_step AS "lastStep", next_ordinal AS "nextOrdinal"
          FROM main.agent_control_run_once_states WHERE run_id = ${activation.runId}`,
        [{ status: "no-eligible-task", lastStep: "completed", nextOrdinal: 5 }],
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }),
  );
});
