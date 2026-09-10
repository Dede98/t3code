import {
  AgentControlEvent,
  AgentControlRunOnceActivation,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { canonicalJson } from "../../agentControl/initialPlanning/eventEvidence.ts";
import {
  admitRunOnceActivation,
  fingerprintRunOnceModeCommand,
  loadRunOnceModeAuthority,
  writeRunOnceStepInTransaction,
} from "../../agentControl/runOnce/authority.ts";
import { deriveAgentControlRunOnceId } from "../../agentControl/runOnce/identity.ts";
import { fingerprintAgentControlRunOnceSource } from "../../agentControl/runOnce/source.ts";

const decodeEvent = Schema.decodeUnknownEffect(AgentControlEvent);
const decodeActivation = Schema.decodeUnknownEffect(AgentControlRunOnceActivation);

const at = "2026-09-10T20:00:00.000Z";
const layer = it.layer(NodeSqliteClient.layerMemory());
const bytes = (value: Parameters<typeof canonicalJson>[0]) =>
  new TextEncoder().encode(canonicalJson(value));

layer("selected-task Run-Once activation authority", (it) => {
  it.effect(
    "reproduces the old trigger rejection, then durably admits and replays the exact selected activation",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 80 });
        const projectId = ProjectId.make("selected-activation-project");
        const taskId = AgentControlTaskId.make("selected-activation-task");
        const commandId = CommandId.make("selected-activation-command");
        const eventId = EventId.make("selected-activation-event");
        const source = {
          projectId,
          repository: { repositoryNodeId: "repo" },
          completedAt: at,
          issues: [],
        };
        const github = yield* sql<{ sequence: number }>`INSERT INTO agent_control_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, causation_event_id, correlation_id, actor_authority, payload_json, metadata_json)
      VALUES ('github-source', 'github-intake', ${projectId}, 2, 'agentControl.github.poll.succeeded', ${at}, 'github-source-command', NULL, 'github-source-command', 'controller', ${canonicalJson(source)}, '{"schemaVersion":1}') RETURNING sequence`;
        const payload = {
          projectId,
          previousMode: "observe",
          mode: "run-once",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
          runOnceTaskId: taskId,
        };
        const inserted = yield* sql<{ sequence: number }>`INSERT INTO agent_control_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, causation_event_id, correlation_id, actor_authority, payload_json, metadata_json)
      VALUES (${eventId}, 'project-controller', ${projectId}, 2, 'agentControl.project.mode.changed', ${at}, ${commandId}, NULL, ${commandId}, 'human', ${canonicalJson(payload)}, '{"schemaVersion":1}') RETURNING sequence`;
        const event = yield* decodeEvent({
          eventId,
          aggregateKind: "project-controller",
          aggregateId: projectId,
          streamVersion: 2,
          sequence: inserted[0]!.sequence,
          type: "agentControl.project.mode.changed",
          occurredAt: at,
          commandId,
          causationEventId: null,
          correlationId: commandId,
          authority: "human",
          payload,
          metadata: { schemaVersion: 1 },
        });
        const fingerprint = fingerprintRunOnceModeCommand({
          commandId,
          projectId,
          expectedRevision: 1,
          mode: "run-once",
          runOnceTaskId: taskId,
        });
        yield* sql`INSERT INTO agent_control_command_receipts
      (command_id, command_fingerprint, authority, aggregate_kind, aggregate_id, status, result_sequence, result_stream_version, event_created, accepted_at, error_code)
      VALUES (${commandId}, ${fingerprint}, 'human', 'project-controller', ${projectId}, 'accepted', ${event.sequence}, 2, 1, ${at}, NULL)`;
        yield* sql`INSERT INTO agent_control_project_states (project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at)
      VALUES (${projectId}, 'run-once', NULL, 2, ${event.sequence}, ${at})`;
        yield* sql`INSERT INTO agent_control_task_reconcile_states (project_id, target_sequence, last_completed_sequence, revision, status, updated_at)
      VALUES (${projectId}, ${github[0]!.sequence}, ${github[0]!.sequence}, 1, 'completed', ${at})`;
        const activation = yield* decodeActivation({
          schemaVersion: 1,
          runId: deriveAgentControlRunOnceId({
            projectId,
            activationEventId: eventId,
            activationEventSequence: event.sequence,
            activationEventStreamVersion: 2,
            activationCommandId: commandId,
          }),
          projectId,
          activationEventId: eventId,
          activationEventSequence: event.sequence,
          activationEventStreamVersion: 2,
          activationCommandId: commandId,
          githubIntakeSequence: github[0]!.sequence,
          githubEventId: "github-source",
          githubEventSequence: github[0]!.sequence,
          githubEventStreamVersion: 2,
          reconcileRevision: 1,
          sourceFingerprint: fingerprintAgentControlRunOnceSource({
            schemaVersion: 1,
            projectId,
            githubIntakeSequence: github[0]!.sequence,
            githubProjectionRevision: 2,
            githubConfigRevision: 2,
            repositoryNodeId: "repo",
            pollStatus: "success",
            expectedIssueCount: 0,
          }),
          activatedAt: at,
          originMode: "observe",
          armedDispatchId: null,
          armedClaimId: null,
          armedMarkerId: null,
        });
        const admit = sql.withTransaction(
          Effect.gen(function* () {
            const authority = yield* loadRunOnceModeAuthority(sql, projectId, event);
            yield* admitRunOnceActivation(sql, activation, authority);
            return yield* writeRunOnceStepInTransaction(sql, {
              runId: activation.runId,
              projectId,
              ordinal: 1,
              step: "activation-admitted",
              payload: { schemaVersion: 1, activation: { ...activation } },
              bindings: {},
              state: {
                projectId,
                status: "active",
                taskId: null,
                stageRunId: null,
                leaseId: null,
                worktreeReservationId: null,
                controlledThreadReservationId: null,
                terminalTaskEventId: null,
                activationProjectRevision: 2,
                resetProjectRevision: null,
              },
              recordedAt: at,
            });
          }),
        );
        const rejected = yield* Effect.exit(admit);
        assert.equal(rejected._tag, "Failure");
        if (rejected._tag === "Failure")
          assert.include(
            Cause.pretty(rejected.cause),
            "run-once activation event authority is inconsistent",
          );
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count FROM agent_control_run_once_activations`,
          [{ count: 0 }],
        );
        const untouchedBefore =
          yield* sql`SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND name LIKE 'agent_control_run_once_%' AND name <> 'agent_control_run_once_activation_event_validate' ORDER BY name`;
        yield* runMigrations({ toMigrationInclusive: 81 });
        assert.deepStrictEqual(
          yield* sql`SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND name LIKE 'agent_control_run_once_%' AND name <> 'agent_control_run_once_activation_event_validate' ORDER BY name`,
          untouchedBefore,
        );
        yield* admit;
        yield* admit;
        assert.deepStrictEqual(
          yield* sql`SELECT status, last_step AS step, next_ordinal AS ordinal FROM agent_control_run_once_states WHERE run_id=${activation.runId}`,
          [{ status: "active", step: "activation-admitted", ordinal: 2 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count FROM agent_control_run_once_step_markers WHERE run_id=${activation.runId}`,
          [{ count: 1 }],
        );
      }),
  );

  it.effect(
    "keeps legacy guards strict and rejects changed, missing, null and extra task authority",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projectId = "guard-project",
          commandId = "guard-command",
          taskId = "guard-task";
        const legacy = {
          changedAt: at,
          mode: "run-once",
          pausedFromMode: null,
          previousMode: "observe",
          previousPausedFromMode: null,
          projectId,
        };
        const selected = { ...legacy, runOnceTaskId: taskId };
        const metadata = bytes({ schemaVersion: 1 });
        const oldFingerprint = fingerprintRunOnceModeCommand({
          projectId,
          commandId,
          expectedRevision: 1,
          mode: "run-once",
        });
        const fingerprint = fingerprintRunOnceModeCommand({
          projectId,
          commandId,
          expectedRevision: 1,
          mode: "run-once",
          runOnceTaskId: taskId,
        });
        const valid = yield* sql`SELECT
      t3_run_once_mode_command_fingerprint_match(${oldFingerprint},${commandId},${projectId},1,'run-once') AS oldCommand,
      t3_run_once_mode_event_match(${bytes(legacy)},${metadata},${projectId},'observe','run-once',NULL,NULL,${at}) AS oldEvent,
      t3_run_once_mode_command_fingerprint_match(${fingerprint},${commandId},${projectId},1,'run-once',${taskId}) AS selectedCommand,
      t3_run_once_mode_event_match(${bytes(selected)},${metadata},${projectId},'observe','run-once',NULL,NULL,${at},${taskId}) AS selectedEvent,
      t3_run_once_mode_event_match(${bytes(selected)},${metadata},${projectId},'observe','run-once',NULL,NULL,${at}) AS legacyRejectsTask`;
        assert.deepStrictEqual(valid, [
          {
            oldCommand: 1,
            oldEvent: 1,
            selectedCommand: 1,
            selectedEvent: 1,
            legacyRejectsTask: 0,
          },
        ]);
        for (const wrong of [null, "different", "", " bad ", 1]) {
          assert.deepStrictEqual(
            yield* sql`SELECT
        t3_run_once_mode_command_fingerprint_match(${fingerprint},${commandId},${projectId},1,'run-once',${wrong}) AS command,
        t3_run_once_mode_event_match(${bytes(selected)},${metadata},${projectId},'observe','run-once',NULL,NULL,${at},${wrong}) AS event`,
            [{ command: 0, event: 0 }],
          );
        }
        for (const payload of [
          legacy,
          { ...legacy, runOnceTaskId: null },
          { ...selected, extra: true },
        ]) {
          assert.deepStrictEqual(
            yield* sql`SELECT t3_run_once_mode_event_match(${bytes(payload)},${metadata},${projectId},'observe','run-once',NULL,NULL,${at},${taskId}) AS result`,
            [{ result: 0 }],
          );
        }
      }),
  );
});
