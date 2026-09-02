import {
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlRunOnceActivation,
  type AgentControlRunOnceStep,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson, type JsonValue } from "../initialPlanning/eventEvidence.ts";
import {
  admitRunOnceActivation,
  fingerprintRunOnceModeCommand,
  writeRunOnceStep,
} from "./authority.ts";
import { deriveAgentControlRunOnceId } from "./identity.ts";
import { fingerprintAgentControlRunOnceSource } from "./source.ts";

const at = "2026-08-31T14:00:00.000Z";
const targets = [
  "task-selected",
  "stage-prepared",
  "lease-reserved",
  "worktree-ready",
  "thread-activated",
] as const satisfies ReadonlyArray<AgentControlRunOnceStep>;

it.live("terminally supersedes every active step before admitting a new activation", () =>
  Effect.scoped(
    Effect.forEach(
      targets,
      (target) =>
        Effect.scoped(
          Effect.gen(function* () {
            const suffix = target;
            const projectId = ProjectId.make(`run-once-takeover-${suffix}`);
            const taskId = AgentControlTaskId.make(`run-once-takeover-task-${suffix}`);
            const scope = yield* Scope.make("sequential");
            const context = yield* Layer.buildWithScope(NodeSqliteClient.layerMemory(), scope);
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            yield* sql`DROP TRIGGER main.agent_control_run_once_downstream_evidence_validate`;

            const insertEvent = (input: {
              readonly eventId: string;
              readonly aggregateKind: string;
              readonly streamId: string;
              readonly streamVersion: number;
              readonly eventType: string;
              readonly commandId: string;
              readonly authority: string;
              readonly payload: JsonValue;
            }) =>
              sql<{ readonly sequence: number }>`
                INSERT INTO main.agent_control_events (
                  event_id, aggregate_kind, stream_id, stream_version, event_type,
                  occurred_at, command_id, causation_event_id, correlation_id,
                  actor_authority, payload_json, metadata_json
                ) VALUES (
                  ${input.eventId}, ${input.aggregateKind}, ${input.streamId},
                  ${input.streamVersion}, ${input.eventType}, ${at}, ${input.commandId}, NULL,
                  ${input.commandId}, ${input.authority}, ${canonicalJson(input.payload)},
                  '{"schemaVersion":1}'
                ) RETURNING sequence
              `.pipe(Effect.map((rows) => rows[0]!.sequence));
            const insertModeReceipt = (input: {
              readonly commandId: string;
              readonly authority: "human" | "system";
              readonly expectedRevision: number;
              readonly mode: "manual" | "observe" | "run-once";
              readonly sequence: number;
              readonly streamVersion: number;
            }) =>
              sql`
                INSERT INTO main.agent_control_command_receipts (
                  command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
                  status, result_sequence, result_stream_version, event_created, accepted_at,
                  error_code
                ) VALUES (
                  ${input.commandId}, ${fingerprintRunOnceModeCommand({
                    commandId: input.commandId,
                    projectId,
                    expectedRevision: input.expectedRevision,
                    mode: input.mode,
                  })}, ${input.authority}, 'project-controller', ${projectId}, 'accepted',
                  ${input.sequence}, ${input.streamVersion}, 1, ${at}, NULL
                )
              `;

            yield* insertEvent({
              eventId: `${suffix}-observe-event`,
              aggregateKind: "project-controller",
              streamId: projectId,
              streamVersion: 1,
              eventType: "agentControl.project.mode.changed",
              commandId: `${suffix}-observe-command`,
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
            yield* insertEvent({
              eventId: `${suffix}-github-config-event`,
              aggregateKind: "github-intake",
              streamId: projectId,
              streamVersion: 1,
              eventType: "agentControl.github.config.set",
              commandId: `${suffix}-github-config-command`,
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
                repository: { repositoryNodeId: "takeover-repository", nameWithOwner: "o/r" },
                configuredAt: at,
              },
            });
            const githubSequence = yield* insertEvent({
              eventId: `${suffix}-github-success-event`,
              aggregateKind: "github-intake",
              streamId: projectId,
              streamVersion: 2,
              eventType: "agentControl.github.poll.succeeded",
              commandId: `${suffix}-github-success-command`,
              authority: "controller",
              payload: {
                projectId,
                repository: { repositoryNodeId: "takeover-repository", nameWithOwner: "o/r" },
                attemptedAt: at,
                completedAt: at,
                cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
                issues: [],
              },
            });
            const activationCommandId = CommandId.make(`${suffix}-activation-command`);
            const activationEventId = EventId.make(`${suffix}-activation-event`);
            const activationPayload = {
              projectId,
              previousMode: "observe",
              mode: "run-once",
              previousPausedFromMode: null,
              pausedFromMode: null,
              changedAt: at,
            } as const;
            const activationSequence = yield* insertEvent({
              eventId: activationEventId,
              aggregateKind: "project-controller",
              streamId: projectId,
              streamVersion: 2,
              eventType: "agentControl.project.mode.changed",
              commandId: activationCommandId,
              authority: "human",
              payload: activationPayload,
            });
            yield* insertModeReceipt({
              commandId: activationCommandId,
              authority: "human",
              expectedRevision: 1,
              mode: "run-once",
              sequence: activationSequence,
              streamVersion: 2,
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
            const taskState = {
              schemaVersion: 1,
              taskId,
              source: {
                projectId,
                repositoryNodeId: "takeover-repository",
                issueNodeId: `issue-${suffix}`,
                issueNumber: 1,
                issueUrl: "https://example.test/1",
              },
              status: "candidate",
              sourceGate: "eligible",
              stage: "intake",
              sourceUpdatedAt: at,
              githubIntakeSequence: githubSequence,
              sourceSnapshot: {
                repositoryNodeId: "takeover-repository",
                issueNodeId: `issue-${suffix}`,
                number: 1,
                url: "https://example.test/1",
                state: "open",
                title: "takeover",
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
              sequence: githubSequence + 1,
            } as const;
            yield* sql`
              INSERT INTO main.agent_control_task_states (
                task_id, project_id, repository_node_id, issue_node_id, issue_number, issue_url,
                status, source_gate, stage, source_updated_at, github_intake_sequence, state_json,
                created_at, updated_at, revision, last_event_sequence
              ) VALUES (
                ${taskId}, ${projectId}, 'takeover-repository', ${`issue-${suffix}`}, 1,
                'https://example.test/1', 'candidate', 'eligible', 'intake', ${at},
                ${githubSequence}, ${new TextEncoder().encode(
                  canonicalJson(taskState as unknown as JsonValue),
                )}, ${at}, ${at}, 1, ${githubSequence + 1}
              )
            `;
            const runId = deriveAgentControlRunOnceId({
              projectId,
              activationEventId,
              activationEventSequence: activationSequence,
              activationEventStreamVersion: 2,
              activationCommandId,
            });
            const activation = {
              schemaVersion: 1,
              runId,
              projectId,
              activationEventId,
              activationEventSequence: activationSequence,
              activationEventStreamVersion: 2,
              activationCommandId,
              githubIntakeSequence: githubSequence,
              githubEventId: EventId.make(`${suffix}-github-success-event`),
              githubEventSequence: githubSequence,
              githubEventStreamVersion: 2,
              reconcileRevision: 1,
              sourceFingerprint: fingerprintAgentControlRunOnceSource({
                schemaVersion: 1,
                projectId,
                githubIntakeSequence: githubSequence,
                githubProjectionRevision: 2,
                githubConfigRevision: 2,
                repositoryNodeId: "takeover-repository",
                pollStatus: "success",
                expectedIssueCount: 0,
              }),
              activatedAt: at,
              originMode: "observe",
              armedDispatchId: null,
              armedClaimId: null,
              armedMarkerId: null,
            } satisfies AgentControlRunOnceActivation;
            const activationFingerprint = fingerprintRunOnceModeCommand({
              commandId: activationCommandId,
              projectId,
              expectedRevision: 1,
              mode: "run-once",
            });
            yield* admitRunOnceActivation(sql, activation, {
              expectedRevision: 1,
              commandFingerprint: activationFingerprint,
              eventPayloadBytes: new TextEncoder().encode(canonicalJson(activationPayload)),
              eventMetadataBytes: new TextEncoder().encode('{"schemaVersion":1}'),
            });

            let state = {
              projectId,
              status: "active" as const,
              taskId: null as string | null,
              stageRunId: null as string | null,
              leaseId: null as string | null,
              worktreeReservationId: null as string | null,
              controlledThreadReservationId: null as string | null,
              terminalTaskEventId: null,
              activationProjectRevision: 2,
              resetProjectRevision: null as number | null,
            };
            const steps: ReadonlyArray<AgentControlRunOnceStep> = [
              "activation-admitted",
              "task-selected",
              "stage-prepared",
              "lease-reserved",
              "worktree-ready",
              "thread-activated",
            ];
            for (const [index, step] of steps.entries()) {
              if (step === "task-selected") state.taskId = taskId;
              if (step === "stage-prepared") state.stageRunId = `stage-${suffix}`;
              if (step === "lease-reserved") state.leaseId = `lease-${suffix}`;
              if (step === "worktree-ready") {
                state.worktreeReservationId = `worktree-${suffix}`;
              }
              if (step === "thread-activated") {
                state.controlledThreadReservationId = `thread-${suffix}`;
              }
              yield* writeRunOnceStep(sql, {
                runId,
                projectId,
                ordinal: index + 1,
                step,
                payload: { schemaVersion: 1, step },
                bindings: {
                  taskId: state.taskId,
                  stageRunId: state.stageRunId,
                  leaseId: state.leaseId,
                  worktreeReservationId: state.worktreeReservationId,
                  controlledThreadReservationId: state.controlledThreadReservationId,
                },
                state,
                recordedAt: at,
              });
              if (step === target) break;
            }

            const takeoverCommandId = CommandId.make(`${suffix}-takeover-command`);
            const takeoverPayload = {
              projectId,
              previousMode: "run-once",
              mode: "manual",
              previousPausedFromMode: null,
              pausedFromMode: null,
              changedAt: at,
            } as const;
            const takeoverSequence = yield* insertEvent({
              eventId: `${suffix}-takeover-event`,
              aggregateKind: "project-controller",
              streamId: projectId,
              streamVersion: 3,
              eventType: "agentControl.project.mode.changed",
              commandId: takeoverCommandId,
              authority: "human",
              payload: takeoverPayload,
            });
            yield* insertModeReceipt({
              commandId: takeoverCommandId,
              authority: "human",
              expectedRevision: 2,
              mode: "manual",
              sequence: takeoverSequence,
              streamVersion: 3,
            });
            yield* sql`
              UPDATE main.agent_control_project_states SET mode = 'manual', revision = 3,
                last_event_sequence = ${takeoverSequence} WHERE project_id = ${projectId}
            `;
            const takeoverAuthority = {
              expectedRevision: 2,
              commandFingerprint: fingerprintRunOnceModeCommand({
                commandId: takeoverCommandId,
                projectId,
                expectedRevision: 2,
                mode: "manual",
              }),
              eventPayloadBytes: new TextEncoder().encode(canonicalJson(takeoverPayload)),
              eventMetadataBytes: new TextEncoder().encode('{"schemaVersion":1}'),
            };
            const supersededOrdinal = steps.indexOf(target) + 2;
            state.resetProjectRevision = 3;
            yield* writeRunOnceStep(sql, {
              runId,
              projectId,
              ordinal: supersededOrdinal,
              step: "mode-reset-superseded",
              payload: { schemaVersion: 1, projectRevision: 3 },
              bindings: {
                taskId: state.taskId,
                stageRunId: state.stageRunId,
                leaseId: state.leaseId,
                worktreeReservationId: state.worktreeReservationId,
                controlledThreadReservationId: state.controlledThreadReservationId,
                modeEventId: EventId.make(`${suffix}-takeover-event`),
                modeEventSequence: takeoverSequence,
                modeEventStreamVersion: 3,
                modeExpectedRevision: takeoverAuthority.expectedRevision,
                modeCommandFingerprint: takeoverAuthority.commandFingerprint,
                modeEventPayloadBytes: takeoverAuthority.eventPayloadBytes,
                modeEventMetadataBytes: takeoverAuthority.eventMetadataBytes,
              },
              state,
              recordedAt: at,
            });
            yield* writeRunOnceStep(sql, {
              runId,
              projectId,
              ordinal: supersededOrdinal + 1,
              step: "completed",
              payload: { schemaVersion: 1, status: "completed" },
              bindings: {
                taskId: state.taskId,
                stageRunId: state.stageRunId,
                leaseId: state.leaseId,
                worktreeReservationId: state.worktreeReservationId,
                controlledThreadReservationId: state.controlledThreadReservationId,
              },
              state: { ...state, status: "completed" },
              recordedAt: at,
            });

            const observeTwo = yield* insertEvent({
              eventId: `${suffix}-observe-two-event`,
              aggregateKind: "project-controller",
              streamId: projectId,
              streamVersion: 4,
              eventType: "agentControl.project.mode.changed",
              commandId: `${suffix}-observe-two-command`,
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
            const nextCommandId = CommandId.make(`${suffix}-next-activation-command`);
            const nextEventId = EventId.make(`${suffix}-next-activation-event`);
            const nextPayload = { ...activationPayload };
            const nextSequence = yield* insertEvent({
              eventId: nextEventId,
              aggregateKind: "project-controller",
              streamId: projectId,
              streamVersion: 5,
              eventType: "agentControl.project.mode.changed",
              commandId: nextCommandId,
              authority: "human",
              payload: nextPayload,
            });
            yield* insertModeReceipt({
              commandId: nextCommandId,
              authority: "human",
              expectedRevision: 4,
              mode: "run-once",
              sequence: nextSequence,
              streamVersion: 5,
            });
            yield* sql`
              UPDATE main.agent_control_project_states SET mode = 'run-once', revision = 5,
                last_event_sequence = ${nextSequence} WHERE project_id = ${projectId}
            `;
            const nextRunId = deriveAgentControlRunOnceId({
              projectId,
              activationEventId: nextEventId,
              activationEventSequence: nextSequence,
              activationEventStreamVersion: 5,
              activationCommandId: nextCommandId,
            });
            const nextActivation = {
              ...activation,
              runId: nextRunId,
              activationEventId: nextEventId,
              activationEventSequence: nextSequence,
              activationEventStreamVersion: 5,
              activationCommandId: nextCommandId,
            };
            assert.deepStrictEqual(
              yield* admitRunOnceActivation(sql, nextActivation, {
                expectedRevision: 4,
                commandFingerprint: fingerprintRunOnceModeCommand({
                  commandId: nextCommandId,
                  projectId,
                  expectedRevision: 4,
                  mode: "run-once",
                }),
                eventPayloadBytes: new TextEncoder().encode(canonicalJson(nextPayload)),
                eventMetadataBytes: new TextEncoder().encode('{"schemaVersion":1}'),
              }),
              { replayed: false },
              target,
            );
            assert.deepStrictEqual(
              yield* sql`
                SELECT status, last_step AS "lastStep" FROM main.agent_control_run_once_states
                WHERE run_id = ${runId}
              `,
              [{ status: "completed", lastStep: "completed" }],
              target,
            );
            void observeTwo;
          }),
        ),
      { concurrency: 5, discard: true },
    ),
  ),
);
