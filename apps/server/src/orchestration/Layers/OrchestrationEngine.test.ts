import {
  CheckpointRef,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  ProviderInstanceId,
  RuntimeItemId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import { describe, expect, it } from "vite-plus/test";

import { PersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import {
  OrchestrationCommandReceiptRepository,
  type OrchestrationCommandReceipt,
} from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  OrchestrationEventStore,
  type OrchestrationEventStoreShape,
} from "../../persistence/Services/OrchestrationEventStore.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  OrchestrationProjectionPipeline,
  type OrchestrationProjectionPipelineShape,
} from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ServerConfig } from "../../config.ts";
import {
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT,
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX,
} from "../../agentControl/verificationTurn/runtimeEventAuthority.ts";
import { classifyVerificationResultRuntimeEventAuthorityRace } from "./OrchestrationEngine.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asCheckpointRef = (value: string): CheckpointRef => CheckpointRef.make(value);

async function createOrchestrationSystem(
  persistenceLayer:
    | typeof SqlitePersistenceMemory
    | ReturnType<typeof makeSqlitePersistenceLive> = SqlitePersistenceMemory,
) {
  const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
    prefix: "t3-orchestration-engine-test-",
  });
  const commandReceiptLayer = OrchestrationCommandReceiptRepositoryLive;
  const orchestrationLayer = Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(commandReceiptLayer),
    ),
    OrchestrationProjectionSnapshotQueryLive,
    commandReceiptLayer,
  ).pipe(
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provideMerge(persistenceLayer),
    Layer.provideMerge(ServerConfigLayer),
    Layer.provideMerge(NodeServices.layer),
  );
  const runtime = ManagedRuntime.make(orchestrationLayer);
  const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
  const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
  const commandReceipts = await runtime.runPromise(
    Effect.service(OrchestrationCommandReceiptRepository),
  );
  const sql = await runtime.runPromise(Effect.service(SqlClient.SqlClient));
  return {
    engine,
    sql,
    readModel: () => runtime.runPromise(snapshotQuery.getSnapshot()),
    run: <A, E>(effect: Effect.Effect<A, E>) => runtime.runPromise(effect),
    getReceipt: (commandId: CommandId): Promise<OrchestrationCommandReceipt | null> =>
      runtime.runPromise(
        commandReceipts.getByCommandId({ commandId }).pipe(Effect.map(Option.getOrNull)),
      ),
    dispose: () => runtime.dispose(),
  };
}

async function seedProjectDeleteReplay(
  system: Awaited<ReturnType<typeof createOrchestrationSystem>>,
  suffix: string,
  threadCount: number,
) {
  const occurredAt = "2026-01-01T00:00:00.000Z";
  const projectId = asProjectId(`project-delete-replay-${suffix}`);
  const commandId = CommandId.make(`command-delete-replay-${suffix}`);
  const sqlText = (value: string): string => `'${value.replaceAll("'", "''")}'`;
  const runSeedStage = async <A, E>(stage: string, effect: Effect.Effect<A, E>): Promise<A> => {
    try {
      return await system.run(effect);
    } catch (cause) {
      throw new Error(`project.delete replay seed failed at ${stage}`, { cause });
    }
  };
  await runSeedStage(
    "project.create",
    system.engine.dispatch({
      type: "project.create",
      commandId: CommandId.make(`command-delete-replay-project-${suffix}`),
      projectId,
      title: `Delete replay ${suffix}`,
      workspaceRoot: `/tmp/project-delete-replay-${suffix}`,
      createdAt: occurredAt,
    }),
  );
  if (threadCount > 0) {
    await runSeedStage(
      "projection_threads",
      system.sql.unsafe(`
        WITH RECURSIVE candidate(ordinal) AS (
          SELECT 1
          UNION ALL
          SELECT ordinal + 1 FROM candidate WHERE ordinal < ${threadCount}
        )
        INSERT INTO main.projection_threads (
          thread_id, project_id, title, branch, worktree_path, latest_turn_id,
          created_at, updated_at, deleted_at, runtime_mode, interaction_mode,
          model_selection_json, archived_at, latest_user_message_at,
          pending_approval_count, pending_user_input_count,
          has_actionable_proposed_plan, agent_control_json
        )
        SELECT printf('thread-delete-replay-${suffix}-%06d', ordinal), ${sqlText(projectId)},
          printf('Thread %d', ordinal), NULL, NULL, NULL,
          ${sqlText(occurredAt)}, ${sqlText(occurredAt)}, NULL,
          'approval-required', 'default', '{"instanceId":"codex","model":"gpt-5-codex"}',
          NULL, NULL, 0, 0, 0, NULL
        FROM candidate
      `).unprepared,
    );
    await runSeedStage(
      "thread.deleted events",
      system.sql.unsafe(`
        WITH RECURSIVE candidate(ordinal) AS (
          SELECT 1
          UNION ALL
          SELECT ordinal + 1 FROM candidate WHERE ordinal < ${threadCount}
        )
        INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        )
        SELECT printf('event-delete-replay-${suffix}-%06d', ordinal), 'thread',
          printf('thread-delete-replay-${suffix}-%06d', ordinal), 1, 'thread.deleted',
          ${sqlText(occurredAt)}, ${sqlText(commandId)}, NULL, ${sqlText(commandId)}, 'client',
          json_object(
            'threadId', printf('thread-delete-replay-${suffix}-%06d', ordinal),
            'deletedAt', ${sqlText(occurredAt)}
          ), '{}'
        FROM candidate
      `).unprepared,
    );
  }
  const terminalEventId = `event-delete-replay-${suffix}-project`;
  await runSeedStage(
    "project.deleted event",
    system.sql`
      INSERT INTO main.orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_kind,
        payload_json, metadata_json
      ) VALUES (
        ${terminalEventId}, 'project', ${projectId}, 2,
        'project.deleted', ${occurredAt}, ${commandId}, NULL, ${commandId}, 'client',
        ${JSON.stringify({ projectId, deletedAt: occurredAt })}, '{}'
      )
    `,
  );
  const [terminal] = await runSeedStage(
    "project.deleted sequence",
    system.sql<{ readonly sequence: number }>`
      SELECT sequence FROM main.orchestration_events WHERE event_id=${terminalEventId}
    `,
  );
  await runSeedStage(
    "accepted receipt",
    system.sql`
      INSERT INTO main.orchestration_command_receipts (
        command_id, authority, aggregate_kind, aggregate_id, accepted_at,
        result_sequence, status, error
      ) VALUES (
        ${commandId}, 'system', 'project', ${projectId}, ${occurredAt},
        ${terminal!.sequence}, 'accepted', NULL
      )
    `,
  );
  return {
    command: {
      type: "project.delete" as const,
      commandId,
      projectId,
      ...(threadCount === 0 ? {} : { force: true as const }),
    },
    result: { sequence: terminal!.sequence },
  };
}

function now() {
  return "2026-01-01T00:00:00.000Z";
}

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

describe("OrchestrationEngine", () => {
  it("classifies only exact RuntimeEventId capture-append race signals", () => {
    const error = (cause: unknown) =>
      new PersistenceSqlError({
        operation: "OrchestrationEventStore.append:query",
        detail: "injected capture append failure",
        cause,
      });
    for (const [label, cause, expected] of [
      [
        "authority trigger",
        { errcode: 1811, message: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT },
        "authority-trigger",
      ],
      [
        "authority index message",
        {
          errcode: 2067,
          message: `UNIQUE constraint failed: index '${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}'`,
        },
        "authority-index",
      ],
      [
        "authority index structured identity",
        { errcode: 2067, constraint: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX },
        "authority-index",
      ],
      ["busy", { errcode: 5, code: "SQLITE_BUSY" }, "busy"],
      ["busy snapshot", { errcode: 517, code: "SQLITE_BUSY_SNAPSHOT" }, "busy-snapshot"],
      ["locked", { errcode: 6, code: "SQLITE_LOCKED" }, null],
      ["cantopen", { errcode: 14, code: "SQLITE_CANTOPEN" }, null],
      ["ioerr", { errcode: 10, code: "SQLITE_IOERR" }, null],
      ["full", { errcode: 13, code: "SQLITE_FULL" }, null],
      ["readonly", { errcode: 8, code: "SQLITE_READONLY" }, null],
      ["corrupt", { errcode: 11, code: "SQLITE_CORRUPT" }, null],
      ["foreign key", { errcode: 787, code: "SQLITE_CONSTRAINT_FOREIGNKEY" }, null],
      [
        "foreign unique",
        { errcode: 2067, constraint: "foreign_unique", message: "UNIQUE constraint failed: x.y" },
        null,
      ],
      ["foreign trigger", { errcode: 1811, message: "foreign trigger failure" }, null],
      ["generic retryable", { isRetryable: true, reason: { _tag: "LockTimeoutError" } }, null],
      ["connection", { _tag: "ConnectionError", errcode: 14 }, null],
      ["connection with busy code", { _tag: "ConnectionError", errcode: 5 }, null],
      [
        "trigger text without code",
        { message: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT },
        null,
      ],
      [
        "index identity without code",
        { constraint: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX },
        null,
      ],
    ] as const) {
      expect(classifyVerificationResultRuntimeEventAuthorityRace(error(cause)), label).toBe(
        expected,
      );
    }
  });

  it("assigns system authority to internal dispatches and agent-control to the reserved path", async () => {
    const system = await createOrchestrationSystem();
    const createdAt = now();
    const systemCommandId = CommandId.make("cmd-authority-system");
    const clientCommandId = CommandId.make("cmd-authority-client");
    const agentControlCommandId = CommandId.make("cmd-authority-agent-control");

    await system.run(
      system.engine.dispatch({
        type: "project.create",
        commandId: systemCommandId,
        projectId: asProjectId("project-authority-system"),
        title: "System Authority",
        workspaceRoot: "/tmp/project-authority-system",
        createdAt,
      }),
    );
    await system.run(
      system.engine.dispatchClient({
        type: "project.create",
        commandId: clientCommandId,
        projectId: asProjectId("project-authority-client"),
        title: "Client Authority",
        workspaceRoot: "/tmp/project-authority-client",
        createdAt,
      }),
    );
    await system.run(
      system.engine.dispatchAgentControl({
        type: "project.create",
        commandId: agentControlCommandId,
        projectId: asProjectId("project-authority-agent-control"),
        title: "Agent Control Authority",
        workspaceRoot: "/tmp/project-authority-agent-control",
        createdAt,
      }),
    );

    await expect(system.getReceipt(systemCommandId)).resolves.toMatchObject({
      authority: "system",
    });
    await expect(system.getReceipt(clientCommandId)).resolves.toMatchObject({
      authority: "client",
    });
    await expect(system.getReceipt(agentControlCommandId)).resolves.toMatchObject({
      authority: "agent-control",
    });
    await system.dispose();
  });

  it("rejects command-id replay under a different authority", async () => {
    const system = await createOrchestrationSystem();
    const command = {
      type: "project.create" as const,
      commandId: CommandId.make("cmd-authority-replay"),
      projectId: asProjectId("project-authority-replay"),
      title: "Authority Replay",
      workspaceRoot: "/tmp/project-authority-replay",
      createdAt: now(),
    };

    const firstResult = await system.run(system.engine.dispatch(command));
    const sameAuthorityReplay = await system.run(system.engine.dispatch(command));

    expect(sameAuthorityReplay).toEqual(firstResult);

    await expect(system.run(system.engine.dispatchClient(command))).rejects.toMatchObject({
      _tag: "OrchestrationCommandAuthorityMismatchError",
      commandId: command.commandId,
      receiptAuthority: "system",
      attemptedAuthority: "client",
    });
    await system.dispose();
  });

  it("raw-validates and exhaustively binds every generic accepted-receipt replay", async () => {
    const seed = async (
      suffix: string,
      persistenceLayer:
        | typeof SqlitePersistenceMemory
        | ReturnType<typeof makeSqlitePersistenceLive> = SqlitePersistenceMemory,
    ) => {
      const system = await createOrchestrationSystem(persistenceLayer);
      const projectId = asProjectId(`project-generic-replay-${suffix}`);
      const threadId = ThreadId.make(`thread-generic-replay-${suffix}`);
      const command = {
        type: "thread.session.stop" as const,
        commandId: CommandId.make(`cmd-generic-replay-${suffix}`),
        threadId,
        createdAt: now(),
      };
      await system.run(
        system.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make(`cmd-project-generic-replay-${suffix}`),
          projectId,
          title: `Generic replay ${suffix}`,
          workspaceRoot: `/tmp/project-generic-replay-${suffix}`,
          createdAt: now(),
        }),
      );
      await system.run(
        system.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`cmd-thread-generic-replay-${suffix}`),
          threadId,
          projectId,
          title: `Generic replay ${suffix}`,
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt: now(),
        }),
      );
      const first = await system.run(system.engine.dispatch(command));
      return { system, command, first };
    };

    const assertRejectedWithoutWrites = async (
      suffix: string,
      corrupt: (sql: SqlClient.SqlClient, commandId: CommandId) => Effect.Effect<unknown, SqlError>,
      expectedTag: string = "OrchestrationCommandIdentityConflictError",
    ) => {
      const { system, command } = await seed(suffix);
      await system.run(corrupt(system.sql, command.commandId));
      const before = await system.run(
        system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      const snapshotBefore = await system.readModel();
      const exit = await system.run(Effect.exit(system.engine.dispatch(command)));
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        const failure = Cause.findErrorOption(exit.cause);
        expect(
          Option.isSome(failure) ? (failure.value as { readonly _tag?: string })._tag : "",
        ).toBe(expectedTag);
      }
      expect(
        await system.run(
          system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
      ).toEqual(before);
      expect(await system.readModel()).toEqual(snapshotBefore);
      await system.dispose();
    };

    const valid = await seed("valid");
    expect(await valid.system.run(valid.system.engine.dispatch(valid.command))).toEqual(
      valid.first,
    );
    await valid.system.dispose();

    await assertRejectedWithoutWrites(
      "missing-result",
      (sql, commandId) =>
        sql`DELETE FROM main.orchestration_events WHERE command_id = ${commandId}`,
    );
    await assertRejectedWithoutWrites(
      "wrong-result-sequence",
      (sql, commandId) =>
        sql`
        UPDATE main.orchestration_command_receipts
        SET result_sequence = result_sequence - 1
        WHERE command_id = ${commandId}
      `,
    );
    await assertRejectedWithoutWrites(
      "extra-text",
      (sql, commandId) =>
        sql`
        INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
        )
        SELECT event_id || '-extra', aggregate_kind, stream_id, stream_version + 1, event_type,
          occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        FROM main.orchestration_events WHERE command_id = ${commandId}
      `,
    );
    await assertRejectedWithoutWrites(
      "bounded-history",
      (sql, commandId) =>
        sql`
        WITH RECURSIVE candidate(ordinal) AS (
          SELECT 1
          UNION ALL
          SELECT ordinal + 1 FROM candidate WHERE ordinal < 1024
        )
        INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
        )
        SELECT event.event_id || '-bounded-' || candidate.ordinal,
          event.aggregate_kind, event.stream_id, event.stream_version + candidate.ordinal,
          event.event_type, event.occurred_at, event.command_id, event.causation_event_id,
          event.correlation_id, event.actor_kind, event.payload_json, event.metadata_json
        FROM main.orchestration_events event CROSS JOIN candidate
        WHERE event.command_id = ${commandId}
      `,
    );
    await assertRejectedWithoutWrites(
      "extra-blob",
      (sql, commandId) =>
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO main.orchestration_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
              command_id, causation_event_id, correlation_id, actor_kind,
              payload_json, metadata_json
            )
            SELECT event_id || '-blob', aggregate_kind, stream_id, stream_version + 1,
              event_type, occurred_at, command_id, causation_event_id, correlation_id,
              actor_kind, payload_json, metadata_json
            FROM main.orchestration_events WHERE command_id = ${commandId}
          `;
          yield* sql`DROP TRIGGER main.agent_control_orchestration_event_update_storage_validate`;
          yield* sql`
            UPDATE main.orchestration_events
            SET command_id = CAST(command_id AS BLOB)
            WHERE event_id LIKE '%-blob'
          `;
        }),
      "PersistenceDecodeError",
    );
    await assertRejectedWithoutWrites(
      "wrong-event-type",
      (sql, commandId) =>
        sql`
        UPDATE main.orchestration_events
        SET event_type = 'thread.turn-interrupt-requested',
          payload_json = json_object('threadId', stream_id, 'createdAt', occurred_at)
        WHERE command_id = ${commandId}
      `,
    );
    await assertRejectedWithoutWrites(
      "wrong-stream",
      (sql, commandId) =>
        sql`
        UPDATE main.orchestration_events
        SET stream_id = stream_id || '-wrong',
          payload_json = json_object('threadId', stream_id || '-wrong', 'createdAt', occurred_at)
        WHERE command_id = ${commandId}
      `,
      "PersistenceDecodeError",
    );
    await assertRejectedWithoutWrites(
      "wrong-correlation",
      (sql, commandId) =>
        sql`
        UPDATE main.orchestration_events
        SET causation_event_id = (
          SELECT event_id FROM main.orchestration_events
          WHERE event_type = 'thread.created' ORDER BY sequence DESC LIMIT 1
        ), correlation_id = 'wrong-correlation'
        WHERE command_id = ${commandId}
      `,
    );

    const natural = await seed("natural-chain");
    const turnCommand = {
      type: "thread.turn.start" as const,
      commandId: CommandId.make("cmd-generic-replay-natural-chain-turn"),
      threadId: natural.command.threadId,
      message: {
        messageId: asMessageId("message-generic-replay-natural-chain"),
        role: "user" as const,
        text: "natural chain",
        attachments: [],
      },
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      createdAt: now(),
    };
    const turnFirst = await natural.system.run(natural.system.engine.dispatch(turnCommand));
    expect(await natural.system.run(natural.system.engine.dispatch(turnCommand))).toEqual(
      turnFirst,
    );
    await natural.system.dispose();

    const directory = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-generic-accepted-replay-"),
    );
    const filename = NodePath.join(directory, "state.sqlite");
    try {
      const restartFirst = await seed("restart", makeSqlitePersistenceLive(filename));
      expect(await restartFirst.system.run(restartFirst.system.sql`PRAGMA journal_mode`)).toEqual([
        { journal_mode: "wal" },
      ]);
      await restartFirst.system.dispose();

      const restarted = await createOrchestrationSystem(makeSqlitePersistenceLive(filename));
      expect(await restarted.run(restarted.sql`PRAGMA journal_mode`)).toEqual([
        { journal_mode: "wal" },
      ]);
      expect(await restarted.run(restarted.engine.dispatch(restartFirst.command))).toEqual(
        restartFirst.first,
      );
      await restarted.dispose();
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  }, 60_000);

  it("replays thread.meta.update from the exact decider state before its candidate", async () => {
    const system = await createOrchestrationSystem();
    const projectId = asProjectId("project-meta-decider-replay");
    const threadId = ThreadId.make("thread-meta-decider-replay");
    await system.run(
      system.engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("command-meta-decider-project"),
        projectId,
        title: "Meta replay",
        workspaceRoot: "/tmp/project-meta-decider-replay",
        createdAt: now(),
      }),
    );
    await system.run(
      system.engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command-meta-decider-thread"),
        threadId,
        projectId,
        title: "Meta replay",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "main",
        worktreePath: null,
        createdAt: now(),
      }),
    );

    const variants = [
      {
        name: "no-expected-branch",
        command: { branch: "feature" },
        expectedBranch: "feature",
      },
      {
        name: "matching-expected-branch",
        command: { branch: "release", expectedBranch: "feature" },
        expectedBranch: "release",
      },
      {
        name: "mismatching-expected-branch",
        command: { branch: "attacker-requested", expectedBranch: "stale" },
        expectedBranch: "release",
      },
      {
        name: "title-only",
        command: { title: "Retitled" },
        expectedBranch: undefined,
      },
      { name: "noop", command: {}, expectedBranch: undefined },
      {
        name: "combined-meta",
        command: {
          title: "Combined",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.1-codex",
          },
          branch: null,
          expectedBranch: "release",
          worktreePath: "/tmp/meta-decider-worktree",
        },
        expectedBranch: null,
      },
    ] as const;
    let mismatchCommand:
      | Extract<OrchestrationCommand, { readonly type: "thread.meta.update" }>
      | undefined;
    for (const variant of variants) {
      const command = {
        type: "thread.meta.update" as const,
        commandId: CommandId.make(`command-meta-decider-${variant.name}`),
        threadId,
        ...variant.command,
      };
      const first = await system.run(system.engine.dispatch(command));
      const beforeReplay = await system.run(
        system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      const snapshotBefore = await system.readModel();
      expect(await system.run(system.engine.dispatch(command))).toEqual(first);
      expect(
        await system.run(
          system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
      ).toEqual(beforeReplay);
      expect(await system.readModel()).toEqual(snapshotBefore);
      const [stored] = await system.run(
        system.sql<{ readonly branch: string | null; readonly branchType: string | null }>`
          SELECT json_extract(payload_json, '$.branch') AS branch,
            json_type(payload_json, '$.branch') AS "branchType"
          FROM main.orchestration_events WHERE command_id=${command.commandId}
        `,
      );
      if (variant.expectedBranch === undefined) {
        expect(stored).toEqual({ branch: null, branchType: null });
      } else {
        expect(stored?.branch).toBe(variant.expectedBranch);
        expect(stored?.branchType).toBe(variant.expectedBranch === null ? "null" : "text");
      }
      if (variant.name === "mismatching-expected-branch") mismatchCommand = command;
    }

    expect(mismatchCommand).toBeDefined();
    await system.run(
      system.sql`UPDATE main.orchestration_events
        SET payload_json=json_set(payload_json, '$.branch', 'attacker-third-branch')
        WHERE command_id=${mismatchCommand!.commandId}`,
    );
    const beforeConflict = await system.run(
      system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
    );
    const conflicted = await system.run(Effect.exit(system.engine.dispatch(mismatchCommand!)));
    expect(conflicted._tag).toBe("Failure");
    if (conflicted._tag === "Failure") {
      const error = Cause.findErrorOption(conflicted.cause);
      expect(Option.isSome(error) ? (error.value as { readonly _tag?: string })._tag : "").toBe(
        "OrchestrationCommandIdentityConflictError",
      );
    }
    expect(
      await system.run(system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`),
    ).toEqual(beforeConflict);
    await system.dispose();
  }, 60_000);

  it("replays exact thread.meta.update decider evidence after a fresh WAL restart", async () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-meta-replay-wal-"));
    const filename = NodePath.join(directory, "state.sqlite");
    try {
      const firstSystem = await createOrchestrationSystem(makeSqlitePersistenceLive(filename));
      const projectId = asProjectId("project-meta-replay-wal");
      const threadId = ThreadId.make("thread-meta-replay-wal");
      await firstSystem.run(
        firstSystem.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("command-meta-replay-wal-project"),
          projectId,
          title: "Meta WAL",
          workspaceRoot: "/tmp/project-meta-replay-wal",
          createdAt: now(),
        }),
      );
      await firstSystem.run(
        firstSystem.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("command-meta-replay-wal-thread"),
          threadId,
          projectId,
          title: "Meta WAL",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: "main",
          worktreePath: null,
          createdAt: now(),
        }),
      );
      const command = {
        type: "thread.meta.update" as const,
        commandId: CommandId.make("command-meta-replay-wal-update"),
        threadId,
        branch: "requested",
        expectedBranch: "stale",
        title: "Meta WAL updated",
      };
      const first = await firstSystem.run(firstSystem.engine.dispatch(command));
      await firstSystem.dispose();

      const restarted = await createOrchestrationSystem(makeSqlitePersistenceLive(filename));
      expect(await restarted.run(restarted.sql`PRAGMA journal_mode`)).toEqual([
        { journal_mode: "wal" },
      ]);
      const before = await restarted.run(
        restarted.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      expect(await restarted.run(restarted.engine.dispatch(command))).toEqual(first);
      expect(
        await restarted.run(
          restarted.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
      ).toEqual(before);
      await restarted.dispose();
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  }, 60_000);

  it("binds every persisted thread.meta.update replay coordinate independently", async () => {
    const mutations = [
      {
        name: "payload-thread-id",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET payload_json=json_set(payload_json, '$.threadId', 'thread-attacker')
            WHERE command_id=${commandId}`,
      },
      {
        name: "payload-title",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET payload_json=json_set(payload_json, '$.title', 'Attacker title')
            WHERE command_id=${commandId}`,
      },
      {
        name: "payload-model-selection",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET payload_json=json_set(
              payload_json, '$.modelSelection',
              json('{"instanceId":"codex","model":"attacker-model"}')
            ) WHERE command_id=${commandId}`,
      },
      {
        name: "payload-branch",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET payload_json=json_set(payload_json, '$.branch', 'attacker-third-branch')
            WHERE command_id=${commandId}`,
      },
      {
        name: "payload-worktree-path",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET payload_json=json_set(payload_json, '$.worktreePath', '/tmp/attacker')
            WHERE command_id=${commandId}`,
      },
      {
        name: "payload-updated-at",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET payload_json=json_set(payload_json, '$.updatedAt', '2026-01-01T00:00:01.000Z')
            WHERE command_id=${commandId}`,
      },
      {
        name: "event-type",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET event_type='thread.message-sent'
            WHERE command_id=${commandId}`,
      },
      {
        name: "metadata",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET metadata_json='{"adapterKey":"codex"}'
            WHERE command_id=${commandId}`,
      },
      {
        name: "stream",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET stream_id='thread-meta-attacker'
            WHERE command_id=${commandId}`,
      },
      {
        name: "stream-version",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET stream_version=stream_version + 1
            WHERE command_id=${commandId}`,
      },
      {
        name: "sequence",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          Effect.gen(function* () {
            yield* sql`PRAGMA foreign_keys=OFF`;
            yield* sql`UPDATE main.orchestration_events SET sequence=sequence + 100000
              WHERE command_id=${commandId}`;
            yield* sql`PRAGMA foreign_keys=ON`;
          }),
      },
      {
        name: "actor",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET actor_kind='server'
            WHERE command_id=${commandId}`,
      },
      {
        name: "causation",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET causation_event_id=(
              SELECT event_id FROM main.orchestration_events ORDER BY sequence LIMIT 1
            ) WHERE command_id=${commandId}`,
      },
      {
        name: "correlation",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events SET correlation_id='command-meta-attacker'
            WHERE command_id=${commandId}`,
      },
      {
        name: "occurred-at",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_events
            SET occurred_at='2026-01-01T00:00:01.000Z',
              payload_json=json_set(payload_json, '$.updatedAt', '2026-01-01T00:00:01.000Z')
            WHERE command_id=${commandId}`,
      },
      {
        name: "receipt-result-sequence",
        apply: (sql: SqlClient.SqlClient, commandId: CommandId) =>
          sql`UPDATE main.orchestration_command_receipts SET result_sequence=(
              SELECT min(sequence) FROM main.orchestration_events
            ) WHERE command_id=${commandId}`,
      },
    ] as const;

    for (const mutation of mutations) {
      const system = await createOrchestrationSystem();
      const projectId = asProjectId(`project-meta-coordinate-${mutation.name}`);
      const threadId = ThreadId.make(`thread-meta-coordinate-${mutation.name}`);
      await system.run(
        system.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make(`command-meta-coordinate-project-${mutation.name}`),
          projectId,
          title: "Meta coordinate",
          workspaceRoot: `/tmp/project-meta-coordinate-${mutation.name}`,
          createdAt: now(),
        }),
      );
      await system.run(
        system.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`command-meta-coordinate-thread-${mutation.name}`),
          threadId,
          projectId,
          title: "Meta coordinate",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: "main",
          worktreePath: null,
          createdAt: now(),
        }),
      );
      const command = {
        type: "thread.meta.update" as const,
        commandId: CommandId.make(`command-meta-coordinate-update-${mutation.name}`),
        threadId,
        title: "Expected title",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.1-codex",
        },
        branch: "requested",
        expectedBranch: "stale",
        worktreePath: "/tmp/expected-worktree",
      };
      await system.run(system.engine.dispatch(command));
      await system.run(mutation.apply(system.sql, command.commandId));
      const changesBefore = await system.run(
        system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      const snapshotBefore = await system.readModel();
      const replay = await system.run(Effect.exit(system.engine.dispatch(command)));
      expect(replay._tag, mutation.name).toBe("Failure");
      if (replay._tag === "Failure") {
        const error = Cause.findErrorOption(replay.cause);
        const tag = Option.isSome(error)
          ? (error.value as { readonly _tag?: string })._tag
          : undefined;
        expect(
          tag === "OrchestrationCommandIdentityConflictError" || tag === "PersistenceDecodeError",
          mutation.name,
        ).toBe(true);
      }
      expect(
        await system.run(
          system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
        mutation.name,
      ).toEqual(changesBefore);
      expect(await system.readModel(), mutation.name).toEqual(snapshotBefore);
      await system.dispose();
    }
  }, 120_000);

  it("accepted-receipt replays exact historical five-field runtimeEventType metadata", async () => {
    const system = await createOrchestrationSystem();
    const projectId = asProjectId("project-historical-five-field-replay");
    const threadId = ThreadId.make("thread-historical-five-field-replay");
    await system.run(
      system.engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("command-historical-five-field-project"),
        projectId,
        title: "Historical five-field replay",
        workspaceRoot: "/tmp/project-historical-five-field-replay",
        createdAt: now(),
      }),
    );
    await system.run(
      system.engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command-historical-five-field-thread"),
        threadId,
        projectId,
        title: "Historical five-field replay",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt: now(),
      }),
    );
    const command = {
      type: "thread.message.assistant.delta" as const,
      commandId: CommandId.make("provider:historical-five-field:assistant-delta"),
      threadId,
      messageId: MessageId.make("assistant:historical-five-field"),
      delta: "historical bytes",
      turnId: TurnId.make("turn-historical-five-field"),
      providerRuntimeMessage: {
        runtimeEventId: EventId.make("event-historical-five-field"),
        eventType: "content.delta" as const,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerTurnId: TurnId.make("turn-historical-five-field"),
        providerItemId: RuntimeItemId.make("item-historical-five-field"),
      },
      createdAt: now(),
    };
    const first = await system.run(system.engine.dispatch(command));
    const historicalMetadata =
      '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-five-field","runtimeEventType":"content.delta","providerInstanceId":"codex","providerTurnId":"turn-historical-five-field","providerItemId":"item-historical-five-field"}}';
    await system.run(
      system.sql`UPDATE main.orchestration_events SET metadata_json=${historicalMetadata}
        WHERE command_id=${command.commandId}`,
    );
    const bytesBefore = await system.run(
      system.sql<{ readonly metadataHex: string }>`
        SELECT hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
        FROM main.orchestration_events WHERE command_id=${command.commandId}
      `,
    );
    expect(bytesBefore).toEqual([
      { metadataHex: Buffer.from(historicalMetadata).toString("hex").toUpperCase() },
    ]);
    const changesBefore = await system.run(
      system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
    );
    expect(await system.run(system.engine.dispatch(command))).toEqual(first);
    expect(
      await system.run(system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`),
    ).toEqual(changesBefore);
    expect(
      await system.run(
        system.sql<{ readonly metadataHex: string }>`
          SELECT hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events WHERE command_id=${command.commandId}
        `,
      ),
    ).toEqual(bytesBefore);
    await system.dispose();
  });

  it("streams legitimate project.delete replay chains across and above 1024 events", async () => {
    const system = await createOrchestrationSystem();
    for (const threadCount of [0, 1, 1023, 1024, 1025, 1088]) {
      const seeded = await seedProjectDeleteReplay(system, `size-${threadCount}`, threadCount);
      expect(
        await system.run(
          system.sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM main.orchestration_events
            WHERE command_id=${seeded.command.commandId}
          `,
        ),
      ).toEqual([{ count: threadCount + 1 }]);
      const before = await system.run(
        system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      const snapshotBefore = await system.readModel();
      expect(await system.run(system.engine.dispatch(seeded.command))).toEqual(seeded.result);
      expect(
        await system.run(
          system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
      ).toEqual(before);
      expect(await system.readModel()).toEqual(snapshotBefore);
    }
    await system.dispose();
  }, 180_000);

  it("rejects malformed streamed project.delete chains without partial mutation", async () => {
    const runCorruption = async (
      name: string,
      threadCount: number,
      corrupt: (
        system: Awaited<ReturnType<typeof createOrchestrationSystem>>,
        seeded: Awaited<ReturnType<typeof seedProjectDeleteReplay>>,
      ) => Promise<void>,
    ) => {
      const system = await createOrchestrationSystem();
      const seeded = await seedProjectDeleteReplay(system, name, threadCount);
      await corrupt(system, seeded);
      const before = await system.run(
        system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      const snapshotBefore = await system.readModel();
      const exit = await system.run(Effect.exit(system.engine.dispatch(seeded.command)));
      expect(exit._tag, name).toBe("Failure");
      expect(
        await system.run(
          system.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
        name,
      ).toEqual(before);
      expect(await system.readModel(), name).toEqual(snapshotBefore);
      await system.dispose();
    };

    await runCorruption("missing-terminal", 1, async (system, seeded) => {
      await system.run(
        system.sql`DELETE FROM main.orchestration_events
          WHERE command_id=${seeded.command.commandId} AND event_type='project.deleted'`,
      );
    });
    await runCorruption("event-after-terminal", 0, async (system, seeded) => {
      await system.run(
        system.sql`INSERT INTO main.projection_threads (
          thread_id, project_id, title, branch, worktree_path, latest_turn_id,
          created_at, updated_at, deleted_at, runtime_mode, interaction_mode,
          model_selection_json, archived_at, latest_user_message_at,
          pending_approval_count, pending_user_input_count,
          has_actionable_proposed_plan, agent_control_json
        ) VALUES (
          'thread-delete-replay-event-after-terminal-extra', ${seeded.command.projectId},
          'Extra', NULL, NULL, NULL, ${now()}, ${now()}, NULL, 'approval-required', 'default',
          '{"instanceId":"codex","model":"gpt-5-codex"}', NULL, NULL, 0, 0, 0, NULL
        )`,
      );
      await system.run(
        system.sql`INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        ) VALUES (
          'event-delete-replay-event-after-terminal-extra', 'thread',
          'thread-delete-replay-event-after-terminal-extra', 1, 'thread.deleted', ${now()},
          ${seeded.command.commandId}, NULL, ${seeded.command.commandId}, 'client',
          json_object('threadId', 'thread-delete-replay-event-after-terminal-extra',
            'deletedAt', ${now()}), '{}'
        )`,
      );
    });
    await runCorruption("duplicate-thread", 2, async (system, seeded) => {
      await system.run(system.sql`DROP INDEX main.idx_orch_events_stream_version`);
      await system.run(
        system.sql`UPDATE main.orchestration_events
          SET stream_id='thread-delete-replay-duplicate-thread-000001',
            payload_json=json_object(
              'threadId', 'thread-delete-replay-duplicate-thread-000001',
              'deletedAt', occurred_at
            )
          WHERE command_id=${seeded.command.commandId}
            AND stream_id='thread-delete-replay-duplicate-thread-000002'`,
      );
    });
    await runCorruption("foreign-project", 1, async (system) => {
      await system.run(
        system.sql`UPDATE main.projection_threads SET project_id='foreign-project'
          WHERE thread_id='thread-delete-replay-foreign-project-000001'`,
      );
    });
    await runCorruption("blob-sibling", 0, async (system, seeded) => {
      await system.run(
        system.sql`INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        ) VALUES (
          'event-delete-replay-blob-sibling-extra', 'project', ${seeded.command.projectId}, 3,
          'project.deleted', ${now()}, ${seeded.command.commandId}, NULL,
          ${seeded.command.commandId}, 'client',
          json_object('projectId', ${seeded.command.projectId}, 'deletedAt', ${now()}), '{}'
        )`,
      );
      await system.run(
        system.sql`DROP TRIGGER main.agent_control_orchestration_event_update_storage_validate`,
      );
      await system.run(
        system.sql`UPDATE main.orchestration_events SET command_id=CAST(command_id AS BLOB)
          WHERE event_id='event-delete-replay-blob-sibling-extra'`,
      );
    });
  }, 120_000);

  it("replays a 1025-thread project.delete chain through a fresh WAL connection", async () => {
    const directory = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-project-delete-replay-wal-"),
    );
    const filename = NodePath.join(directory, "state.sqlite");
    try {
      const firstSystem = await createOrchestrationSystem(makeSqlitePersistenceLive(filename));
      const seeded = await seedProjectDeleteReplay(firstSystem, "wal-1025", 1025);
      await firstSystem.dispose();
      const restarted = await createOrchestrationSystem(makeSqlitePersistenceLive(filename));
      expect(await restarted.run(restarted.sql`PRAGMA journal_mode`)).toEqual([
        { journal_mode: "wal" },
      ]);
      const before = await restarted.run(
        restarted.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
      );
      expect(await restarted.run(restarted.engine.dispatch(seeded.command))).toEqual(seeded.result);
      expect(
        await restarted.run(
          restarted.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
        ),
      ).toEqual(before);
      await restarted.dispose();
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  }, 120_000);

  it("bootstraps command handling from persisted projections without reading the full snapshot", async () => {
    let nextSequence = 8;
    const eventStore: OrchestrationEventStoreShape = {
      append: (event) =>
        Effect.sync(() => {
          const savedEvent = {
            ...event,
            sequence: nextSequence,
          } as OrchestrationEvent;
          nextSequence += 1;
          return savedEvent;
        }),
      appendAgentControlThreadMaterialization: (event) =>
        Effect.sync(() => {
          const savedEvent = {
            ...event,
            sequence: nextSequence,
          } as OrchestrationEvent;
          nextSequence += 1;
          return savedEvent;
        }),
      readFromSequence: () => Stream.empty,
      readAll: () =>
        Stream.fail(
          new PersistenceSqlError({
            operation: "test.readAll",
            detail: "historical replay should not be used during bootstrap",
          }),
        ),
    };

    const projectionSnapshot = {
      snapshotSequence: 7,
      updatedAt: "2026-03-03T00:00:04.000Z",
      projects: [
        {
          id: asProjectId("project-bootstrap"),
          title: "Bootstrap Project",
          workspaceRoot: "/tmp/project-bootstrap",
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          scripts: [],
          createdAt: "2026-03-03T00:00:00.000Z",
          updatedAt: "2026-03-03T00:00:01.000Z",
          deletedAt: null,
        },
      ],
      threads: [
        {
          id: ThreadId.make("thread-bootstrap"),
          projectId: asProjectId("project-bootstrap"),
          title: "Bootstrap Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access" as const,
          branch: null,
          worktreePath: null,
          latestTurn: null,
          createdAt: "2026-03-03T00:00:02.000Z",
          updatedAt: "2026-03-03T00:00:03.000Z",
          archivedAt: null,
          deletedAt: null,
          messages: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          session: null,
        },
      ],
    };
    const commandReadModel = {
      ...projectionSnapshot,
      threads: projectionSnapshot.threads.map((thread) => ({
        ...thread,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
      })),
    };
    let fullSnapshotReadCount = 0;

    const layer = OrchestrationEngineLive.pipe(
      Layer.provide(
        Layer.succeed(ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.succeed(commandReadModel),
          getSnapshot: () =>
            Effect.sync(() => {
              fullSnapshotReadCount += 1;
              return projectionSnapshot;
            }),
          getShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: projectionSnapshot.snapshotSequence,
              projects: [],
              threads: [],
              updatedAt: projectionSnapshot.updatedAt,
            }),
          getArchivedShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: projectionSnapshot.snapshotSequence,
              projects: [],
              threads: [],
              updatedAt: projectionSnapshot.updatedAt,
            }),
          getSnapshotSequence: () =>
            Effect.succeed({ snapshotSequence: projectionSnapshot.snapshotSequence }),
          getCounts: () => Effect.succeed({ projectCount: 1, threadCount: 1 }),
          getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
          getProjectShellById: () => Effect.succeed(Option.none()),
          getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
          getThreadCheckpointContext: () => Effect.succeed(Option.none()),
          getFullThreadDiffContext: () => Effect.succeed(Option.none()),
          getThreadShellById: () => Effect.succeed(Option.none()),
          getThreadDetailById: () => Effect.succeed(Option.none()),
          getThreadDetailSnapshot: () => Effect.succeed(Option.none()),
        }),
      ),
      Layer.provide(
        Layer.succeed(OrchestrationProjectionPipeline, {
          bootstrap: Effect.void,
          projectEvent: () => Effect.void,
        } satisfies OrchestrationProjectionPipelineShape),
      ),
      Layer.provide(Layer.succeed(OrchestrationEventStore, eventStore)),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    );

    const runtime = ManagedRuntime.make(layer);

    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    expect(await runtime.runPromise(engine.latestSequence)).toBe(7);
    const result = await runtime.runPromise(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-bootstrap-thread-update"),
        threadId: ThreadId.make("thread-bootstrap"),
        title: "Updated Bootstrap Thread",
      }),
    );

    expect(result.sequence).toBe(8);
    expect(await runtime.runPromise(engine.latestSequence)).toBe(8);
    expect(fullSnapshotReadCount).toBe(0);

    await runtime.dispose();
  });

  it("persists deterministic read models for repeated snapshot reads", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-1-create"),
        projectId: asProjectId("project-1"),
        title: "Project 1",
        workspaceRoot: "/tmp/project-1",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-1-create"),
        threadId: ThreadId.make("thread-1"),
        projectId: asProjectId("project-1"),
        title: "Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("msg-1"),
          role: "user",
          text: "hello",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt,
      }),
    );

    const readModelA = await system.readModel();
    const readModelB = await system.readModel();
    expect(readModelB).toEqual(readModelA);
    await system.dispose();
  });

  it("archives and unarchives threads through orchestration commands", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-archive-create"),
        projectId: asProjectId("project-archive"),
        title: "Project Archive",
        workspaceRoot: "/tmp/project-archive",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-archive-create"),
        threadId: ThreadId.make("thread-archive"),
        projectId: asProjectId("project-archive"),
        title: "Archive me",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("cmd-thread-archive"),
        threadId: ThreadId.make("thread-archive"),
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.archivedAt,
    ).not.toBeNull();

    await system.run(
      engine.dispatch({
        type: "thread.unarchive",
        commandId: CommandId.make("cmd-thread-unarchive"),
        threadId: ThreadId.make("thread-archive"),
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.archivedAt,
    ).toBeNull();

    await system.dispose();
  });

  it("replays append-only events from sequence", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-replay-create"),
        projectId: asProjectId("project-replay"),
        title: "Replay Project",
        workspaceRoot: "/tmp/project-replay",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-replay-create"),
        threadId: ThreadId.make("thread-replay"),
        projectId: asProjectId("project-replay"),
        title: "replay",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("cmd-thread-replay-delete"),
        threadId: ThreadId.make("thread-replay"),
      }),
    );

    const events = await system.run(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(events.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
      "thread.deleted",
    ]);
    await system.dispose();
  });

  it("streams persisted domain events in order", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-stream-create"),
        projectId: asProjectId("project-stream"),
        title: "Stream Project",
        workspaceRoot: "/tmp/project-stream",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    const eventTypes: string[] = [];
    await system.run(
      Effect.gen(function* () {
        const eventQueue = yield* Queue.unbounded<OrchestrationEvent>();
        yield* Effect.forkScoped(
          Stream.take(engine.streamDomainEvents, 2).pipe(
            Stream.runForEach((event) => Queue.offer(eventQueue, event).pipe(Effect.asVoid)),
          ),
        );
        yield* Effect.sleep("10 millis");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-stream-thread-create"),
          threadId: ThreadId.make("thread-stream"),
          projectId: asProjectId("project-stream"),
          title: "domain-stream",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        });
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-stream-thread-update"),
          threadId: ThreadId.make("thread-stream"),
          title: "domain-stream-updated",
        });
        eventTypes.push((yield* Queue.take(eventQueue)).type);
        eventTypes.push((yield* Queue.take(eventQueue)).type);
      }).pipe(Effect.scoped),
    );

    expect(eventTypes).toEqual(["thread.created", "thread.meta-updated"]);
    await system.dispose();
  });

  it("does not regress a generated branch to a stale temporary worktree branch", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-branch-race-project-create"),
        projectId: asProjectId("project-branch-race"),
        title: "Branch Race Project",
        workspaceRoot: "/tmp/project-branch-race",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-branch-race-thread-create"),
        threadId: ThreadId.make("thread-branch-race"),
        projectId: asProjectId("project-branch-race"),
        title: "Branch Race Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "t3code/generated-branch-name",
        worktreePath: "/tmp/project-branch-race-worktree",
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-stale-temporary-branch-sync"),
        threadId: ThreadId.make("thread-branch-race"),
        branch: "t3code/1234abcd",
        expectedBranch: "t3code/1234abcd",
      }),
    );

    const snapshot = await system.readModel();
    expect(snapshot.threads[0]?.branch).toBe("t3code/generated-branch-name");
    await system.dispose();
  });

  it("allows authoritative worktree bootstrap to assign a temporary branch", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-worktree-bootstrap-project-create"),
        projectId: asProjectId("project-worktree-bootstrap"),
        title: "Worktree Bootstrap Project",
        workspaceRoot: "/tmp/project-worktree-bootstrap",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-worktree-bootstrap-thread-create"),
        threadId: ThreadId.make("thread-worktree-bootstrap"),
        projectId: asProjectId("project-worktree-bootstrap"),
        title: "Worktree Bootstrap Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "main",
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-authoritative-worktree-bootstrap"),
        threadId: ThreadId.make("thread-worktree-bootstrap"),
        branch: "t3code/1234abcd",
        worktreePath: "/tmp/project-worktree-bootstrap-worktree",
      }),
    );

    const snapshot = await system.readModel();
    expect(snapshot.threads[0]?.branch).toBe("t3code/1234abcd");
    expect(snapshot.threads[0]?.worktreePath).toBe("/tmp/project-worktree-bootstrap-worktree");
    await system.dispose();
  });

  it("records command ack duration using the first committed event type", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-ack-create"),
        projectId: asProjectId("project-ack"),
        title: "Ack Project",
        workspaceRoot: "/tmp/project-ack",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-ack-create"),
        threadId: ThreadId.make("thread-ack"),
        projectId: asProjectId("project-ack"),
        title: "Ack Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const snapshots = await system.run(Metric.snapshot);
    expect(
      hasMetricSnapshot(snapshots, "t3_orchestration_command_ack_duration", {
        commandType: "thread.create",
        aggregateKind: "thread",
        ackEventType: "thread.created",
      }),
    ).toBe(true);

    await system.dispose();
  });

  it("records failed command dispatches as metric failures", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-missing-project"),
          threadId: ThreadId.make("thread-missing-project"),
          projectId: asProjectId("project-missing"),
          title: "Missing Project Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("does not exist");

    const snapshots = await system.run(Metric.snapshot);
    expect(
      hasMetricSnapshot(snapshots, "t3_orchestration_commands_total", {
        commandType: "thread.create",
        aggregateKind: "thread",
        outcome: "failure",
      }),
    ).toBe(true);

    await system.dispose();
  });

  it("stores completed checkpoint summaries even when no files changed", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-turn-diff-create"),
        projectId: asProjectId("project-turn-diff"),
        title: "Turn Diff Project",
        workspaceRoot: "/tmp/project-turn-diff",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-turn-diff-create"),
        threadId: ThreadId.make("thread-turn-diff"),
        projectId: asProjectId("project-turn-diff"),
        title: "Turn diff thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-turn-diff-complete"),
        threadId: ThreadId.make("thread-turn-diff"),
        turnId: asTurnId("turn-1"),
        completedAt: createdAt,
        checkpointRef: asCheckpointRef("refs/t3/checkpoints/thread-turn-diff/turn/1"),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );

    const thread = (await system.readModel()).threads.find(
      (entry) => entry.id === "thread-turn-diff",
    );
    expect(thread?.checkpoints).toEqual([
      {
        turnId: asTurnId("turn-1"),
        checkpointTurnCount: 1,
        checkpointRef: asCheckpointRef("refs/t3/checkpoints/thread-turn-diff/turn/1"),
        status: "ready",
        files: [],
        assistantMessageId: null,
        completedAt: createdAt,
      },
    ]);
    await system.dispose();
  });

  it("keeps processing queued commands after a storage failure", async () => {
    type StoredEvent =
      ReturnType<OrchestrationEventStoreShape["append"]> extends Effect.Effect<infer A, any, any>
        ? A
        : never;
    const events: StoredEvent[] = [];
    let nextSequence = 1;
    let shouldFailFirstAppend = true;

    const flakyStore: OrchestrationEventStoreShape = {
      append(event) {
        if (shouldFailFirstAppend && event.commandId === CommandId.make("cmd-flaky-1")) {
          shouldFailFirstAppend = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.append",
              detail: "append failed",
            }),
          );
        }
        const savedEvent = {
          ...event,
          sequence: nextSequence,
        } as StoredEvent;
        nextSequence += 1;
        events.push(savedEvent);
        return Effect.succeed(savedEvent);
      },
      appendAgentControlThreadMaterialization(event) {
        return flakyStore.append(event);
      },
      readFromSequence(sequenceExclusive) {
        return Stream.fromIterable(events.filter((event) => event.sequence > sequenceExclusive));
      },
      readAll() {
        return Stream.fromIterable(events);
      },
    };

    const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
      prefix: "t3-orchestration-engine-test-",
    });

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(OrchestrationProjectionPipelineLive),
        Layer.provide(Layer.succeed(OrchestrationEventStore, flakyStore)),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provideMerge(ServerConfigLayer),
        Layer.provideMerge(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-flaky-create"),
        projectId: asProjectId("project-flaky"),
        title: "Flaky Project",
        workspaceRoot: "/tmp/project-flaky",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-flaky-1"),
          threadId: ThreadId.make("thread-flaky-fail"),
          projectId: asProjectId("project-flaky"),
          title: "flaky-fail",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("append failed");

    const result = await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-flaky-2"),
        threadId: ThreadId.make("thread-flaky-ok"),
        projectId: asProjectId("project-flaky"),
        title: "flaky-ok",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    expect(result.sequence).toBe(2);
    const eventsAfterRetry = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterRetry.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
    ]);
    await runtime.dispose();
  });

  it("rolls back all events for a multi-event command when projection fails mid-dispatch", async () => {
    let shouldFailRequestedProjection = true;
    const flakyProjectionPipeline: OrchestrationProjectionPipelineShape = {
      bootstrap: Effect.void,
      projectEvent: (event) => {
        if (
          shouldFailRequestedProjection &&
          event.commandId === CommandId.make("cmd-turn-start-atomic") &&
          event.type === "thread.turn-start-requested"
        ) {
          shouldFailRequestedProjection = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.projection",
              detail: "projection failed",
            }),
          );
        }
        return Effect.void;
      },
    };

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(Layer.succeed(OrchestrationProjectionPipeline, flakyProjectionPipeline)),
        Layer.provide(OrchestrationEventStoreLive),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provide(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-atomic-create"),
        projectId: asProjectId("project-atomic"),
        title: "Atomic Project",
        workspaceRoot: "/tmp/project-atomic",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-atomic-create"),
        threadId: ThreadId.make("thread-atomic"),
        projectId: asProjectId("project-atomic"),
        title: "atomic",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const turnStartCommand = {
      type: "thread.turn.start" as const,
      commandId: CommandId.make("cmd-turn-start-atomic"),
      threadId: ThreadId.make("thread-atomic"),
      message: {
        messageId: asMessageId("msg-atomic-1"),
        role: "user" as const,
        text: "hello",
        attachments: [],
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required" as const,
      createdAt,
    };

    await expect(runtime.runPromise(engine.dispatch(turnStartCommand))).rejects.toThrow(
      "projection failed",
    );

    const eventsAfterFailure = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterFailure.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
    ]);

    const retryResult = await runtime.runPromise(engine.dispatch(turnStartCommand));
    expect(retryResult.sequence).toBe(4);

    const eventsAfterRetry = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterRetry.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
      "thread.message-sent",
      "thread.turn-start-requested",
    ]);
    expect(
      eventsAfterRetry.filter((event) => event.commandId === turnStartCommand.commandId),
    ).toHaveLength(2);

    await runtime.dispose();
  });

  it("reconciles command state when append persists but projection fails", async () => {
    type StoredEvent =
      ReturnType<OrchestrationEventStoreShape["append"]> extends Effect.Effect<infer A, any, any>
        ? A
        : never;
    const events: StoredEvent[] = [];
    let nextSequence = 1;

    const nonTransactionalStore: OrchestrationEventStoreShape = {
      append(event) {
        const savedEvent = {
          ...event,
          sequence: nextSequence,
        } as StoredEvent;
        nextSequence += 1;
        events.push(savedEvent);
        return Effect.succeed(savedEvent);
      },
      appendAgentControlThreadMaterialization(event) {
        return nonTransactionalStore.append(event);
      },
      readFromSequence(sequenceExclusive) {
        return Stream.fromIterable(events.filter((event) => event.sequence > sequenceExclusive));
      },
      readAll() {
        return Stream.fromIterable(events);
      },
    };

    let shouldFailProjection = true;
    const flakyProjectionPipeline: OrchestrationProjectionPipelineShape = {
      bootstrap: Effect.void,
      projectEvent: (event) => {
        if (
          shouldFailProjection &&
          event.commandId === CommandId.make("cmd-thread-archive-sync-fail")
        ) {
          shouldFailProjection = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.projection",
              detail: "projection failed",
            }),
          );
        }
        return Effect.void;
      },
    };

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(Layer.succeed(OrchestrationProjectionPipeline, flakyProjectionPipeline)),
        Layer.provide(Layer.succeed(OrchestrationEventStore, nonTransactionalStore)),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provide(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-sync-create"),
        projectId: asProjectId("project-sync"),
        title: "Sync Project",
        workspaceRoot: "/tmp/project-sync",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-sync-create"),
        threadId: ThreadId.make("thread-sync"),
        projectId: asProjectId("project-sync"),
        title: "sync-before",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.archive",
          commandId: CommandId.make("cmd-thread-archive-sync-fail"),
          threadId: ThreadId.make("thread-sync"),
        }),
      ),
    ).rejects.toThrow("projection failed");

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.archive",
          commandId: CommandId.make("cmd-thread-archive-sync-retry"),
          threadId: ThreadId.make("thread-sync"),
        }),
      ),
    ).rejects.toThrow("already archived");

    await runtime.dispose();
  });

  it("fails command dispatch when command invariants are violated", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-invariant-missing-thread"),
          threadId: ThreadId.make("thread-missing"),
          message: {
            messageId: asMessageId("msg-missing"),
            role: "user",
            text: "hello",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now(),
        }),
      ),
    ).rejects.toThrow("Thread 'thread-missing' does not exist");

    await system.dispose();
  });

  it("rejects duplicate thread creation", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-duplicate-create"),
        projectId: asProjectId("project-duplicate"),
        title: "Duplicate Project",
        workspaceRoot: "/tmp/project-duplicate",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-duplicate-1"),
        threadId: ThreadId.make("thread-duplicate"),
        projectId: asProjectId("project-duplicate"),
        title: "duplicate",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-duplicate-2"),
          threadId: ThreadId.make("thread-duplicate"),
          projectId: asProjectId("project-duplicate"),
          title: "duplicate",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("already exists");

    await system.dispose();
  });
});
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
