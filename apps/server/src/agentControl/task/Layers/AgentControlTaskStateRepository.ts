import {
  AgentControlTaskId,
  AgentControlTaskPipelineStage,
  AgentControlTaskSourceGate,
  AgentControlTaskState,
  AgentControlTaskStatus,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  IsoDateTime,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../Errors.ts";
import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import {
  AgentControlTaskStateRepository,
  type AgentControlTaskEnumerationEntry,
  type AgentControlTaskStateRepositoryShape,
} from "../Services/AgentControlTaskStateRepository.ts";

const StateCoordinates = Schema.Struct({
  taskId: AgentControlTaskId,
  projectId: ProjectId,
  revision: NonNegativeInt,
  sequence: NonNegativeInt,
  repositoryNodeId: Schema.String,
  issueNodeId: Schema.String,
  issueNumber: PositiveInt,
  issueUrl: Schema.String,
  status: AgentControlTaskStatus,
  sourceGate: AgentControlTaskSourceGate,
  stage: AgentControlTaskPipelineStage,
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
const ProjectionStorageClasses = Schema.Struct({
  stateStorageClass: Schema.Literal("text"),
  stateBytes: Schema.Unknown,
  taskIdStorageClass: Schema.Literal("text"),
  projectIdStorageClass: Schema.Literal("text"),
  revisionStorageClass: Schema.Literal("integer"),
  sequenceStorageClass: Schema.Literal("integer"),
  repositoryNodeIdStorageClass: Schema.Literal("text"),
  issueNodeIdStorageClass: Schema.Literal("text"),
  issueNumberStorageClass: Schema.Literal("integer"),
  issueUrlStorageClass: Schema.Literal("text"),
  statusStorageClass: Schema.Literal("text"),
  sourceGateStorageClass: Schema.Literal("text"),
  stageStorageClass: Schema.Literal("text"),
  sourceUpdatedAtStorageClass: Schema.Literal("text"),
  githubIntakeSequenceStorageClass: Schema.Literal("integer"),
  createdAtStorageClass: Schema.Literal("text"),
  updatedAtStorageClass: Schema.Literal("text"),
});
const decodeStateCoordinates = Schema.decodeUnknownEffect(StateCoordinates);
const decodeProjectionStorageClasses = Schema.decodeUnknownEffect(ProjectionStorageClasses);
const decodeStoredState = Schema.decodeUnknownEffect(
  AgentControlTaskState.annotate({ parseOptions: { onExcessProperty: "error" } }),
);
const decodeState = Schema.decodeUnknownEffect(AgentControlTaskState);
const encodeState = Schema.encodeUnknownEffect(Schema.fromJsonString(AgentControlTaskState));
const decodeTaskId = Schema.decodeUnknownEffect(AgentControlTaskId);
const decodeProjectId = Schema.decodeUnknownEffect(ProjectId);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

export const decodeAgentControlTaskProjectionRow = (
  row: Record<string, unknown>,
  operation: string,
): Effect.Effect<AgentControlTaskState, AgentControlPersistenceDecodeError> =>
  Effect.gen(function* () {
    let source: string;
    if ("stateStorageClass" in row) {
      const storage = yield* decodeProjectionStorageClasses(row).pipe(
        Effect.mapError((cause) => decodeError(operation, cause)),
      );
      source = yield* Effect.try({
        try: () => decodeCanonicalUtf8Bytes(storage.stateBytes),
        catch: (cause) => decodeError(operation, cause),
      });
    } else {
      source = yield* Effect.try({
        try: () => {
          if (typeof row.state !== "string") {
            throw new Error("task projection state must retain SQLite TEXT authority");
          }
          return row.state;
        },
        catch: (cause) => decodeError(operation, cause),
      });
    }
    const parsed = yield* Effect.try({
      try: () => parseJsonStrict(source),
      catch: (cause) => decodeError(operation, cause),
    });
    const state = yield* decodeStoredState(parsed).pipe(
      Effect.mapError((cause) => decodeError(operation, cause)),
    );
    if (canonicalJson(state as unknown as JsonValue) !== canonicalJson(parsed as JsonValue)) {
      return yield* decodeError(
        operation,
        new Error("task projection state contains non-schema authority"),
      );
    }
    const coordinates = yield* decodeStateCoordinates(row).pipe(
      Effect.mapError((cause) => decodeError(operation, cause)),
    );
    const {
      taskId,
      projectId,
      revision,
      sequence,
      repositoryNodeId,
      issueNodeId,
      issueNumber,
      issueUrl,
      status,
      sourceGate,
      stage,
      sourceUpdatedAt,
      githubIntakeSequence,
      createdAt,
      updatedAt,
    } = coordinates;
    if (
      state.taskId !== taskId ||
      state.source.projectId !== projectId ||
      state.revision !== revision ||
      state.sequence !== sequence ||
      state.source.repositoryNodeId !== repositoryNodeId ||
      state.source.issueNodeId !== issueNodeId ||
      state.source.issueNumber !== issueNumber ||
      state.source.issueUrl !== issueUrl ||
      state.status !== status ||
      state.sourceGate !== sourceGate ||
      state.stage !== stage ||
      state.sourceUpdatedAt !== sourceUpdatedAt ||
      state.githubIntakeSequence !== githubIntakeSequence ||
      state.createdAt !== createdAt ||
      state.updatedAt !== updatedAt
    ) {
      return yield* decodeError(operation, new Error("task projection identity mismatch"));
    }
    return state;
  });

const makeRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlTaskStateRepositoryShape["get"] = (taskId) =>
    sql<Record<string, unknown>>`
      SELECT typeof(state_json) AS "stateStorageClass",
             CAST(state_json AS BLOB) AS "stateBytes",
             typeof(task_id) AS "taskIdStorageClass",
             typeof(project_id) AS "projectIdStorageClass",
             typeof(revision) AS "revisionStorageClass",
             typeof(last_event_sequence) AS "sequenceStorageClass",
             typeof(repository_node_id) AS "repositoryNodeIdStorageClass",
             typeof(issue_node_id) AS "issueNodeIdStorageClass",
             typeof(issue_number) AS "issueNumberStorageClass",
             typeof(issue_url) AS "issueUrlStorageClass",
             typeof(status) AS "statusStorageClass",
             typeof(source_gate) AS "sourceGateStorageClass",
             typeof(stage) AS "stageStorageClass",
             typeof(source_updated_at) AS "sourceUpdatedAtStorageClass",
             typeof(github_intake_sequence) AS "githubIntakeSequenceStorageClass",
             typeof(created_at) AS "createdAtStorageClass",
             typeof(updated_at) AS "updatedAtStorageClass",
             task_id AS "taskId", project_id AS "projectId",
             revision, last_event_sequence AS sequence,
             repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
             issue_number AS "issueNumber", issue_url AS "issueUrl", status,
             source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
             github_intake_sequence AS "githubIntakeSequence",
             created_at AS "createdAt", updated_at AS "updatedAt"
      FROM main.agent_control_task_states
      WHERE task_id = ${taskId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskStateRepository.get", cause)),
      Effect.flatMap((rows) => {
        const row = rows[0];
        return row === undefined
          ? Effect.succeed(Option.none())
          : decodeAgentControlTaskProjectionRow(row, "AgentControlTaskStateRepository.get").pipe(
              Effect.map(Option.some),
            );
      }),
    );

  const save: AgentControlTaskStateRepositoryShape["save"] = (rawState, expectedRevision) =>
    Effect.gen(function* () {
      const state = yield* decodeState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlTaskStateRepository.save:input", cause),
        ),
      );
      if (state.revision !== expectedRevision + 1) {
        return yield* decodeError(
          "AgentControlTaskStateRepository.save:revision",
          new Error("task projection revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlTaskStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expectedRevision === 0
          ? yield* sql<{ readonly taskId: unknown }>`
              INSERT INTO main.agent_control_task_states (
                task_id, project_id, repository_node_id, issue_node_id,
                issue_number, issue_url, status, source_gate, stage,
                source_updated_at, github_intake_sequence, state_json,
                created_at, updated_at, revision, last_event_sequence
              ) VALUES (
                ${state.taskId}, ${state.source.projectId},
                ${state.source.repositoryNodeId}, ${state.source.issueNodeId},
                ${state.source.issueNumber}, ${state.source.issueUrl},
                ${state.status}, ${state.sourceGate}, ${state.stage},
                ${state.sourceUpdatedAt}, ${state.githubIntakeSequence}, ${stateJson},
                ${state.createdAt}, ${state.updatedAt}, ${state.revision}, ${state.sequence}
              )
              ON CONFLICT (task_id) DO NOTHING
              RETURNING task_id AS "taskId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlTaskStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<{ readonly taskId: unknown }>`
              UPDATE main.agent_control_task_states
              SET status = ${state.status},
                  source_gate = ${state.sourceGate},
                  stage = ${state.stage},
                  source_updated_at = ${state.sourceUpdatedAt},
                  github_intake_sequence = ${state.githubIntakeSequence},
                  state_json = ${stateJson},
                  updated_at = ${state.updatedAt},
                  revision = ${state.revision},
                  last_event_sequence = ${state.sequence}
              WHERE task_id = ${state.taskId} AND revision = ${expectedRevision}
              RETURNING task_id AS "taskId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlTaskStateRepository.save:update", cause),
              ),
            );
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlTaskStateRepository.save:conflict",
          new Error("task projection write conflict"),
        );
      }
    });

  const listProject: AgentControlTaskStateRepositoryShape["listProject"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT typeof(state_json) AS "stateStorageClass",
             CAST(state_json AS BLOB) AS "stateBytes",
             typeof(task_id) AS "taskIdStorageClass",
             typeof(project_id) AS "projectIdStorageClass",
             typeof(revision) AS "revisionStorageClass",
             typeof(last_event_sequence) AS "sequenceStorageClass",
             typeof(repository_node_id) AS "repositoryNodeIdStorageClass",
             typeof(issue_node_id) AS "issueNodeIdStorageClass",
             typeof(issue_number) AS "issueNumberStorageClass",
             typeof(issue_url) AS "issueUrlStorageClass",
             typeof(status) AS "statusStorageClass",
             typeof(source_gate) AS "sourceGateStorageClass",
             typeof(stage) AS "stageStorageClass",
             typeof(source_updated_at) AS "sourceUpdatedAtStorageClass",
             typeof(github_intake_sequence) AS "githubIntakeSequenceStorageClass",
             typeof(created_at) AS "createdAtStorageClass",
             typeof(updated_at) AS "updatedAtStorageClass",
             task_id AS "taskId", project_id AS "projectId",
             revision, last_event_sequence AS sequence,
             repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
             issue_number AS "issueNumber", issue_url AS "issueUrl", status,
             source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
             github_intake_sequence AS "githubIntakeSequence",
             created_at AS "createdAt", updated_at AS "updatedAt"
      FROM main.agent_control_task_states
      WHERE project_id = ${projectId}
      ORDER BY issue_number ASC, task_id ASC
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskStateRepository.listProject", cause)),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          Effect.gen(function* () {
            const taskId = Option.getOrNull(yield* Effect.option(decodeTaskId(row.taskId)));
            const rowProjectId = Option.getOrNull(
              yield* Effect.option(decodeProjectId(row.projectId)),
            );
            const decoded = yield* Effect.option(
              decodeAgentControlTaskProjectionRow(
                row,
                "AgentControlTaskStateRepository.listProject",
              ),
            );
            return Option.isSome(decoded)
              ? ({
                  _tag: "Valid",
                  state: decoded.value,
                } satisfies AgentControlTaskEnumerationEntry)
              : ({
                  _tag: "Corrupt",
                  taskId,
                  projectId: rowProjectId,
                } satisfies AgentControlTaskEnumerationEntry);
          }),
        ),
      ),
    );

  const listAll: AgentControlTaskStateRepositoryShape["listAll"] = sql<Record<string, unknown>>`
    SELECT typeof(state_json) AS "stateStorageClass",
           CAST(state_json AS BLOB) AS "stateBytes",
           typeof(task_id) AS "taskIdStorageClass",
           typeof(project_id) AS "projectIdStorageClass",
           typeof(revision) AS "revisionStorageClass",
           typeof(last_event_sequence) AS "sequenceStorageClass",
           typeof(repository_node_id) AS "repositoryNodeIdStorageClass",
           typeof(issue_node_id) AS "issueNodeIdStorageClass",
           typeof(issue_number) AS "issueNumberStorageClass",
           typeof(issue_url) AS "issueUrlStorageClass",
           typeof(status) AS "statusStorageClass",
           typeof(source_gate) AS "sourceGateStorageClass",
           typeof(stage) AS "stageStorageClass",
           typeof(source_updated_at) AS "sourceUpdatedAtStorageClass",
           typeof(github_intake_sequence) AS "githubIntakeSequenceStorageClass",
           typeof(created_at) AS "createdAtStorageClass",
           typeof(updated_at) AS "updatedAtStorageClass",
           task_id AS "taskId", project_id AS "projectId",
           revision, last_event_sequence AS sequence,
           repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
           issue_number AS "issueNumber", issue_url AS "issueUrl", status,
           source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
           github_intake_sequence AS "githubIntakeSequence",
           created_at AS "createdAt", updated_at AS "updatedAt"
    FROM main.agent_control_task_states
    ORDER BY project_id ASC, issue_number ASC, task_id ASC
  `.pipe(
    Effect.mapError((cause) => sqlError("AgentControlTaskStateRepository.listAll", cause)),
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          const taskId = Option.getOrNull(yield* Effect.option(decodeTaskId(row.taskId)));
          const projectId = Option.getOrNull(yield* Effect.option(decodeProjectId(row.projectId)));
          const decoded = yield* Effect.option(
            decodeAgentControlTaskProjectionRow(row, "AgentControlTaskStateRepository.listAll"),
          );
          return Option.isSome(decoded)
            ? ({
                _tag: "Valid",
                state: decoded.value,
              } satisfies AgentControlTaskEnumerationEntry)
            : ({
                _tag: "Corrupt",
                taskId,
                projectId,
              } satisfies AgentControlTaskEnumerationEntry);
        }),
      ),
    ),
  );

  const findByIdentity: AgentControlTaskStateRepositoryShape["findByIdentity"] = (
    projectId,
    repositoryNodeId,
    issueNodeId,
  ) =>
    sql<Record<string, unknown>>`
      SELECT typeof(state_json) AS "stateStorageClass",
             CAST(state_json AS BLOB) AS "stateBytes",
             typeof(task_id) AS "taskIdStorageClass",
             typeof(project_id) AS "projectIdStorageClass",
             typeof(revision) AS "revisionStorageClass",
             typeof(last_event_sequence) AS "sequenceStorageClass",
             typeof(repository_node_id) AS "repositoryNodeIdStorageClass",
             typeof(issue_node_id) AS "issueNodeIdStorageClass",
             typeof(issue_number) AS "issueNumberStorageClass",
             typeof(issue_url) AS "issueUrlStorageClass",
             typeof(status) AS "statusStorageClass",
             typeof(source_gate) AS "sourceGateStorageClass",
             typeof(stage) AS "stageStorageClass",
             typeof(source_updated_at) AS "sourceUpdatedAtStorageClass",
             typeof(github_intake_sequence) AS "githubIntakeSequenceStorageClass",
             typeof(created_at) AS "createdAtStorageClass",
             typeof(updated_at) AS "updatedAtStorageClass",
             task_id AS "taskId", project_id AS "projectId",
             revision, last_event_sequence AS sequence,
             repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
             issue_number AS "issueNumber", issue_url AS "issueUrl", status,
             source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
             github_intake_sequence AS "githubIntakeSequence",
             created_at AS "createdAt", updated_at AS "updatedAt"
      FROM main.agent_control_task_states
      WHERE project_id = ${projectId}
        AND repository_node_id = ${repositoryNodeId}
        AND issue_node_id = ${issueNodeId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskStateRepository.findByIdentity", cause)),
      Effect.flatMap((rows) => {
        if (rows.length > 1) {
          return Effect.fail(
            decodeError(
              "AgentControlTaskStateRepository.findByIdentity:ambiguous",
              new Error("ambiguous task source identity"),
            ),
          );
        }
        const row = rows[0];
        return row === undefined
          ? Effect.succeed(Option.none())
          : decodeAgentControlTaskProjectionRow(
              row,
              "AgentControlTaskStateRepository.findByIdentity",
            ).pipe(Effect.map(Option.some));
      }),
    );

  const findBySourceNumber: AgentControlTaskStateRepositoryShape["findBySourceNumber"] = (
    projectId,
    repositoryNodeId,
    issueNumber,
  ) =>
    sql<Record<string, unknown>>`
      SELECT typeof(state_json) AS "stateStorageClass",
             CAST(state_json AS BLOB) AS "stateBytes",
             typeof(task_id) AS "taskIdStorageClass",
             typeof(project_id) AS "projectIdStorageClass",
             typeof(revision) AS "revisionStorageClass",
             typeof(last_event_sequence) AS "sequenceStorageClass",
             typeof(repository_node_id) AS "repositoryNodeIdStorageClass",
             typeof(issue_node_id) AS "issueNodeIdStorageClass",
             typeof(issue_number) AS "issueNumberStorageClass",
             typeof(issue_url) AS "issueUrlStorageClass",
             typeof(status) AS "statusStorageClass",
             typeof(source_gate) AS "sourceGateStorageClass",
             typeof(stage) AS "stageStorageClass",
             typeof(source_updated_at) AS "sourceUpdatedAtStorageClass",
             typeof(github_intake_sequence) AS "githubIntakeSequenceStorageClass",
             typeof(created_at) AS "createdAtStorageClass",
             typeof(updated_at) AS "updatedAtStorageClass",
             task_id AS "taskId", project_id AS "projectId",
             revision, last_event_sequence AS sequence,
             repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
             issue_number AS "issueNumber", issue_url AS "issueUrl", status,
             source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
             github_intake_sequence AS "githubIntakeSequence",
             created_at AS "createdAt", updated_at AS "updatedAt"
      FROM main.agent_control_task_states
      WHERE project_id = ${projectId}
        AND repository_node_id = ${repositoryNodeId}
        AND issue_number = ${issueNumber}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlTaskStateRepository.findBySourceNumber", cause),
      ),
      Effect.flatMap((rows) => {
        if (rows.length > 1) {
          return Effect.fail(
            decodeError(
              "AgentControlTaskStateRepository.findBySourceNumber:ambiguous",
              new Error("ambiguous task source number"),
            ),
          );
        }
        const row = rows[0];
        return row === undefined
          ? Effect.succeed(Option.none())
          : decodeAgentControlTaskProjectionRow(
              row,
              "AgentControlTaskStateRepository.findBySourceNumber",
            ).pipe(Effect.map(Option.some));
      }),
    );

  const deleteAll = sql`DELETE FROM main.agent_control_task_states`.pipe(
    Effect.mapError((cause) => sqlError("AgentControlTaskStateRepository.deleteAll", cause)),
    Effect.asVoid,
  );

  return AgentControlTaskStateRepository.of({
    get,
    save,
    listProject,
    listAll,
    findByIdentity,
    findBySourceNumber,
    deleteAll,
  });
});

export const layer = Layer.effect(AgentControlTaskStateRepository, makeRepository);
