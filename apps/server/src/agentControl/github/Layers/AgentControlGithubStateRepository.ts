import {
  AgentControlGithubIntakeState,
  AgentControlGithubIssueSnapshot,
  NonNegativeInt,
  PositiveInt,
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
  AgentControlGithubStateRepository,
  type AgentControlGithubStateRepositoryShape,
} from "../Services/AgentControlGithubStateRepository.ts";

const StateRow = Schema.Struct({
  state: Schema.fromJsonString(AgentControlGithubIntakeState),
  revision: PositiveInt,
  sequence: PositiveInt,
});
const IssueRow = Schema.Struct({
  snapshot: Schema.fromJsonString(AgentControlGithubIssueSnapshot),
});
const decodeStateRow = Schema.decodeUnknownEffect(StateRow);
const decodeIssueRow = Schema.decodeUnknownEffect(IssueRow);
const decodeState = Schema.decodeUnknownEffect(AgentControlGithubIntakeState);
const decodeIssues = Schema.decodeUnknownEffect(Schema.Array(AgentControlGithubIssueSnapshot));
const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlGithubIntakeState),
);
const encodeIssue = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlGithubIssueSnapshot),
);
const decodeRevision = Schema.decodeUnknownEffect(NonNegativeInt);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const makeRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlGithubStateRepositoryShape["get"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT state_json AS state, revision, last_event_sequence AS sequence
      FROM agent_control_github_intake_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlGithubStateRepository.get", cause)),
      Effect.flatMap((rows) => {
        const row = rows[0];
        if (row === undefined) return Effect.succeed(Option.none());
        return decodeStateRow(row).pipe(
          Effect.mapError((cause) => decodeError("AgentControlGithubStateRepository.get", cause)),
          Effect.flatMap(({ state, revision, sequence }) =>
            state.projectId === projectId &&
            state.revision === revision &&
            state.sequence === sequence
              ? Effect.succeed(Option.some(state))
              : Effect.fail(
                  decodeError(
                    "AgentControlGithubStateRepository.get:invariant",
                    new Error("state identity mismatch"),
                  ),
                ),
          ),
        );
      }),
    );

  const save: AgentControlGithubStateRepositoryShape["save"] = (rawState, expectedRevision) =>
    Effect.gen(function* () {
      const state = yield* decodeState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubStateRepository.save:input", cause),
        ),
      );
      const expected = yield* decodeRevision(expectedRevision).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubStateRepository.save:revision", cause),
        ),
      );
      if (state.revision !== expected + 1 || state.updatedAt === null) {
        return yield* decodeError(
          "AgentControlGithubStateRepository.save:invariant",
          new Error("revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expected === 0
          ? yield* sql<{ readonly projectId: unknown }>`
              INSERT INTO agent_control_github_intake_states (
                project_id, state_json, revision, last_event_sequence, updated_at
              ) VALUES (
                ${state.projectId}, ${stateJson}, ${state.revision}, ${state.sequence}, ${state.updatedAt}
              )
              ON CONFLICT (project_id) DO NOTHING
              RETURNING project_id AS "projectId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlGithubStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<{ readonly projectId: unknown }>`
              UPDATE agent_control_github_intake_states
              SET state_json = ${stateJson}, revision = ${state.revision},
                  last_event_sequence = ${state.sequence}, updated_at = ${state.updatedAt}
              WHERE project_id = ${state.projectId} AND revision = ${expected}
              RETURNING project_id AS "projectId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlGithubStateRepository.save:update", cause),
              ),
            );
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlGithubStateRepository.save:conflict",
          new Error("state write conflict"),
        );
      }
    });

  const replaceIssues: AgentControlGithubStateRepositoryShape["replaceIssues"] = (
    projectId,
    rawIssues,
  ) =>
    Effect.gen(function* () {
      const issues = yield* decodeIssues(rawIssues).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubStateRepository.replaceIssues:input", cause),
        ),
      );
      yield* sql`DELETE FROM agent_control_github_timeline_events WHERE project_id = ${projectId}`;
      yield* sql`DELETE FROM agent_control_github_issues WHERE project_id = ${projectId}`;
      yield* Effect.forEach(
        issues,
        (issue) =>
          Effect.gen(function* () {
            const snapshot = yield* encodeIssue(issue).pipe(
              Effect.mapError((cause) =>
                decodeError("AgentControlGithubStateRepository.replaceIssues:encode", cause),
              ),
            );
            yield* sql`
              INSERT INTO agent_control_github_issues (
                project_id, issue_node_id, issue_number, repository_node_id,
                snapshot_json, updated_at
              ) VALUES (
                ${projectId}, ${issue.issueNodeId}, ${issue.number}, ${issue.repositoryNodeId},
                ${snapshot}, ${issue.updatedAt}
              )
            `;
            yield* Effect.forEach(
              issue.timelineEvents,
              (event) =>
                sql`
                  INSERT INTO agent_control_github_timeline_events (
                    project_id, issue_node_id, external_event_id, event_type,
                    label_name, actor_login, occurred_at
                  ) VALUES (
                    ${projectId}, ${issue.issueNodeId}, ${event.externalEventId}, ${event.type},
                    ${event.labelName}, ${event.actorLogin}, ${event.occurredAt}
                  )
                `,
              { concurrency: 1, discard: true },
            );
          }),
        { concurrency: 1, discard: true },
      );
    }).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(sqlError("AgentControlGithubStateRepository.replaceIssues", cause)),
      ),
    );

  const listIssues: AgentControlGithubStateRepositoryShape["listIssues"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT snapshot_json AS snapshot
      FROM agent_control_github_issues
      WHERE project_id = ${projectId}
      ORDER BY issue_number ASC, issue_node_id ASC
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlGithubStateRepository.listIssues", cause)),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeIssueRow(row).pipe(
            Effect.mapError((cause) =>
              decodeError("AgentControlGithubStateRepository.listIssues", cause),
            ),
            Effect.map(({ snapshot }) => snapshot),
          ),
        ),
      ),
    );

  const deleteProject: AgentControlGithubStateRepositoryShape["deleteProject"] = (projectId) =>
    Effect.all(
      [
        sql`DELETE FROM agent_control_github_timeline_events WHERE project_id = ${projectId}`,
        sql`DELETE FROM agent_control_github_issues WHERE project_id = ${projectId}`,
      ],
      { concurrency: 1, discard: true },
    ).pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlGithubStateRepository.deleteProject", cause),
      ),
    );

  const deleteAll = Effect.all(
    [
      sql`DELETE FROM agent_control_github_timeline_events`,
      sql`DELETE FROM agent_control_github_issues`,
      sql`DELETE FROM agent_control_github_intake_states`,
    ],
    { concurrency: 1, discard: true },
  ).pipe(
    Effect.mapError((cause) => sqlError("AgentControlGithubStateRepository.deleteAll", cause)),
  );

  return AgentControlGithubStateRepository.of({
    get,
    save,
    replaceIssues,
    listIssues,
    deleteProject,
    deleteAll,
  });
});

export const layer = Layer.effect(AgentControlGithubStateRepository, makeRepository);
