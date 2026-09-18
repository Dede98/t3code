import type {
  AgentControlEpicRuntimeView,
  AgentControlEpicDependencyPlan,
  AgentControlEpicProjectDependencyPlan,
  AgentControlEpicSource,
  AgentControlVerificationChecks,
  ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { validateEpicDependencyPlan } from "./dependencyPlan.ts";
import { epicDigest, epicJson } from "./authority.ts";

export const createEpicRun = Effect.fn("createEpicRun")(function* (input: {
  projectId: ProjectId;
  commandId: string;
  source: AgentControlEpicSource;
  checks: AgentControlVerificationChecks;
  parallelism?: number;
  dependencyPlan?: AgentControlEpicDependencyPlan;
  projectDependencyPlan?: AgentControlEpicProjectDependencyPlan;
  initialBase?: { commitSha: string; targetBranch: string };
}) {
  yield* validateEpicDependencyPlan(
    input.source,
    input.dependencyPlan,
    input.parallelism,
    input.projectDependencyPlan
      ? new Set([
          ...input.projectDependencyPlan.tasks.map((task) => task.issueNodeId),
          ...input.projectDependencyPlan.epics.map((epic) => epic.issueNodeId),
        ])
      : undefined,
  );
  const now = DateTime.formatIso(yield* DateTime.now);
  return {
    epicRunId: `epic-${epicDigest({ projectId: input.projectId, commandId: input.commandId })}`,
    projectId: input.projectId,
    revision: 1,
    status: "running",
    source: input.source,
    checks: input.checks,
    parallelism: input.parallelism ?? 1,
    ...(input.dependencyPlan
      ? {
          dependencyPlan: input.dependencyPlan,
          dependencyPlanDigest: epicDigest(input.dependencyPlan),
        }
      : {}),
    ...(input.initialBase ? { initialBase: input.initialBase } : {}),
    ...(input.projectDependencyPlan
      ? {
          projectDependencyPlan: input.projectDependencyPlan,
          projectDependencyPlanDigest: epicDigest(input.projectDependencyPlan),
        }
      : {}),
    members: input.source.tasks.map((task) => ({
      issueNodeId: task.issue.issueNodeId,
      issueNumber: task.issue.number,
      taskId: null,
      childRunId: null,
      status: task.issue.state === "closed" ? "external-closed" : "pending",
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
    createdAt: now,
    updatedAt: now,
  } satisfies AgentControlEpicRuntimeView;
});
export const insertEpicRun = Effect.fn("insertEpicRun")(function* (
  sql: SqlClient.SqlClient,
  state: AgentControlEpicRuntimeView,
) {
  const json = epicJson(state);
  const digest = epicDigest(state);
  yield* sql`INSERT INTO main.agent_control_epic_runs(epic_run_id,project_id,revision,state_json,state_digest) VALUES (${state.epicRunId},${state.projectId},${state.revision},${json},${digest})`;
  yield* sql`INSERT INTO main.agent_control_epic_history(epic_run_id,revision,state_json,state_digest) VALUES (${state.epicRunId},${state.revision},${json},${digest})`;
  yield* sql`INSERT INTO main.agent_control_epic_targets(project_id,epic_run_id) VALUES (${state.projectId},${state.epicRunId})`;
});
