import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { AgentControlGithubRepositoryBinding } from "./agentControlGithub.ts";

/** Native GitHub relationships describe scope, never execution authorization. */
export const AgentControlEpicIssue = Schema.Struct({
  repositoryNodeId: TrimmedNonEmptyString,
  nameWithOwner: TrimmedNonEmptyString,
  issueNodeId: TrimmedNonEmptyString,
  number: PositiveInt,
  url: TrimmedNonEmptyString,
  title: Schema.String,
  state: Schema.Literals(["open", "closed"]),
  subIssueCount: NonNegativeInt,
});
export type AgentControlEpicIssue = typeof AgentControlEpicIssue.Type;

export const AgentControlEpicTaskSource = Schema.Struct({
  issue: AgentControlEpicIssue,
  position: NonNegativeInt,
  dependencies: Schema.Array(AgentControlEpicIssue),
});
export type AgentControlEpicTaskSource = typeof AgentControlEpicTaskSource.Type;

export const AgentControlEpicSourceBlocker = Schema.Struct({
  code: Schema.Literals([
    "nested-sub-issues",
    "cross-repository",
    "missing-prerequisite",
    "dependency-cycle",
    "empty-epic",
    "closed-epic",
  ]),
  issueNumber: Schema.NullOr(PositiveInt),
  message: Schema.String,
});
export type AgentControlEpicSourceBlocker = typeof AgentControlEpicSourceBlocker.Type;

export const AgentControlEpicSource = Schema.Struct({
  format: Schema.Literal("github-native-sub-issues-v1"),
  repository: AgentControlGithubRepositoryBinding,
  epic: AgentControlEpicIssue,
  dependencies: Schema.optionalKey(Schema.Array(AgentControlEpicIssue)),
  tasks: Schema.Array(AgentControlEpicTaskSource),
  blockers: Schema.Array(AgentControlEpicSourceBlocker),
  fingerprint: TrimmedNonEmptyString,
  inspectedAt: IsoDateTime,
});
export type AgentControlEpicSource = typeof AgentControlEpicSource.Type;
