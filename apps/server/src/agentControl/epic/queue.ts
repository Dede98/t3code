import {
  AgentControlEpicRpcError,
  AgentControlProjectPolicy,
  isAgentControlEpicQueueEnabled,
  type AgentControlEpicPreview,
  type AgentControlEpicQueue,
  type AgentControlEpicQueueChangeInput,
  type AgentControlEpicQueueEntry,
  type AgentControlEpicRuntimeView,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { epicDigest, epicError, loadSelectedEpic, saveEpicRun } from "./authority.ts";
import { epicStructureDigest } from "./model.ts";
import { loadEpicQueue, saveEpicQueue } from "./queueAuthority.ts";
import { createEpicRun, insertEpicRun } from "./runState.ts";
import { EpicHandoffRemote, EpicHandoffRemoteError } from "./remote.ts";

const decodePolicy = Schema.decodeUnknownEffect(Schema.fromJsonString(AgentControlProjectPolicy));
const isEpicError = Schema.is(AgentControlEpicRpcError);
const isRemoteError = Schema.is(EpicHandoffRemoteError);
export const mapEpicQueueError = (cause: unknown) =>
  isEpicError(cause)
    ? cause
    : epicError(
        "epic-unavailable",
        "Epic queue persistence or execution authority is unavailable.",
      );
const entryForRun = (run: AgentControlEpicRuntimeView): AgentControlEpicQueueEntry => ({
  entryId: `adopted-${run.epicRunId}`,
  source: run.source,
  approvedAt: run.createdAt,
  epicRunId: run.epicRunId,
  status: "active",
  blockers: [],
});

/** Uses the Epic project lock and the Armed worker; no independent execution scheduler. */
export const makeEpicQueue = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* AgentControlEngine;
  const remote = yield* Effect.serviceOption(EpicHandoffRemote);
  const change = Effect.fn("EpicQueue.change")(function* (
    input: AgentControlEpicQueueChangeInput,
    preview: (number: number) => Effect.Effect<AgentControlEpicPreview, AgentControlEpicRpcError>,
  ) {
    const replay = yield* sql<{
      digest: string;
      projectId: string;
    }>`SELECT request_digest AS digest,project_id AS "projectId" FROM main.agent_control_epic_queue_commands WHERE command_id=${input.commandId}`;
    if (replay[0]) {
      if (replay[0].digest !== epicDigest(input) || replay[0].projectId !== input.projectId)
        return yield* epicError(
          "command-conflict",
          "This queue command was already used for another request.",
        );
      const queue = yield* loadEpicQueue(sql, input.projectId);
      if (!queue)
        return yield* epicError("authority-conflict", "The saved queue command lost its queue.");
      return queue;
    }
    const inspected =
      input.action.kind === "approve" ? yield* preview(input.action.epicNumber) : null;
    if (
      input.action.kind === "approve" &&
      inspected?.source.fingerprint !== input.action.expectedFingerprint
    )
      return yield* epicError(
        "scope-changed",
        "The Epic changed since preview. Inspect it again before approving.",
      );
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const previous = yield* loadEpicQueue(sql, input.projectId);
        if ((previous?.revision ?? 0) !== input.expectedRevision)
          return yield* epicError(
            "revision-conflict",
            "The Epic queue changed. Reload before editing it.",
          );
        const enabled = isAgentControlEpicQueueEnabled(previous);
        if (!enabled && input.action.kind !== "approve")
          return yield* epicError("queue-disabled", "Approve an Epic to enable the queue.");
        const selected = yield* loadSelectedEpic(sql, input.projectId);
        if (!enabled && selected?.status === "stopped")
          return yield* epicError(
            "epic-stopped",
            "Clear the stopped Epic selection before enabling its project queue.",
          );
        const project = yield* engine.getProjectState({ projectId: input.projectId });
        if (
          !enabled &&
          !selected &&
          (project.mode === "armed" ||
            project.mode === "run-once" ||
            project.pausedFromMode === "run-once")
        )
          return yield* epicError(
            "project-busy",
            "Disarm ordinary task automation and finish its active run before enabling the Epic queue.",
          );
        const activeRuns =
          yield* sql`SELECT 1 FROM main.agent_control_run_once_states WHERE project_id=${input.projectId} AND status='active'`;
        if (!enabled && !selected && activeRuns.length)
          return yield* epicError(
            "project-busy",
            "Finish the active task before enabling the Epic queue.",
          );
        let entries = [...(enabled ? previous!.entries : selected ? [entryForRun(selected)] : [])];
        const action = input.action;
        if (action.kind === "leave") {
          const active = entries.find((entry) => entry.status === "active");
          if (active && active.epicRunId !== selected?.epicRunId)
            return yield* epicError(
              "authority-conflict",
              "The active queue entry lost its Epic selection.",
            );
          if (
            project.mode === "armed" ||
            project.mode === "run-once" ||
            project.pausedFromMode === "run-once" ||
            activeRuns.length
          )
            return yield* epicError(
              "queue-busy",
              "Turn Armed off and wait for active work to settle before leaving the queue.",
            );
          if (entries.some((entry) => entry.status === "pending"))
            return yield* epicError(
              "queue-has-pending",
              "Remove waiting entries before leaving the queue. Their approval is not discarded automatically.",
            );
          if (selected) {
            if (!entries.some((entry) => entry.epicRunId === selected.epicRunId))
              return yield* epicError(
                "authority-conflict",
                "The selected Epic does not belong to this queue.",
              );
            if (selected.status !== "succeeded" && selected.status !== "stopped")
              yield* saveEpicRun(sql, selected, { status: "stopped" });
            yield* sql`DELETE FROM main.agent_control_epic_targets WHERE project_id=${input.projectId} AND epic_run_id=${selected.epicRunId}`;
          }
          const left = yield* saveEpicQueue(sql, previous, {
            projectId: input.projectId,
            revision: previous!.revision,
            enabled: false,
            entries: [],
            nextEntryId: null,
            waitReason: null,
            nextCheckAt: null,
          });
          yield* sql`INSERT INTO main.agent_control_epic_queue_commands(command_id,request_digest,project_id) VALUES (${input.commandId},${epicDigest(input)},${input.projectId})`;
          return left;
        }
        if (action.kind === "approve") {
          if (!inspected) return yield* epicError("authority-conflict", "Epic preview is missing.");
          if (entries.filter((entry) => entry.status !== "merged").length >= 20)
            return yield* epicError("queue-full", "The queue supports up to 20 Epics.");
          if (
            entries.some(
              (entry) => entry.source.epic.issueNodeId === inspected.source.epic.issueNodeId,
            )
          )
            return yield* epicError(
              "epic-already-approved",
              "This Epic is already approved or has queue execution history.",
            );
          if (
            inspected.source.blockers.some((blocker) =>
              ["cross-repository", "nested-sub-issues", "empty-epic", "closed-epic"].includes(
                blocker.code,
              ),
            )
          )
            return yield* epicError(
              "unsupported-epic",
              "Approve an open, same-repository Epic with one level of native sub-issues.",
            );
          entries.push({
            entryId: `queue-${epicDigest({ projectId: input.projectId, commandId: input.commandId })}`,
            source: inspected.source,
            approvedAt: DateTime.formatIso(yield* DateTime.now),
            epicRunId: null,
            status: "pending",
            blockers: inspected.blockers,
          });
        } else if (action.kind === "remove") {
          const entry = entries.find((entry) => entry.entryId === action.entryId);
          if (!entry || entry.status !== "pending")
            return yield* epicError(
              "entry-started",
              "Only an unstarted queue entry can be removed.",
            );
          entries = entries.filter((item) => item.entryId !== action.entryId);
        } else {
          const pending = entries.filter((entry) => entry.status === "pending");
          if (
            action.entryIds.length !== pending.length ||
            new Set(action.entryIds).size !== pending.length ||
            action.entryIds.some((id) => !pending.some((entry) => entry.entryId === id))
          )
            return yield* epicError(
              "invalid-order",
              "Reorder exactly the current unstarted entries. Active work cannot be moved.",
            );
          entries = [
            ...entries.filter((entry) => entry.status !== "pending"),
            ...action.entryIds.map((id) => pending.find((entry) => entry.entryId === id)!),
          ];
        }
        const queue = yield* saveEpicQueue(sql, previous, {
          projectId: input.projectId,
          enabled: true,
          revision: previous?.revision ?? 0,
          entries,
          nextEntryId: null,
          waitReason: "Queue changed; waiting for Armed to check eligibility.",
          nextCheckAt: null,
        });
        yield* sql`INSERT INTO main.agent_control_epic_queue_commands(command_id,request_digest,project_id) VALUES (${input.commandId},${epicDigest(input)},${input.projectId})`;
        return queue;
      }),
    );
  });

  const process = Effect.fn("EpicQueue.process")(function* (
    projectId: ProjectId,
    preview: (number: number) => Effect.Effect<AgentControlEpicPreview, AgentControlEpicRpcError>,
  ) {
    let queue = yield* loadEpicQueue(sql, projectId);
    if (!queue || !isAgentControlEpicQueueEnabled(queue)) return false;
    const project = yield* engine.getProjectState({ projectId });
    if (project.mode !== "armed" || project.pausedFromMode !== null) return true;
    const now = yield* DateTime.now;
    if (
      queue.nextCheckAt &&
      DateTime.toEpochMillis(DateTime.makeUnsafe(queue.nextCheckAt)) > DateTime.toEpochMillis(now)
    )
      return true;
    const nextCheckAt = DateTime.formatIso(DateTime.add(now, { seconds: 60 }));
    const persist = (changes: Partial<AgentControlEpicQueue>) =>
      sql.withTransaction(saveEpicQueue(sql, queue, { ...queue!, ...changes }));
    const selected = yield* loadSelectedEpic(sql, projectId);
    const active = queue.entries.find((entry) => entry.status === "active");
    if (active && (!selected || selected.epicRunId !== active.epicRunId))
      return yield* epicError(
        "authority-conflict",
        "The active queue entry lost its Epic selection.",
      );
    let entries = [...queue.entries];
    let candidate = entries.find(
      (entry) =>
        entry.status === "pending" &&
        entry.blockers.length === 0 &&
        entry.source.tasks.some((task) => task.issue.state === "open"),
    );
    let candidatePreview: AgentControlEpicPreview | undefined;
    const wait = (reason: string, schedule = true, checkAt = nextCheckAt) =>
      persist({
        entries,
        nextEntryId: candidate?.entryId ?? null,
        waitReason: reason,
        nextCheckAt: schedule ? checkAt : null,
      });
    let previousRun = selected;
    if (active && selected) {
      if (selected.status !== "succeeded") {
        yield* wait(
          selected.status === "stopped"
            ? "The active Epic was stopped. Its queue entry remains reserved for human resolution."
            : selected.status === "blocked"
              ? selected.blockers.map((item) => item.message).join(" ")
              : "The active Epic is executing and verifying its child tasks.",
        );
        return true;
      }
      if (!selected.handoff?.pullRequest) {
        yield* wait("Verification passed. Explicitly publish the draft pull request to continue.");
        return true;
      }
      if (Option.isNone(remote)) {
        yield* wait("GitHub pull request observation is unavailable on this server.");
        return true;
      }
      const projects = yield* sql<{
        cwd: string;
      }>`SELECT workspace_root AS cwd FROM main.projection_projects WHERE project_id=${projectId} AND deleted_at IS NULL`;
      if (!projects[0]) {
        yield* wait("The project directory is no longer available.");
        return true;
      }
      const observed = yield* remote.value
        .readPullRequest({
          cwd: projects[0].cwd,
          repository: selected.handoff.repository,
          pullRequest: selected.handoff.pullRequest,
        })
        .pipe(Effect.catchTag("EpicHandoffRemoteError", (error) => Effect.succeed(error)));
      if (isRemoteError(observed)) {
        yield* wait(observed.message);
        return true;
      }
      if (epicDigest(observed) !== epicDigest(selected.handoff.pullRequest)) {
        previousRun = yield* sql.withTransaction(
          saveEpicRun(sql, selected, {
            handoff: {
              ...selected.handoff,
              pullRequest: observed,
              status: observed.state === "closed" ? "blocked" : "published",
              error:
                observed.state === "closed"
                  ? {
                      code: "pull-request-closed",
                      message:
                        "The pull request was closed without merge. Reopen it or confirm its later merge on GitHub.",
                    }
                  : null,
              updatedAt: DateTime.formatIso(now),
            },
          }),
        );
      }
      if (observed.state !== "merged") {
        yield* wait(
          observed.state === "closed"
            ? "The pull request was closed without merge. Reopen it on GitHub; the queue will check again."
            : "Waiting for human review and merge of the published pull request.",
        );
        return true;
      }
      // Durable merge observation survives a restart before fetching or selecting another run.
      entries = entries.map((entry) =>
        entry.entryId === active.entryId ? { ...entry, status: "merged" } : entry,
      );
      queue = yield* persist({
        entries,
        nextEntryId: candidate?.entryId ?? null,
        waitReason: "Merge confirmed; preparing a fresh target-branch base.",
        nextCheckAt: null,
      });
    }
    // During execution/review the candidate is a saved hint. Refresh pending
    // scopes only at an actual selection boundary, then stop after the first fit.
    candidate = undefined;
    // Pending scope is approved once; state, approval and dependencies are refreshed before each start.
    for (const entry of entries.filter((entry) => entry.status === "pending")) {
      const inspectedResult = yield* Effect.result(preview(entry.source.epic.number));
      if (inspectedResult._tag === "Failure") {
        const error = inspectedResult.failure;
        if (
          !["source-unavailable", "project-unavailable", "intake-incomplete"].includes(error.code)
        )
          return yield* error;
        entries = entries.map((item) =>
          item === entry
            ? {
                ...entry,
                blockers: [
                  {
                    code: error.code,
                    issueNumber: entry.source.epic.number,
                    message: error.message,
                  },
                ],
              }
            : item,
        );
        continue;
      }
      const inspected = inspectedResult.success;
      const blockers =
        epicStructureDigest(inspected.source) === epicStructureDigest(entry.source)
          ? [
              ...inspected.blockers,
              ...(!inspected.source.tasks.some((task) => task.issue.state === "open")
                ? [
                    {
                      code: "no-open-tasks",
                      issueNumber: entry.source.epic.number,
                      message:
                        "This Epic has no open child tasks to execute and cannot produce a verified pull request.",
                    },
                  ]
                : []),
            ]
          : [
              {
                code: "scope-changed",
                issueNumber: entry.source.epic.number,
                message:
                  "Epic membership or dependencies changed after approval. Remove this waiting entry and approve its current scope.",
              },
            ];
      entries = entries.map((item) => (item === entry ? { ...entry, blockers } : item));
      if (!candidate && inspected.canStart && blockers.length === 0) {
        candidate = entry;
        candidatePreview = inspected;
        break;
      }
    }
    if (!candidate || !candidatePreview) {
      yield* wait(
        entries.some((entry) => entry.status === "pending")
          ? "No approved Epic currently satisfies its dependencies and execution requirements."
          : "All approved Epics are complete. Approve another Epic to continue.",
        entries.some((entry) => entry.status === "pending"),
        // Dependency-only waits need full scope reads; poll less often than the saved PR.
        DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      );
      return true;
    }
    if (Option.isNone(remote) || !remote.value.refreshQueueBase) {
      yield* wait("Fresh target-branch loading is unavailable on this server.");
      return true;
    }
    // Selection records actual execution order, which can differ from queue order
    // when an earlier entry is blocked. Never use the last array entry as merge proof.
    if (
      entries.some((entry) => entry.status === "merged") &&
      (!previousRun?.handoff?.pullRequest ||
        !entries.some(
          (entry) => entry.status === "merged" && entry.epicRunId === previousRun.epicRunId,
        ))
    )
      return yield* epicError(
        "authority-conflict",
        "The last completed queue run lost its saved selection or pull request.",
      );
    const projects = yield* sql<{
      cwd: string;
    }>`SELECT workspace_root AS cwd FROM main.projection_projects WHERE project_id=${projectId} AND deleted_at IS NULL`;
    if (!projects[0]) {
      yield* wait("The project directory is no longer available.");
      return true;
    }
    const base = yield* remote.value
      .refreshQueueBase({
        cwd: projects[0].cwd,
        repository: candidate.source.repository,
        ...(previousRun?.handoff ? { previousHandoff: previousRun.handoff } : {}),
      })
      .pipe(Effect.catchTag("EpicHandoffRemoteError", (error) => Effect.succeed(error)));
    if (isRemoteError(base)) {
      yield* wait(base.message);
      return true;
    }
    const policies = yield* sql<{
      policy: string;
    }>`SELECT policy_json AS policy FROM main.agent_control_project_policies WHERE project_id=${projectId}`;
    const policy = yield* decodePolicy(policies[0]?.policy ?? "{}");
    const run = yield* createEpicRun({
      projectId,
      commandId: candidate.entryId,
      source: candidatePreview.source,
      checks: policy.verificationChecks ?? [],
      initialBase: base,
    });
    const nextEntries = entries.map((entry) =>
      entry.entryId === candidate.entryId
        ? { ...entry, status: "active" as const, epicRunId: run.epicRunId, blockers: [] }
        : entry,
    );
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const current = yield* engine.getProjectState({ projectId });
        if (
          current.mode !== "armed" ||
          current.pausedFromMode !== null ||
          current.revision !== project.revision
        )
          return;
        const activeRuns =
          yield* sql`SELECT 1 FROM main.agent_control_run_once_states WHERE project_id=${projectId} AND status='active'`;
        if (activeRuns.length) return;
        const currentSelection = yield* loadSelectedEpic(sql, projectId);
        if (currentSelection?.epicRunId !== selected?.epicRunId)
          return yield* epicError(
            "authority-conflict",
            "Epic selection changed before queue activation.",
          );
        if (currentSelection)
          yield* sql`DELETE FROM main.agent_control_epic_targets WHERE project_id=${projectId} AND epic_run_id=${currentSelection.epicRunId}`;
        yield* insertEpicRun(sql, run);
        yield* saveEpicQueue(sql, queue, {
          ...queue!,
          entries: nextEntries,
          nextEntryId: null,
          waitReason: "The active Epic is executing and verifying its child tasks.",
          nextCheckAt,
        });
      }),
    );
    return true;
  });
  return { change, process };
});
