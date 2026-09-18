import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import {
  ProviderDriverKind,
  type RuntimeMode,
  type ModelSelection,
  type ProviderSession,
} from "@t3tools/contracts";
import {
  type ChatAttachment,
  CommandId,
  EventId,
  type OrchestrationEvent,
  type ProjectId,
  type OrchestrationSession,
  ThreadId,
  type TurnId,
} from "@t3tools/contracts";
import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";
import { buildGeneratedWorktreeBranchName, isTemporaryWorktreeBranch } from "@t3tools/shared/git";
import * as Cache from "effect/Cache";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";

import { resolveThreadWorkspaceCwd } from "../../checkpointing/Utils.ts";
import { increment, orchestrationEventsProcessedTotal } from "../../observability/Metrics.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  ProviderWorkspaceMissingError,
} from "../../provider/Errors.ts";
import type { ProviderServiceError } from "../../provider/Errors.ts";
import { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import { ProviderAuthService } from "../../provider/Services/ProviderAuthService.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import {
  type CoordinatedProviderPermit,
  ProviderResourceCoordinator,
} from "../../resourceAdmission/ProviderResourceCoordinator.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "../../agentControl/initialPlanning/Layers/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningHandoffStore } from "../../agentControl/initialPlanning/Services/AgentControlInitialPlanningHandoffStore.ts";
import { canonicalJson, sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";
import { ProviderTurnRequestExecutorLive } from "./ProviderTurnRequestExecutor.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ProviderCommandReactor,
  type ProviderCommandReactorShape,
} from "../Services/ProviderCommandReactor.ts";
import { ProviderTurnRequestExecutor } from "../Services/ProviderTurnRequestExecutor.ts";
import { ProviderCommandReactorHooks } from "../Services/ProviderCommandReactorHooks.ts";
import { forkParked, ServerActivation } from "../../serverActivation.ts";
import { canReplaceThreadTitle, DEFAULT_THREAD_TITLE } from "../threadTitles.ts";
import {
  resolveSourceControlWriterModelSelection,
  ServerSettingsService,
} from "../../serverSettings.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import { GitWorkflowService } from "../../git/GitWorkflowService.ts";
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
const isProviderAdapterValidationError = Schema.is(ProviderAdapterValidationError);
const isProviderWorkspaceMissingError = Schema.is(ProviderWorkspaceMissingError);
const isProviderDriverKind = Schema.is(ProviderDriverKind);

type ProviderIntentEvent = Extract<
  OrchestrationEvent,
  {
    type:
      | "thread.meta-updated"
      | "thread.runtime-mode-set"
      | "thread.turn-start-requested"
      | "thread.turn-interrupt-requested"
      | "thread.approval-response-requested"
      | "thread.user-input-response-requested"
      | "thread.session-stop-requested"
      | "thread.settled";
  }
>;

function toNonEmptyProviderInput(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : undefined;
}

const isCompactCommandMessage = (message: ThreadTitleMessage): boolean =>
  message.role === "user" &&
  (message.attachments?.length ?? 0) === 0 &&
  message.text.trim().toLowerCase() === "/compact";
function mapProviderSessionStatusToOrchestrationStatus(
  status: "connecting" | "ready" | "running" | "error" | "closed",
): OrchestrationSession["status"] {
  switch (status) {
    case "connecting":
      return "starting";
    case "running":
      return "running";
    case "error":
      return "error";
    case "closed":
      return "stopped";
    case "ready":
    default:
      return "ready";
  }
}

const turnStartKeyForEvent = (event: ProviderIntentEvent): string =>
  event.commandId !== null ? `command:${event.commandId}` : `event:${event.eventId}`;

const HANDLED_TURN_START_KEY_MAX = 10_000;
const HANDLED_TURN_START_KEY_TTL = Duration.minutes(30);
const DEFAULT_RUNTIME_MODE: RuntimeMode = "full-access";
const MAX_REGENERATION_ATTACHMENTS = 4;
const MAX_THREAD_TITLE_CONTEXT_CHARS = 8_000;
const MAX_FIRST_USER_TITLE_CONTEXT_CHARS = 2_000;
const THREAD_TITLE_CONTEXT_TRUNCATION_MARKER = "[Earlier content truncated]\n\n";
const FIRST_USER_CONTEXT_TRUNCATION_MARKER = "\n[First user message truncated]";

type ThreadTitleMessage = {
  readonly role: "user" | "assistant" | "system";
  readonly text: string;
  readonly attachments?: ReadonlyArray<ChatAttachment> | undefined;
};

function formatThreadTitleSection(message: ThreadTitleMessage): string | undefined {
  if (message.role === "system") {
    return undefined;
  }
  const text = assistantCitationsToPlainText(message.text).trim();
  const attachmentSummary = (message.attachments ?? [])
    .map((attachment) => attachment.name)
    .join(", ");
  const contents = [
    ...(text.length > 0 ? [text] : []),
    ...(attachmentSummary.length > 0 ? [`[Attachments: ${attachmentSummary}]`] : []),
  ].join("\n");
  return contents.length > 0 ? `${message.role.toUpperCase()}:\n${contents}` : undefined;
}

function limitFirstUserSection(section: string): string {
  if (section.length <= MAX_FIRST_USER_TITLE_CONTEXT_CHARS) {
    return section;
  }
  return `${section.slice(
    0,
    MAX_FIRST_USER_TITLE_CONTEXT_CHARS - FIRST_USER_CONTEXT_TRUNCATION_MARKER.length,
  )}${FIRST_USER_CONTEXT_TRUNCATION_MARKER}`;
}

function collectRecentThreadTitleContext(
  messages: ReadonlyArray<ThreadTitleMessage>,
  maxChars: number,
): {
  readonly context: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly truncated: boolean;
} {
  let context = "";
  let truncated = false;
  const retainedAttachments: Array<ChatAttachment> = [];

  for (const message of messages.toReversed()) {
    const section = formatThreadTitleSection(message);
    if (section === undefined) {
      continue;
    }

    const separator = context.length > 0 ? "\n\n" : "";
    const available = maxChars - context.length - separator.length;
    if (section.length > available) {
      if (available > 0) {
        context = `${section.slice(-available)}${separator}${context}`;
        retainedAttachments.unshift(...(message.attachments ?? []));
      }
      truncated = true;
      break;
    }
    context = `${section}${separator}${context}`;
    retainedAttachments.unshift(...(message.attachments ?? []));
  }

  return { context, attachments: retainedAttachments, truncated };
}

function formatThreadTitleContext(messages: ReadonlyArray<ThreadTitleMessage>): {
  readonly message: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
} {
  const recent = collectRecentThreadTitleContext(messages, MAX_THREAD_TITLE_CONTEXT_CHARS);
  if (!recent.truncated) {
    return {
      message: recent.context,
      attachments: recent.attachments.slice(-MAX_REGENERATION_ATTACHMENTS),
    };
  }

  const firstUserMessage = messages.find(
    (message) => message.role === "user" && formatThreadTitleSection(message),
  );
  const firstUserSection = firstUserMessage
    ? formatThreadTitleSection(firstUserMessage)
    : undefined;
  if (!firstUserMessage || !firstUserSection) {
    return {
      message: `${THREAD_TITLE_CONTEXT_TRUNCATION_MARKER}${recent.context}`,
      attachments: recent.attachments.slice(-MAX_REGENERATION_ATTACHMENTS),
    };
  }

  const pinnedSection = limitFirstUserSection(firstUserSection);
  const recentContextBudget =
    MAX_THREAD_TITLE_CONTEXT_CHARS -
    pinnedSection.length -
    "\n\n".length -
    THREAD_TITLE_CONTEXT_TRUNCATION_MARKER.length;
  const retainedRecent = collectRecentThreadTitleContext(messages, recentContextBudget);
  const pinnedAttachment = firstUserMessage.attachments?.[0];
  const recentAttachments = retainedRecent.attachments.filter(
    (attachment) => attachment.id !== pinnedAttachment?.id,
  );

  return {
    message: `${pinnedSection}\n\n${THREAD_TITLE_CONTEXT_TRUNCATION_MARKER}${retainedRecent.context}`,
    attachments: [
      ...(pinnedAttachment ? [pinnedAttachment] : []),
      ...recentAttachments.slice(
        -(MAX_REGENERATION_ATTACHMENTS - (pinnedAttachment === undefined ? 0 : 1)),
      ),
    ],
  };
}

function providerErrorLabel(value: string | undefined): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : "unknown";
}

export function providerErrorLabelFromInstanceHint(input: {
  readonly instanceId?: string | undefined;
  readonly modelSelectionInstanceId?: string | undefined;
  readonly sessionProvider?: string | undefined;
}): string {
  return providerErrorLabel(
    input.instanceId ?? input.modelSelectionInstanceId ?? input.sessionProvider,
  );
}

function findProviderAdapterRequestError(
  cause: Cause.Cause<ProviderServiceError>,
): ProviderAdapterRequestError | undefined {
  const failReason = cause.reasons.find(Cause.isFailReason);
  return isProviderAdapterRequestError(failReason?.error) ? failReason.error : undefined;
}

function isUnknownPendingApprovalRequestError(cause: Cause.Cause<ProviderServiceError>): boolean {
  const error = findProviderAdapterRequestError(cause);
  if (error) {
    const detail = error.detail.toLowerCase();
    return (
      detail.includes("unknown pending approval request") ||
      detail.includes("unknown pending permission request") ||
      detail.includes("unknown pending codex approval request")
    );
  }
  const message = Cause.pretty(cause).toLowerCase();
  return (
    message.includes("unknown pending approval request") ||
    message.includes("unknown pending permission request") ||
    message.includes("unknown pending codex approval request")
  );
}

function isUnknownPendingUserInputRequestError(cause: Cause.Cause<ProviderServiceError>): boolean {
  const error = findProviderAdapterRequestError(cause);
  if (error) {
    const detail = error.detail.toLowerCase();
    return (
      detail.includes("unknown pending user-input request") ||
      detail.includes("unknown pending user input request") ||
      detail.includes("unknown pending codex user input request")
    );
  }
  const message = Cause.pretty(cause).toLowerCase();
  return (
    message.includes("unknown pending user-input request") ||
    message.includes("unknown pending user input request") ||
    message.includes("unknown pending codex user input request")
  );
}

function stalePendingRequestDetail(
  requestKind: "approval" | "user-input",
  requestId: string,
): string {
  return `Stale pending ${requestKind} request: ${requestId}. Provider callback state does not survive app restarts or recovered sessions. Restart the turn to continue.`;
}

export const isImplementationOrVerificationOwnedTurnRequest = Effect.fn(
  "ProviderCommandReactor.isImplementationOrVerificationOwnedTurnRequest",
)(function* (sql: SqlClient.SqlClient, commandId: CommandId) {
  const rows = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count FROM (
      SELECT turn_request_command_id FROM main.agent_control_implementation_handoff_accepted
      WHERE turn_request_command_id = ${commandId}
      UNION ALL
      SELECT turn_request_command_id FROM main.agent_control_verification_handoff_accepted
      WHERE turn_request_command_id = ${commandId}
    )
  `;
  const count = rows[0]?.count;
  if (count !== 0 && count !== 1) {
    return yield* Effect.die(
      new Error(`Automated turn ownership is non-unique for '${commandId}'.`),
    );
  }
  return count === 1;
});

interface EpicReviewRepairTurnOwnership {
  readonly requestId: string;
  readonly attempt: number;
  readonly projectId: string;
  readonly epicRunId: string;
  readonly threadId: string;
  readonly messageId: string;
  readonly providerInstanceId: string;
  readonly model: string;
  readonly mayStart: boolean;
}

/**
 * Review repair is autonomous ownership even after revocation. Returning the
 * retained row prevents a cancelled/replayed command from falling through to
 * the manual interactive lane.
 */
export const loadEpicReviewRepairTurnOwnership = Effect.fn(
  "ProviderCommandReactor.loadEpicReviewRepairTurnOwnership",
)(function* (sql: SqlClient.SqlClient, commandId: CommandId) {
  const rows = yield* sql<{
    requestId: string;
    attempt: number;
    projectId: string;
    epicRunId: string;
    threadId: string;
    messageId: string;
    providerInstanceId: string;
    model: string;
    mayStart: number;
  }>`SELECT intent.request_id AS "requestId",intent.attempt,
      request.project_id AS "projectId",request.epic_run_id AS "epicRunId",
      intent.thread_id AS "threadId",intent.message_id AS "messageId",
      intent.provider_instance_id AS "providerInstanceId",intent.model,
      CASE WHEN cancellation.request_id IS NULL AND claim.request_id IS NULL
        AND result.request_id IS NULL AND run.epic_run_id IS NOT NULL
        AND json_extract(run.state_json,'$.status')='verifying'
        AND json_extract(run.state_json,'$.activeReviewReworkId')=intent.request_id
        AND project.mode IN ('armed','run-once') AND project.paused_from_mode IS NULL
        AND EXISTS (
          SELECT 1 FROM json_each(json_extract(run.state_json,'$.reviewReworks')) rework
          WHERE json_extract(rework.value,'$.requestId')=intent.request_id
            AND json_extract(rework.value,'$.status') IN ('accepted','repairing')
        ) THEN 1 ELSE 0 END AS "mayStart"
    FROM main.agent_control_epic_review_repair_intents intent
    JOIN main.agent_control_epic_review_requests request
      ON request.request_id=intent.request_id
    LEFT JOIN main.agent_control_epic_runs run
      ON run.epic_run_id=request.epic_run_id AND run.project_id=request.project_id
    LEFT JOIN main.agent_control_project_states project
      ON project.project_id=request.project_id
    LEFT JOIN main.agent_control_epic_review_repair_delivery_claims claim
      ON claim.request_id=intent.request_id AND claim.attempt=intent.attempt
    LEFT JOIN main.agent_control_epic_review_repair_cancellations cancellation
      ON cancellation.request_id=intent.request_id AND cancellation.attempt=intent.attempt
    LEFT JOIN main.agent_control_epic_review_repair_results result
      ON result.request_id=intent.request_id AND result.attempt=intent.attempt
    WHERE intent.turn_request_command_id=${commandId}`;
  if (rows.length > 1)
    return yield* Effect.die(
      new Error(`Epic review repair turn ownership is non-unique for '${commandId}'.`),
    );
  const row = rows[0];
  return row
    ? ({ ...row, mayStart: row.mayStart === 1 } satisfies EpicReviewRepairTurnOwnership)
    : null;
});

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerAuthService = yield* ProviderAuthService;
  const providerService = yield* ProviderService;
  const providerResourceCoordinator = yield* Effect.serviceOption(ProviderResourceCoordinator);
  const providerRegistry = yield* ProviderRegistry;
  const turnRequestExecutor = yield* ProviderTurnRequestExecutor;
  const initialPlanningStore = yield* AgentControlInitialPlanningHandoffStore;
  const sql = yield* SqlClient.SqlClient;
  const hooks = yield* ProviderCommandReactorHooks;
  const gitWorkflow = yield* GitWorkflowService;
  const fileSystem = yield* FileSystem.FileSystem;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster;
  const textGeneration = yield* TextGeneration;
  const serverSettingsService = yield* ServerSettingsService;
  const serverCommandId = (tag: string) =>
    crypto.randomUUIDv4.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));
  const serverEventId = () => crypto.randomUUIDv4.pipe(Effect.map(EventId.make));
  const handledTurnStartKeys = yield* Cache.make<string, true>({
    capacity: HANDLED_TURN_START_KEY_MAX,
    timeToLive: HANDLED_TURN_START_KEY_TTL,
    lookup: () => Effect.succeed(true),
  });

  const hasHandledTurnStartRecently = (key: string) =>
    Cache.getOption(handledTurnStartKeys, key).pipe(
      Effect.flatMap((cached) =>
        Cache.set(handledTurnStartKeys, key, true).pipe(Effect.as(Option.isSome(cached))),
      ),
    );

  const threadModelSelections = new Map<string, ModelSelection>();
  const compactingThreadIds = new Set<ThreadId>();
  const stoppingThreadIds = new Set<ThreadId>();
  const pendingManualStarts = new Map<string, Fiber.Fiber<void, never>>();
  const pendingManualPermits = new Map<string, CoordinatedProviderPermit>();

  const appendProviderFailureActivity = (input: {
    readonly threadId: ThreadId;
    readonly kind:
      | "provider.turn.start.failed"
      | "provider.turn.interrupt.failed"
      | "provider.approval.respond.failed"
      | "provider.user-input.respond.failed"
      | "provider.session.stop.failed";
    readonly summary: string;
    readonly detail: string;
    readonly turnId: TurnId | null;
    readonly createdAt: string;
    readonly requestId?: string;
  }) =>
    Effect.all({
      commandId: serverCommandId("provider-failure-activity"),
      eventId: serverEventId(),
    }).pipe(
      Effect.flatMap(({ commandId, eventId }) =>
        orchestrationEngine.dispatch({
          type: "thread.activity.append",
          commandId,
          threadId: input.threadId,
          activity: {
            id: eventId,
            tone: "error",
            kind: input.kind,
            summary: input.summary,
            payload: {
              detail: input.detail,
              ...(input.requestId ? { requestId: input.requestId } : {}),
            },
            turnId: input.turnId,
            createdAt: input.createdAt,
          },
          createdAt: input.createdAt,
        }),
      ),
    );

  const formatFailureDetail = (cause: Cause.Cause<unknown>): string => {
    const failReason = cause.reasons.find(Cause.isFailReason);
    if (isProviderAdapterRequestError(failReason?.error)) {
      return failReason.error.detail;
    }
    if (isProviderAdapterValidationError(failReason?.error)) {
      return failReason.error.issue;
    }
    if (isProviderWorkspaceMissingError(failReason?.error)) {
      return failReason.error.message;
    }
    return Cause.pretty(cause);
  };

  const setThreadSession = (input: {
    readonly threadId: ThreadId;
    readonly session: OrchestrationSession;
    readonly createdAt: string;
  }) =>
    serverCommandId("provider-session-set").pipe(
      Effect.flatMap((commandId) =>
        orchestrationEngine.dispatch({
          type: "thread.session.set",
          commandId,
          threadId: input.threadId,
          session: input.session,
          createdAt: input.createdAt,
        }),
      ),
    );

  const setThreadSessionErrorOnTurnStartFailure = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly detail: string;
    readonly createdAt: string;
  }) {
    const thread = yield* resolveThreadShell(input.threadId);
    if (!thread) {
      return;
    }
    const session = thread.session;
    const activeSession = yield* providerService
      .listSessions()
      .pipe(Effect.map((sessions) => sessions.find((entry) => entry.threadId === input.threadId)));
    const activeSessionIsBusy =
      activeSession?.status === "connecting" || activeSession?.status === "running";
    const sessionIsBusy =
      session !== null &&
      activeSessionIsBusy &&
      (session.activeTurnId !== null ||
        session.status === "starting" ||
        session.status === "running");
    const { admissionWait: _admissionWait, ...sessionWithoutAdmissionWait } = session ?? {
      threadId: input.threadId,
      providerName: null,
      providerInstanceId: thread.modelSelection.instanceId,
      runtimeMode: thread.runtimeMode,
    };
    yield* setThreadSession({
      threadId: input.threadId,
      session: {
        ...sessionWithoutAdmissionWait,
        status: sessionIsBusy
          ? session.status
          : session?.status === "stopped"
            ? "stopped"
            : "error",
        activeTurnId: sessionIsBusy ? session.activeTurnId : null,
        lastError: input.detail,
        updatedAt: input.createdAt,
      },
      createdAt: input.createdAt,
    });
  });

  const restoreCompaction = Effect.fnUntraced(function* (threadId: ThreadId, fromRunning = false) {
    if (stoppingThreadIds.has(threadId)) {
      compactingThreadIds.delete(threadId);
      return;
    }
    const thread = yield* resolveThreadShell(threadId);
    if (!thread?.session) return;
    if (
      thread.session.status !== "starting" &&
      thread.session.status !== "ready" &&
      (!fromRunning || thread.session.status !== "running")
    )
      return;
    const completedAt = DateTime.formatIso(yield* DateTime.now);
    if (stoppingThreadIds.has(threadId)) {
      compactingThreadIds.delete(threadId);
      return;
    }
    yield* setThreadSession({
      threadId,
      session: {
        ...(() => {
          const { admissionWait: _admissionWait, ...session } = thread.session;
          return session;
        })(),
        status: "ready",
        activeTurnId: null,
        lastError: null,
        updatedAt: completedAt,
      },
      createdAt: completedAt,
    });
  });

  const resolveProject = Effect.fnUntraced(function* (projectId: ProjectId) {
    return yield* projectionSnapshotQuery
      .getProjectShellById(projectId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  /**
   * Recreates a thread's worktree from its branch when the directory has
   * disappeared. Provider sessions resume into the persisted cwd, so a missing
   * worktree makes every later turn fail as a bogus "session not found".
   * Best-effort: on failure the turn proceeds and reports the real error.
   */
  const ensureThreadWorktree = Effect.fnUntraced(function* (thread: {
    readonly id: ThreadId;
    readonly projectId: ProjectId;
    readonly branch: string | null;
    readonly worktreePath: string | null;
  }) {
    const { worktreePath, branch } = thread;
    if (!worktreePath || !branch) {
      return;
    }
    const exists = yield* fileSystem.exists(worktreePath).pipe(Effect.orElseSucceed(() => true));
    if (exists) {
      return;
    }
    const project = yield* resolveProject(thread.projectId);
    if (!project) {
      return;
    }
    const cwd = project.workspaceRoot;
    yield* Effect.logWarning("provider command reactor recreating missing worktree", {
      threadId: thread.id,
      worktreePath,
      branch,
    });
    // A directory deleted without `git worktree remove` leaves an admin entry
    // that makes `git worktree add` refuse the path; prune clears it.
    yield* gitWorkflow.pruneWorktrees({ cwd }).pipe(
      Effect.andThen(gitWorkflow.createWorktree({ cwd, refName: branch, path: worktreePath })),
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.failCause(cause)
          : Effect.logWarning("provider command reactor failed to recreate worktree", {
              threadId: thread.id,
              worktreePath,
              cause: Cause.pretty(cause),
            }),
      ),
    );
  });

  const resolveThreadShell = Effect.fnUntraced(function* (threadId: ThreadId) {
    return yield* projectionSnapshotQuery
      .getThreadShellById(threadId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const resolveThreadDetail = Effect.fnUntraced(function* (threadId: ThreadId) {
    return yield* projectionSnapshotQuery
      .getThreadDetailById(threadId, { activityKinds: [] })
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const rejectStartedThreadModelChangeIfRequired = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly currentModelSelection: ModelSelection;
    readonly requestedModelSelection: ModelSelection | undefined;
  }) {
    const requestedModelSelection = input.requestedModelSelection;
    if (
      requestedModelSelection === undefined ||
      (input.currentModelSelection.instanceId === requestedModelSelection.instanceId &&
        input.currentModelSelection.model === requestedModelSelection.model)
    ) {
      return;
    }
    const providers = yield* providerRegistry.getProviders;
    const requiresNewThread =
      providers.find((snapshot) => snapshot.instanceId === input.currentModelSelection.instanceId)
        ?.requiresNewThreadForModelChange === true ||
      providers.find((snapshot) => snapshot.instanceId === requestedModelSelection.instanceId)
        ?.requiresNewThreadForModelChange === true;
    if (!requiresNewThread) {
      return;
    }
    return yield* new ProviderAdapterRequestError({
      provider: providerErrorLabelFromInstanceHint({
        instanceId: String(requestedModelSelection.instanceId),
        modelSelectionInstanceId: String(input.currentModelSelection.instanceId),
      }),
      method: "thread.turn.start",
      detail: `Thread '${input.threadId}' cannot switch models after the conversation has started. Start a new thread to use '${requestedModelSelection.model}'.`,
    });
  });

  const ensureSessionForThread = Effect.fn("ensureSessionForThread")(function* (
    threadId: ThreadId,
    createdAt: string,
    options?: {
      readonly modelSelection?: ModelSelection;
      readonly pendingTurnStart?: boolean;
    },
  ) {
    const thread = yield* resolveThreadShell(threadId);
    if (!thread) {
      return yield* Effect.die(new Error(`Thread '${threadId}' was not found in read model.`));
    }

    const initialThreadSession = thread.session;
    const desiredRuntimeMode = thread.runtimeMode;
    const requestedModelSelection = options?.modelSelection;
    const resolveActiveSession = (threadId: ThreadId) =>
      providerService
        .listSessions()
        .pipe(Effect.map((sessions) => sessions.find((session) => session.threadId === threadId)));

    const activeSession = yield* resolveActiveSession(threadId);
    const activeThreadSession =
      initialThreadSession !== null && initialThreadSession.status !== "stopped" && activeSession
        ? initialThreadSession
        : null;
    if (
      activeThreadSession !== null &&
      activeSession !== undefined &&
      (activeThreadSession.providerInstanceId === undefined ||
        activeSession.providerInstanceId === undefined)
    ) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabel(activeThreadSession.providerName ?? undefined),
        method: "thread.turn.start",
        detail: `Thread '${threadId}' has an active provider session without a provider instance id.`,
      });
    }
    const currentInstanceId =
      activeThreadSession !== null &&
      activeSession !== undefined &&
      activeSession.providerInstanceId !== undefined
        ? activeSession.providerInstanceId
        : thread.modelSelection.instanceId;
    const desiredModelSelection = requestedModelSelection ?? thread.modelSelection;
    const desiredInstanceId = desiredModelSelection.instanceId;
    const currentInfo = yield* providerService.getInstanceInfo(currentInstanceId).pipe(
      Effect.mapError(
        () =>
          new ProviderAdapterRequestError({
            provider: providerErrorLabelFromInstanceHint({
              instanceId: String(currentInstanceId),
              modelSelectionInstanceId: String(thread.modelSelection.instanceId),
              sessionProvider: initialThreadSession?.providerName ?? undefined,
            }),
            method: "thread.turn.start",
            detail: `Thread '${threadId}' references unknown provider instance '${currentInstanceId}'. The instance is not configured in this build.`,
          }),
      ),
    );
    const desiredInfo = yield* providerService.getInstanceInfo(desiredInstanceId).pipe(
      Effect.mapError(
        () =>
          new ProviderAdapterRequestError({
            provider: providerErrorLabelFromInstanceHint({
              instanceId: String(desiredModelSelection.instanceId),
            }),
            method: "thread.turn.start",
            detail: `Requested provider instance '${desiredInstanceId}' is not configured in this build.`,
          }),
      ),
    );
    const desiredDriverKind = desiredInfo.driverKind;
    if (!isProviderDriverKind(desiredDriverKind)) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabel(String(desiredDriverKind)),
        method: "thread.turn.start",
        detail: `Requested provider instance '${desiredInstanceId}' uses unknown provider driver '${desiredDriverKind}'. The driver is not installed in this build.`,
      });
    }
    const preferredProvider: ProviderDriverKind = desiredDriverKind;
    if (options?.pendingTurnStart === true && initialThreadSession?.status !== "running") {
      yield* setThreadSession({
        threadId,
        session: {
          threadId,
          status: "starting",
          providerName: activeSession?.provider ?? preferredProvider,
          providerInstanceId: activeSession?.providerInstanceId ?? desiredInstanceId,
          runtimeMode: desiredRuntimeMode,
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      });
    }
    if (initialThreadSession !== null) {
      yield* rejectStartedThreadModelChangeIfRequired({
        threadId,
        currentModelSelection:
          activeSession?.model !== undefined
            ? {
                ...thread.modelSelection,
                instanceId: currentInstanceId,
                model: activeSession.model,
              }
            : thread.modelSelection,
        requestedModelSelection,
      });
    }
    if (
      initialThreadSession !== null &&
      requestedModelSelection !== undefined &&
      requestedModelSelection.instanceId !== currentInstanceId
    ) {
      if (currentInfo.driverKind !== desiredInfo.driverKind) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' is bound to driver '${currentInfo.driverKind}' and cannot switch to '${desiredInfo.driverKind}'.`,
        });
      }
      if (
        currentInfo.continuationIdentity.continuationKey !==
        desiredInfo.continuationIdentity.continuationKey
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' cannot switch from instance '${currentInstanceId}' to '${desiredInstanceId}' because their provider resume state is incompatible.`,
        });
      }
      if (
        activeSession?.activeTurnId !== undefined ||
        activeSession?.status === "connecting" ||
        activeSession?.status === "running" ||
        initialThreadSession.activeTurnId !== null ||
        initialThreadSession.status === "running"
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' cannot switch provider instances while a turn is active. Wait for the turn to finish or interrupt it before continuing on '${desiredInstanceId}'.`,
        });
      }
    }
    const project = yield* resolveProject(thread.projectId);
    const effectiveCwd = resolveThreadWorkspaceCwd({
      thread,
      projects: project ? [project] : [],
    });
    const refreshWorkspaceSnapshot = effectiveCwd
      ? providerRegistry
          .refreshWorkspaceSnapshot({ instanceId: desiredInstanceId, cwd: effectiveCwd })
          .pipe(Effect.forkDetach)
      : Effect.void;

    const startProviderSession = (input?: {
      readonly resumeCursor?: unknown;
      readonly provider?: ProviderDriverKind;
    }) =>
      providerService
        .startSession(threadId, {
          threadId,
          ...(preferredProvider ? { provider: preferredProvider } : {}),
          providerInstanceId: desiredInstanceId,
          ...(effectiveCwd ? { cwd: effectiveCwd } : {}),
          ...(thread.title ? { title: thread.title } : {}),
          modelSelection: desiredModelSelection,
          ...(input?.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
          runtimeMode: desiredRuntimeMode,
        })
        .pipe(Effect.tap(() => refreshWorkspaceSnapshot));

    const bindSessionToThread = (session: ProviderSession) =>
      Effect.gen(function* () {
        if (session.providerInstanceId === undefined) {
          return yield* new ProviderAdapterRequestError({
            provider: providerErrorLabel(session.provider),
            method: "thread.turn.start",
            detail: `Provider session '${session.threadId}' started without a provider instance id.`,
          });
        }
        yield* setThreadSession({
          threadId,
          session: {
            threadId,
            status:
              options?.pendingTurnStart === true && session.status === "ready"
                ? "starting"
                : mapProviderSessionStatusToOrchestrationStatus(session.status),
            providerName: session.provider,
            providerInstanceId: session.providerInstanceId,
            runtimeMode: desiredRuntimeMode,
            // Provider turn ids are not orchestration turn ids.
            activeTurnId: null,
            lastError: session.lastError ?? null,
            updatedAt: session.updatedAt,
          },
          createdAt,
        });
      });

    // An external start can reach the provider before its projection is bound.
    // Reuse it only when every requested session parameter already matches.
    if (
      initialThreadSession === null &&
      activeSession !== undefined &&
      activeSession.status !== "closed" &&
      activeSession.providerInstanceId === desiredInstanceId &&
      activeSession.provider === preferredProvider &&
      activeSession.runtimeMode === desiredRuntimeMode &&
      activeSession.cwd === effectiveCwd &&
      activeSession.model === desiredModelSelection.model
    ) {
      yield* bindSessionToThread(activeSession);
      yield* refreshWorkspaceSnapshot;
      return threadId;
    }

    const existingSessionThreadId =
      initialThreadSession && initialThreadSession.status !== "stopped" && activeSession
        ? thread.id
        : null;
    if (existingSessionThreadId && initialThreadSession) {
      const runtimeModeChanged = thread.runtimeMode !== initialThreadSession.runtimeMode;
      const cwdChanged = effectiveCwd !== activeSession?.cwd;
      const sessionModelSwitch = (yield* providerService.getCapabilities(desiredInstanceId))
        .sessionModelSwitch;
      const modelChanged =
        requestedModelSelection !== undefined &&
        requestedModelSelection.model !== activeSession?.model;
      const instanceChanged =
        requestedModelSelection !== undefined &&
        activeSession?.providerInstanceId !== requestedModelSelection.instanceId;
      const shouldRestartForModelChange = modelChanged && sessionModelSwitch === "unsupported";
      const previousModelSelection = threadModelSelections.get(threadId);
      const shouldRestartForModelSelectionChange =
        preferredProvider === "claudeAgent" &&
        requestedModelSelection !== undefined &&
        !Equal.equals(previousModelSelection, requestedModelSelection);

      if (
        !runtimeModeChanged &&
        !cwdChanged &&
        !instanceChanged &&
        !shouldRestartForModelChange &&
        !shouldRestartForModelSelectionChange
      ) {
        yield* refreshWorkspaceSnapshot;
        return existingSessionThreadId;
      }

      const resumeCursor = shouldRestartForModelChange
        ? undefined
        : (activeSession?.resumeCursor ?? undefined);
      yield* Effect.logInfo("provider command reactor restarting provider session", {
        threadId,
        existingSessionThreadId,
        currentProvider: activeSession?.provider,
        currentInstanceId,
        desiredInstanceId,
        desiredProvider: desiredModelSelection.instanceId,
        currentRuntimeMode: initialThreadSession.runtimeMode,
        desiredRuntimeMode: thread.runtimeMode,
        runtimeModeChanged,
        previousCwd: activeSession?.cwd,
        desiredCwd: effectiveCwd,
        cwdChanged,
        modelChanged,
        instanceChanged,
        shouldRestartForModelChange,
        shouldRestartForModelSelectionChange,
        hasResumeCursor: resumeCursor !== undefined,
      });
      const restartedSession = yield* startProviderSession(
        resumeCursor !== undefined ? { resumeCursor } : undefined,
      );
      yield* Effect.logInfo("provider command reactor restarted provider session", {
        threadId,
        previousSessionId: existingSessionThreadId,
        restartedSessionThreadId: restartedSession.threadId,
        provider: restartedSession.provider,
        runtimeMode: restartedSession.runtimeMode,
        cwd: restartedSession.cwd,
      });
      yield* bindSessionToThread(restartedSession);
      return restartedSession.threadId;
    }

    const startedSession = yield* startProviderSession(undefined);
    yield* bindSessionToThread(startedSession);
    return startedSession.threadId;
  });

  const buildSendTurnRequestForThread = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly messageText: string;
    readonly attachments?: ReadonlyArray<ChatAttachment>;
    readonly modelSelection?: ModelSelection;
    readonly interactionMode?: "default" | "plan";
    readonly createdAt: string;
  }) {
    const thread = yield* resolveThreadShell(input.threadId);
    if (!thread) {
      return yield* Effect.die(
        new Error(`Thread '${input.threadId}' was not found in read model.`),
      );
    }
    yield* ensureSessionForThread(input.threadId, input.createdAt, {
      ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
      pendingTurnStart: true,
    });
    if (input.modelSelection !== undefined) {
      if (!Equal.equals(thread.modelSelection, input.modelSelection)) {
        yield* orchestrationEngine.dispatch({
          type: "thread.meta.update",
          commandId: yield* serverCommandId("provider-model-selection-commit"),
          threadId: input.threadId,
          modelSelection: input.modelSelection,
        });
      }
      threadModelSelections.set(input.threadId, input.modelSelection);
    }
    const normalizedInput = toNonEmptyProviderInput(input.messageText);
    const normalizedAttachments = input.attachments ?? [];
    const activeSession = yield* providerService
      .listSessions()
      .pipe(
        Effect.map((sessions) => sessions.find((session) => session.threadId === input.threadId)),
      );
    const sessionModelSwitch =
      activeSession === undefined
        ? "in-session"
        : activeSession.providerInstanceId === undefined
          ? yield* new ProviderAdapterRequestError({
              provider: providerErrorLabel(activeSession.provider),
              method: "thread.turn.start",
              detail: `Active provider session '${activeSession.threadId}' is missing a provider instance id.`,
            })
          : (yield* providerService.getCapabilities(activeSession.providerInstanceId))
              .sessionModelSwitch;
    const requestedModelSelection =
      input.modelSelection ?? threadModelSelections.get(input.threadId) ?? thread.modelSelection;
    const modelForTurn =
      sessionModelSwitch === "unsupported" && input.modelSelection === undefined
        ? activeSession?.model !== undefined
          ? {
              ...requestedModelSelection,
              model: activeSession.model,
            }
          : requestedModelSelection
        : input.modelSelection;

    return {
      threadId: input.threadId,
      ...(normalizedInput ? { input: normalizedInput } : {}),
      ...(normalizedAttachments.length > 0 ? { attachments: normalizedAttachments } : {}),
      ...(modelForTurn !== undefined ? { modelSelection: modelForTurn } : {}),
      ...(input.interactionMode !== undefined ? { interactionMode: input.interactionMode } : {}),
    };
  });

  const maybeGenerateAndRenameWorktreeBranchForFirstTurn = Effect.fn(
    "maybeGenerateAndRenameWorktreeBranchForFirstTurn",
  )(function* (input: {
    readonly threadId: ThreadId;
    readonly branch: string | null;
    readonly worktreePath: string | null;
    readonly messageText: string;
    readonly attachments?: ReadonlyArray<ChatAttachment>;
  }) {
    if (!input.branch || !input.worktreePath) {
      return;
    }
    if (!isTemporaryWorktreeBranch(input.branch)) {
      return;
    }

    const oldBranch = input.branch;
    const cwd = input.worktreePath;
    const attachments = input.attachments ?? [];
    yield* Effect.gen(function* () {
      const settings = yield* serverSettingsService.getSettings;
      const modelSelection =
        settings.sourceControlWriterModelSelection === null
          ? settings.textGenerationModelSelection
          : resolveSourceControlWriterModelSelection(
              settings,
              yield* providerRegistry.getProviders,
            );
      const branchNameMode = settings.worktreeBranchNameMode;
      const branchPrefix = settings.worktreeBranchPrefix;

      const generated = yield* textGeneration.generateBranchName({
        cwd,
        message: input.messageText,
        ...(attachments.length > 0 ? { attachments } : {}),
        branchNameMode,
        modelSelection,
      });
      if (!generated) return;

      const targetBranch = buildGeneratedWorktreeBranchName(
        generated.branch,
        branchNameMode,
        branchPrefix,
      );
      if (targetBranch === oldBranch) return;

      const renamed = yield* gitWorkflow.renameBranch({ cwd, oldBranch, newBranch: targetBranch });
      yield* orchestrationEngine.dispatch({
        type: "thread.meta.update",
        commandId: yield* serverCommandId("worktree-branch-rename"),
        threadId: input.threadId,
        branch: renamed.branch,
        worktreePath: cwd,
      });
      yield* vcsStatusBroadcaster.refreshStatus(cwd).pipe(Effect.ignoreCause({ log: true }));
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider command reactor failed to generate or rename worktree branch", {
          threadId: input.threadId,
          cwd,
          oldBranch,
          cause: Cause.pretty(cause),
        }),
      ),
    );
  });

  const maybeGenerateThreadTitleForFirstTurn = Effect.fn("maybeGenerateThreadTitleForFirstTurn")(
    function* (input: {
      readonly threadId: ThreadId;
      readonly cwd: string;
      readonly messageText: string;
      readonly attachments?: ReadonlyArray<ChatAttachment>;
      readonly titleSeed?: string;
    }) {
      const attachments = input.attachments ?? [];
      yield* Effect.gen(function* () {
        const { textGenerationModelSelection: modelSelection } =
          yield* serverSettingsService.getSettings;

        const generated = yield* textGeneration
          .generateThreadTitle({
            cwd: input.cwd,
            message: input.messageText,
            ...(attachments.length > 0 ? { attachments } : {}),
            modelSelection,
          })
          .pipe(
            Effect.retry({
              times: 2,
              schedule: Schedule.exponential("2 seconds"),
            }),
          );
        if (!generated) return;

        const thread = yield* resolveThreadShell(input.threadId);
        if (!thread) return;
        if (!canReplaceThreadTitle(thread.title, input.titleSeed)) {
          return;
        }

        yield* orchestrationEngine.dispatch({
          type: "thread.meta.update",
          commandId: yield* serverCommandId("thread-title-rename"),
          threadId: input.threadId,
          title: generated.title,
        });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider command reactor failed to generate or rename thread title", {
            threadId: input.threadId,
            cwd: input.cwd,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    },
  );

  const regenerateThreadTitle = Effect.fn("regenerateThreadTitle")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.meta-updated" }>,
    requestId: CommandId,
  ) {
    if (event.payload.regenerateTitle !== true) {
      return { _tag: "Superseded" } as const;
    }

    const thread = yield* resolveThreadDetail(event.payload.threadId);
    if (!thread || thread.titleRegeneration?.requestId !== requestId) {
      return { _tag: "Superseded" } as const;
    }

    const { message, attachments } = formatThreadTitleContext(thread.messages);
    if (message.length === 0) {
      return { _tag: "Completed", title: undefined } as const;
    }

    const previousTitle = event.payload.previousTitle ?? thread.title;
    if (thread.title !== previousTitle) {
      return { _tag: "Superseded" } as const;
    }
    const project = yield* resolveProject(thread.projectId);
    const cwd =
      resolveThreadWorkspaceCwd({
        thread,
        projects: project ? [project] : [],
      }) ?? process.cwd();
    const { textGenerationModelSelection: modelSelection } =
      yield* serverSettingsService.getSettings;
    const generated = yield* textGeneration.generateThreadTitle({
      cwd,
      message,
      previousTitle,
      ...(attachments.length > 0 ? { attachments } : {}),
      modelSelection,
    });
    if (generated.title === DEFAULT_THREAD_TITLE || generated.title === previousTitle) {
      return { _tag: "Completed", title: undefined } as const;
    }

    const latestThread = yield* resolveThreadShell(event.payload.threadId);
    if (
      !latestThread ||
      latestThread.titleRegeneration?.requestId !== requestId ||
      latestThread.title !== previousTitle
    ) {
      return { _tag: "Superseded" } as const;
    }

    return { _tag: "Completed", title: generated.title } as const;
  });
  const dispatchThreadTitleRegenerationCompletion = Effect.fn(
    "dispatchThreadTitleRegenerationCompletion",
  )(function* (input: {
    readonly threadId: ThreadId;
    readonly requestId: CommandId;
    readonly title?: string;
  }) {
    yield* orchestrationEngine.dispatch({
      type: "thread.title.regeneration.complete",
      commandId: yield* serverCommandId("thread-title-regeneration-complete"),
      threadId: input.threadId,
      requestId: input.requestId,
      ...(input.title !== undefined ? { title: input.title } : {}),
    });
  });
  const findInterruptedThreadTitleRegenerations = Effect.fn(
    "findInterruptedThreadTitleRegenerations",
  )(function* () {
    const readModel = yield* projectionSnapshotQuery.getCommandReadModel();
    return readModel.threads.flatMap((thread) => {
      const requestId = thread.titleRegeneration?.requestId;
      return requestId === undefined ? [] : [{ threadId: thread.id, requestId }];
    });
  });
  const clearInterruptedThreadTitleRegenerations = Effect.fn(
    "clearInterruptedThreadTitleRegenerations",
  )(function* (
    interrupted: ReadonlyArray<{ readonly threadId: ThreadId; readonly requestId: CommandId }>,
  ) {
    yield* Effect.forEach(
      interrupted,
      ({ threadId, requestId }) => {
        return dispatchThreadTitleRegenerationCompletion({
          threadId,
          requestId,
        }).pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.interrupt;
            }
            return Effect.logWarning(
              "provider command reactor failed to clear interrupted title regeneration",
              {
                threadId,
                cause: Cause.pretty(cause),
              },
            );
          }),
        );
      },
      { discard: true },
    );
  });
  const processThreadTitleRegenerationSafely = Effect.fn("processThreadTitleRegenerationSafely")(
    function* (event: Extract<ProviderIntentEvent, { type: "thread.meta-updated" }>) {
      if (event.payload.regenerateTitle !== true) {
        return;
      }

      const requestId = event.payload.titleRegeneration?.requestId ?? event.commandId;
      if (requestId === null) {
        return;
      }
      const result = yield* regenerateThreadTitle(event, requestId).pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(cause);
          }
          return Effect.logWarning("provider command reactor failed to regenerate thread title", {
            threadId: event.payload.threadId,
            cause: Cause.pretty(cause),
          }).pipe(Effect.as({ _tag: "Completed", title: undefined } as const));
        }),
      );
      if (result._tag === "Superseded") {
        return;
      }

      const completion = {
        threadId: event.payload.threadId,
        requestId,
        ...(result.title !== undefined ? { title: result.title } : {}),
      };
      yield* dispatchThreadTitleRegenerationCompletion(completion).pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(cause);
          }
          return Effect.logWarning(
            "provider command reactor retrying title regeneration completion",
            {
              threadId: event.payload.threadId,
              cause: Cause.pretty(cause),
            },
          ).pipe(Effect.andThen(dispatchThreadTitleRegenerationCompletion(completion)));
        }),
      );
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(cause);
          }
          return Effect.logWarning(
            "provider command reactor failed to complete title regeneration",
            {
              threadId: event.payload.threadId,
              cause: Cause.pretty(cause),
            },
          );
        }),
      ),
  );
  const threadTitleRegenerationWorker = yield* makeDrainableWorker(
    processThreadTitleRegenerationSafely,
  );

  const processTurnStartRequested = Effect.fn("processTurnStartRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.turn-start-requested" }>,
  ) {
    let reviewRepairOwnership: EpicReviewRepairTurnOwnership | null = null;
    let sealedReviewRepairThread = false;
    if (event.commandId !== null) {
      const commandId = event.commandId;
      yield* hooks.beforeInitialPlanningOwnershipRead(commandId);
      const handoffOwned = yield* initialPlanningStore
        .isHandoffOwnedTurnRequest(commandId)
        .pipe(
          Effect.onError(
            (cause) =>
              hooks.onInitialPlanningOwnershipReadFailure?.(commandId, cause) ?? Effect.void,
          ),
        );
      yield* hooks.afterInitialPlanningOwnershipRead(commandId, handoffOwned);
      if (handoffOwned) {
        return;
      }
      if (yield* isImplementationOrVerificationOwnedTurnRequest(sql, commandId)) {
        return;
      }
      reviewRepairOwnership = yield* loadEpicReviewRepairTurnOwnership(sql, commandId);
      if (reviewRepairOwnership && !reviewRepairOwnership.mayStart) return;
    }
    if (reviewRepairOwnership === null) {
      const sealed = yield* sql`SELECT 1
        FROM main.agent_control_epic_review_repair_intents
        WHERE thread_id=${event.payload.threadId}
        LIMIT 1`;
      sealedReviewRepairThread = sealed.length === 1;
    }
    const key = turnStartKeyForEvent(event);
    if (yield* hasHandledTurnStartRecently(key)) {
      return;
    }

    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }
    if (
      reviewRepairOwnership &&
      (reviewRepairOwnership.threadId !== event.payload.threadId ||
        reviewRepairOwnership.messageId !== event.payload.messageId ||
        thread.projectId !== reviewRepairOwnership.projectId)
    )
      return yield* Effect.die(
        new Error("Epic review repair turn event does not match its durable ownership."),
      );
    const turnStart = yield* projectionSnapshotQuery.getTurnStartMessage({
      threadId: thread.id,
      messageId: event.payload.messageId,
    });
    if (Option.isNone(turnStart) || turnStart.value.message.role !== "user") {
      yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.start.failed",
        summary: "Provider turn start failed",
        detail: `User message '${event.payload.messageId}' was not found for turn start request.`,
        turnId: null,
        createdAt: event.payload.createdAt,
        requestId: event.payload.messageId,
      });
      return;
    }
    const { message, hasOtherUserMessages } = turnStart.value;
    const appendTurnStartFailure = (summary: string, detail: string) =>
      appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.start.failed",
        summary,
        detail,
        turnId: null,
        createdAt: event.payload.createdAt,
        requestId: event.payload.messageId,
      });
    if (sealedReviewRepairThread) {
      const detail =
        "This retained review-repair thread accepts only its immutable findings-bound turn.";
      yield* setThreadSessionErrorOnTurnStartFailure({
        threadId: event.payload.threadId,
        detail,
        createdAt: event.payload.createdAt,
      });
      yield* appendTurnStartFailure("Review repair thread is sealed", detail);
      return;
    }

    const handleTurnStartFailure = (cause: Cause.Cause<unknown>) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.void;
      }
      const detail = formatFailureDetail(cause);
      return setThreadSessionErrorOnTurnStartFailure({
        threadId: event.payload.threadId,
        detail,
        createdAt: event.payload.createdAt,
      }).pipe(
        Effect.flatMap(() => appendTurnStartFailure("Provider turn start failed", detail)),
        Effect.asVoid,
      );
    };

    const recoverTurnStartFailure = (cause: Cause.Cause<unknown>) =>
      handleTurnStartFailure(cause).pipe(
        Effect.catchCause((recoveryCause) =>
          Effect.logWarning("provider command reactor failed to recover turn start failure", {
            eventType: event.type,
            threadId: event.payload.threadId,
            cause: Cause.pretty(recoveryCause),
            originalCause: Cause.pretty(cause),
          }),
        ),
      );

    const authCommandHandled = yield* Effect.gen(function* () {
      // Native account commands belong to the thread's existing provider session.
      const instanceId =
        thread.session?.providerInstanceId ??
        event.payload.modelSelection?.instanceId ??
        thread.modelSelection.instanceId;
      const handled = yield* providerAuthService.tryHandlePromptCommand({
        instanceId,
        text: message.text,
        hasAttachments: (message.attachments?.length ?? 0) > 0,
      });
      if (!handled) {
        return false;
      }

      const instanceInfo = yield* providerService.getInstanceInfo(instanceId);
      yield* setThreadSession({
        threadId: thread.id,
        session: {
          threadId: thread.id,
          status: "stopped",
          providerName: instanceInfo.driverKind,
          providerInstanceId: instanceId,
          runtimeMode: thread.runtimeMode,
          activeTurnId: null,
          lastError: null,
          updatedAt: event.payload.createdAt,
        },
        createdAt: event.payload.createdAt,
      });
      yield* orchestrationEngine.dispatch({
        type: "thread.activity.append",
        commandId: yield* serverCommandId("provider-sign-out"),
        threadId: thread.id,
        activity: {
          id: yield* serverEventId(),
          tone: "info",
          kind: "provider.auth.signed-out",
          summary: "Provider signed out",
          payload: { providerInstanceId: instanceId },
          turnId: null,
          createdAt: event.payload.createdAt,
        },
        createdAt: event.payload.createdAt,
      });
      return true;
    }).pipe(Effect.catchCause((cause) => recoverTurnStartFailure(cause).pipe(Effect.as(true))));
    if (authCommandHandled) {
      return;
    }

    yield* ensureThreadWorktree(thread);

    const isCompactCommand = isCompactCommandMessage(message);
    if (!hasOtherUserMessages && !isCompactCommand) {
      const project = yield* resolveProject(thread.projectId);
      const generationCwd =
        resolveThreadWorkspaceCwd({
          thread,
          projects: project ? [project] : [],
        }) ?? process.cwd();
      const generationInput = {
        messageText: assistantCitationsToPlainText(message.text),
        ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
        ...(event.payload.titleSeed !== undefined ? { titleSeed: event.payload.titleSeed } : {}),
      };

      yield* maybeGenerateAndRenameWorktreeBranchForFirstTurn({
        threadId: event.payload.threadId,
        branch: thread.branch,
        worktreePath: thread.worktreePath,
        ...generationInput,
      }).pipe(Effect.forkScoped);

      if (canReplaceThreadTitle(thread.title, event.payload.titleSeed)) {
        yield* maybeGenerateThreadTitleForFirstTurn({
          threadId: event.payload.threadId,
          cwd: generationCwd,
          ...generationInput,
        }).pipe(Effect.forkScoped);
      }
    }

    let compactionSessionEnsured = false;
    const handleCompactionFailure = (cause: Cause.Cause<unknown>) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.void;
      }
      const detail = formatFailureDetail(cause);
      if (!compactionSessionEnsured) {
        return setThreadSessionErrorOnTurnStartFailure({
          threadId: event.payload.threadId,
          detail,
          createdAt: event.payload.createdAt,
        }).pipe(
          Effect.flatMap(() => appendTurnStartFailure("Context compaction failed", detail)),
          Effect.asVoid,
        );
      }
      return appendTurnStartFailure("Context compaction failed", detail).pipe(
        Effect.ensuring(
          restoreCompaction(event.payload.threadId).pipe(
            Effect.catchCause((restoreCause) =>
              Effect.logWarning("failed to restore provider session after compaction failure", {
                threadId: event.payload.threadId,
                cause: Cause.pretty(restoreCause),
              }),
            ),
          ),
        ),
        Effect.asVoid,
      );
    };
    const recoverCompactionFailure = (cause: Cause.Cause<unknown>) =>
      Cause.hasInterrupts(cause)
        ? Effect.void
        : handleCompactionFailure(cause).pipe(
            Effect.catchCause((recoveryCause) =>
              Effect.logWarning("provider command reactor failed to recover compaction failure", {
                eventType: event.type,
                threadId: event.payload.threadId,
                cause: Cause.pretty(recoveryCause),
                originalCause: Cause.pretty(cause),
              }),
            ),
          );
    if (isCompactCommand) {
      if (!hasOtherUserMessages) {
        return yield* appendTurnStartFailure(
          "Context compaction failed",
          "Context compaction requires an existing conversation.",
        );
      }
      const latestThread = yield* resolveThreadShell(event.payload.threadId);
      if (
        compactingThreadIds.has(event.payload.threadId) ||
        latestThread?.session?.status === "starting" ||
        latestThread?.session?.status === "running"
      ) {
        yield* appendTurnStartFailure(
          "Context compaction failed",
          "Context compaction is unavailable while a provider turn is running.",
        );
        return;
      }
      compactingThreadIds.add(event.payload.threadId);
      let providerInvocationStarted = false;
      const compactStart = Effect.gen(function* () {
        yield* ensureSessionForThread(
          event.payload.threadId,
          event.payload.createdAt,
          event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection, pendingTurnStart: true }
            : { pendingTurnStart: true },
        );
        compactionSessionEnsured = true;
        if (event.payload.modelSelection !== undefined) {
          threadModelSelections.set(event.payload.threadId, event.payload.modelSelection);
        }
        if (Option.isNone(providerResourceCoordinator)) {
          yield* providerService.compactThread(
            event.payload.threadId,
            event.payload.modelSelection,
            event.payload.messageId,
          );
          return;
        }
        const coordinator = providerResourceCoordinator.value;
        const instanceId =
          event.payload.modelSelection?.instanceId ??
          thread.session?.providerInstanceId ??
          thread.modelSelection.instanceId;
        const instanceInfo = yield* providerService.getInstanceInfo(instanceId);
        const setAdmissionWait = (admissionWait: OrchestrationSession["admissionWait"]) =>
          Effect.gen(function* () {
            const latest = yield* resolveThreadShell(event.payload.threadId);
            const current = latest?.session;
            yield* setThreadSession({
              threadId: event.payload.threadId,
              session: {
                threadId: event.payload.threadId,
                status: "starting",
                providerName: current?.providerName ?? instanceInfo.driverKind,
                providerInstanceId: instanceId,
                runtimeMode: current?.runtimeMode ?? thread.runtimeMode,
                activeTurnId: null,
                ...(admissionWait === undefined ? {} : { admissionWait }),
                lastError: null,
                updatedAt: event.payload.createdAt,
              },
              createdAt: event.payload.createdAt,
            });
          });
        const permit = yield* Effect.uninterruptibleMask((restore) =>
          Effect.flatMap(
            restore(
              coordinator.acquire({
                idempotencyKey: `manual-compact:${key}`,
                providerInstanceId: instanceId,
                continuationKey: String(instanceInfo.driverKind),
                threadId: String(event.payload.threadId),
                requestedAt: event.payload.createdAt,
                workloadClass: "interactive",
                source: "manual",
                onWait: (wait) => setAdmissionWait(wait).pipe(Effect.ignore),
              }),
            ),
            (acquired) =>
              Effect.sync(() => pendingManualPermits.set(event.payload.threadId, acquired)).pipe(
                Effect.as(acquired),
              ),
          ),
        );
        yield* setAdmissionWait(undefined);
        yield* coordinator.enter(permit);
        yield* providerService
          .compactThread(
            event.payload.threadId,
            event.payload.modelSelection,
            event.payload.messageId,
            {
              onInvocationStarted: () => {
                providerInvocationStarted = true;
              },
            },
          )
          .pipe(Effect.tap(() => coordinator.release(permit)));
      }).pipe(
        Effect.andThen(restoreCompaction(event.payload.threadId, true)),
        Effect.onError(() => {
          const permit = pendingManualPermits.get(event.payload.threadId);
          if (
            permit === undefined ||
            providerInvocationStarted ||
            Option.isNone(providerResourceCoordinator)
          )
            return Effect.void;
          return providerResourceCoordinator.value.release(permit).pipe(Effect.ignore);
        }),
        Effect.onInterrupt(() => {
          const permit = pendingManualPermits.get(event.payload.threadId);
          if (
            permit === undefined ||
            providerInvocationStarted ||
            Option.isNone(providerResourceCoordinator)
          )
            return Effect.void;
          return providerResourceCoordinator.value.release(permit).pipe(Effect.ignore);
        }),
        Effect.catchCause(recoverCompactionFailure),
        Effect.ensuring(
          Effect.sync(() => {
            compactingThreadIds.delete(event.payload.threadId);
            pendingManualStarts.delete(event.payload.threadId);
            pendingManualPermits.delete(event.payload.threadId);
          }),
        ),
      );
      const gate = yield* Deferred.make<void>();
      const fiber = yield* Deferred.await(gate).pipe(
        Effect.andThen(compactStart),
        Effect.forkScoped,
      );
      pendingManualStarts.set(event.payload.threadId, fiber);
      yield* Deferred.succeed(gate, undefined);
      return;
    }
    if (compactingThreadIds.has(event.payload.threadId)) {
      return yield* appendTurnStartFailure(
        "Provider turn start failed",
        "Wait for context compaction to finish before sending another message.",
      );
    }
    const sendTurnRequest = yield* buildSendTurnRequestForThread({
      threadId: event.payload.threadId,
      messageText: message.text,
      ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
      ...(event.payload.modelSelection !== undefined
        ? { modelSelection: event.payload.modelSelection }
        : {}),
      interactionMode: event.payload.interactionMode,
      createdAt: event.payload.createdAt,
    }).pipe(
      Effect.map(Option.some),
      Effect.catchCause((cause) => handleTurnStartFailure(cause).pipe(Effect.as(Option.none()))),
    );

    if (Option.isNone(sendTurnRequest)) {
      return;
    }
    const request = sendTurnRequest.value;
    if (
      reviewRepairOwnership &&
      (request.modelSelection?.instanceId !== reviewRepairOwnership.providerInstanceId ||
        request.modelSelection.model !== reviewRepairOwnership.model)
    )
      return yield* Effect.die(
        new Error("Epic review repair turn model does not match its immutable intent."),
      );
    if (Option.isNone(providerResourceCoordinator)) {
      if (reviewRepairOwnership)
        return yield* appendTurnStartFailure(
          "Review repair could not start",
          "Shared automatic provider admission is unavailable.",
        );
      yield* providerService
        .sendTurn(request)
        .pipe(Effect.asVoid, Effect.catchCause(recoverTurnStartFailure), Effect.forkScoped);
      return;
    }
    const resourceCoordinator = providerResourceCoordinator.value;
    const instanceId =
      request.modelSelection?.instanceId ??
      thread.session?.providerInstanceId ??
      thread.modelSelection.instanceId;
    const instanceInfo = yield* providerService.getInstanceInfo(instanceId).pipe(
      Effect.map(Option.some),
      Effect.catchCause((cause) => recoverTurnStartFailure(cause).pipe(Effect.as(Option.none()))),
    );
    if (Option.isNone(instanceInfo)) return;
    const setAdmissionWait = (admissionWait: OrchestrationSession["admissionWait"]) =>
      Effect.gen(function* () {
        const latest = yield* resolveThreadShell(event.payload.threadId);
        const session = latest?.session;
        yield* setThreadSession({
          threadId: event.payload.threadId,
          session: {
            threadId: event.payload.threadId,
            status: "starting",
            providerName: session?.providerName ?? instanceInfo.value.driverKind,
            providerInstanceId: instanceId,
            runtimeMode: session?.runtimeMode ?? thread.runtimeMode,
            activeTurnId: null,
            ...(admissionWait === undefined ? {} : { admissionWait }),
            lastError: null,
            updatedAt: event.payload.createdAt,
          },
          createdAt: event.payload.createdAt,
        });
      });
    let providerInvocationStarted = false;
    const claimReviewRepairInvocation = reviewRepairOwnership
      ? () =>
          sql
            .withTransaction(
              sql<{
                requestId: string;
              }>`INSERT INTO main.agent_control_epic_review_repair_delivery_claims(
                request_id,attempt,claimed_at)
                SELECT intent.request_id,intent.attempt,${event.payload.createdAt}
                FROM main.agent_control_epic_review_repair_intents intent
                JOIN main.agent_control_epic_review_requests request
                  ON request.request_id=intent.request_id
                JOIN main.agent_control_epic_runs run
                  ON run.epic_run_id=request.epic_run_id AND run.project_id=request.project_id
                JOIN main.agent_control_project_states project
                  ON project.project_id=request.project_id
                LEFT JOIN main.agent_control_epic_review_repair_cancellations cancellation
                  ON cancellation.request_id=intent.request_id AND cancellation.attempt=intent.attempt
                LEFT JOIN main.agent_control_epic_review_repair_results result
                  ON result.request_id=intent.request_id AND result.attempt=intent.attempt
                WHERE intent.request_id=${reviewRepairOwnership.requestId}
                  AND intent.attempt=${reviewRepairOwnership.attempt}
                  AND intent.turn_request_command_id=${event.commandId}
                  AND intent.thread_id=${event.payload.threadId}
                  AND intent.message_id=${event.payload.messageId}
                  AND cancellation.request_id IS NULL AND result.request_id IS NULL
                  AND json_extract(run.state_json,'$.status')='verifying'
                  AND json_extract(run.state_json,'$.activeReviewReworkId')=intent.request_id
                  AND project.mode IN ('armed','run-once') AND project.paused_from_mode IS NULL
                  AND EXISTS (
                    SELECT 1 FROM json_each(json_extract(run.state_json,'$.reviewReworks')) rework
                    WHERE json_extract(rework.value,'$.requestId')=intent.request_id
                      AND json_extract(rework.value,'$.status') IN ('accepted','repairing')
                  )
                RETURNING request_id AS "requestId"`,
            )
            .pipe(
              Effect.filterOrFail(
                (claimed) => claimed.length === 1,
                () =>
                  new ProviderAdapterRequestError({
                    provider: "review-repair",
                    method: "thread.turn.start",
                    detail:
                      "Epic review repair lost authority before the provider invocation boundary.",
                  }),
              ),
              Effect.asVoid,
              Effect.mapError((cause) =>
                isProviderAdapterRequestError(cause)
                  ? cause
                  : new ProviderAdapterRequestError({
                      provider: "review-repair",
                      method: "thread.turn.start",
                      detail: "Epic review repair delivery authority could not be persisted.",
                      cause,
                    }),
              ),
            )
      : undefined;
    const start = Effect.gen(function* () {
      const permit = yield* Effect.uninterruptibleMask((restore) =>
        Effect.flatMap(
          restore(
            resourceCoordinator.acquire({
              idempotencyKey: reviewRepairOwnership
                ? `automatic:epic-review:${reviewRepairOwnership.requestId}:${reviewRepairOwnership.attempt}`
                : `manual:${key}`,
              providerInstanceId: instanceId,
              // Provider instances are not assumed to be separate paid accounts.
              // Settings may explicitly split or join these conservative driver scopes.
              continuationKey: String(instanceInfo.value.driverKind),
              threadId: String(event.payload.threadId),
              requestedAt: event.payload.createdAt,
              workloadClass: reviewRepairOwnership ? "background" : "interactive",
              source: reviewRepairOwnership ? "automatic" : "manual",
              ...(reviewRepairOwnership
                ? { stage: "implementation" as const, handoffId: reviewRepairOwnership.requestId }
                : {}),
              onWait: (wait) => setAdmissionWait(wait).pipe(Effect.ignore),
            }),
          ),
          (acquired) =>
            Effect.sync(() => pendingManualPermits.set(event.payload.threadId, acquired)).pipe(
              Effect.as(acquired),
            ),
        ),
      );
      yield* setAdmissionWait(undefined);
      yield* resourceCoordinator.enter(permit);
      const sendTurnWithInvocationBoundary = providerService.sendTurnWithInvocationBoundary;
      if (reviewRepairOwnership && sendTurnWithInvocationBoundary === undefined)
        return yield* new ProviderAdapterRequestError({
          provider: "review-repair",
          method: "thread.turn.start",
          detail: "The durable provider invocation boundary is unavailable.",
        });
      const result = yield* sendTurnWithInvocationBoundary === undefined
        ? Effect.sync(() => {
            // Older injected services cannot expose the boundary. Preserve
            // fail-closed behavior for those implementations.
            providerInvocationStarted = true;
            return providerService.sendTurn(request);
          }).pipe(Effect.flatten)
        : sendTurnWithInvocationBoundary(request, {
            ...(claimReviewRepairInvocation
              ? { beforeInvocation: claimReviewRepairInvocation }
              : {}),
            onInvocationStarted: () => {
              providerInvocationStarted = true;
            },
          });
      if (reviewRepairOwnership) {
        yield* sql`INSERT INTO main.agent_control_epic_review_repair_delivery_receipts(
          request_id,attempt,provider_turn_id,accepted_at)
          VALUES (${reviewRepairOwnership.requestId},${reviewRepairOwnership.attempt},
            ${String(result.turnId)},${event.payload.createdAt})
          ON CONFLICT(request_id,attempt) DO NOTHING`;
        const receipt = yield* sql<{
          providerTurnId: string;
        }>`SELECT provider_turn_id AS "providerTurnId"
          FROM main.agent_control_epic_review_repair_delivery_receipts
          WHERE request_id=${reviewRepairOwnership.requestId}
            AND attempt=${reviewRepairOwnership.attempt}`;
        if (receipt.length !== 1 || receipt[0]!.providerTurnId !== String(result.turnId))
          return yield* Effect.die(
            new Error("Epic review repair provider receipt conflicts with its accepted turn."),
          );
      }
      yield* resourceCoordinator.enter(permit, String(result.turnId));
    }).pipe(
      Effect.onError(() => {
        const permit = pendingManualPermits.get(event.payload.threadId);
        if (permit === undefined || providerInvocationStarted) return Effect.void;
        return resourceCoordinator.release(permit).pipe(Effect.ignore);
      }),
      Effect.onInterrupt(() =>
        providerService.listSessions().pipe(
          Effect.flatMap((sessions) => {
            const active = sessions.find(
              (candidate) => candidate.threadId === event.payload.threadId,
            );
            const pending = pendingManualPermits.get(event.payload.threadId);
            if (pending === undefined) return Effect.void;
            if (active?.activeTurnId !== undefined)
              return resourceCoordinator
                .enter(pending, String(active.activeTurnId))
                .pipe(Effect.ignore);
            return providerInvocationStarted
              ? Effect.void
              : resourceCoordinator.release(pending).pipe(Effect.ignore);
          }),
          Effect.catch(() => Effect.void),
        ),
      ),
      Effect.asVoid,
      Effect.catchCause(recoverTurnStartFailure),
      Effect.ensuring(
        Effect.sync(() => {
          pendingManualStarts.delete(event.payload.threadId);
          pendingManualPermits.delete(event.payload.threadId);
        }),
      ),
    );
    const gate = yield* Deferred.make<void>();
    const fiber = yield* Deferred.await(gate).pipe(Effect.andThen(start), Effect.forkScoped);
    pendingManualStarts.set(event.payload.threadId, fiber);
    yield* Deferred.succeed(gate, undefined);
  });

  const processTurnInterruptRequested = Effect.fn("processTurnInterruptRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.turn-interrupt-requested" }>,
  ) {
    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }
    const session = thread.session;
    if (!session || session.status === "stopped") {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.interrupt.failed",
        summary: "Provider turn interrupt failed",
        detail: "No active provider session is bound to this thread.",
        turnId: event.payload.turnId ?? null,
        createdAt: event.payload.createdAt,
      });
    }

    const pendingStart = pendingManualStarts.get(event.payload.threadId);
    if (pendingStart !== undefined) {
      pendingManualStarts.delete(event.payload.threadId);
      yield* Fiber.interrupt(pendingStart);
      const active = yield* providerService.listSessions().pipe(
        Effect.map((sessions) =>
          sessions.find((candidate) => candidate.threadId === event.payload.threadId),
        ),
        Effect.catch(() => Effect.succeed(undefined)),
      );
      if (active?.activeTurnId === undefined && active?.status !== "running") {
        const { admissionWait: _admissionWait, ...sessionWithoutAdmissionWait } = session;
        yield* setThreadSession({
          threadId: event.payload.threadId,
          session: {
            ...sessionWithoutAdmissionWait,
            status: "ready",
            activeTurnId: null,
            lastError: null,
            updatedAt: event.payload.createdAt,
          },
          createdAt: event.payload.createdAt,
        });
        return;
      }
    }

    const recoverInterruptFailure = (cause: Cause.Cause<unknown>) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.interrupt;
      }

      const detail = formatFailureDetail(cause);
      return Effect.gen(function* () {
        const latestThread = yield* resolveThreadShell(event.payload.threadId);
        const latestSession = latestThread?.session;
        if (
          !latestSession ||
          latestSession.status === "stopped" ||
          latestSession.status === "ready" ||
          (event.payload.turnId !== undefined &&
            latestSession.activeTurnId !== null &&
            latestSession.activeTurnId !== event.payload.turnId)
        ) {
          return;
        }

        yield* providerService.stopSession({ threadId: event.payload.threadId }).pipe(
          Effect.catchCause((stopCause) => {
            if (Cause.hasInterruptsOnly(stopCause)) {
              return Effect.interrupt;
            }
            return Effect.logWarning(
              "provider command reactor failed to stop session after interrupt failure",
              {
                threadId: event.payload.threadId,
                cause: Cause.pretty(stopCause),
                originalCause: Cause.pretty(cause),
              },
            );
          }),
        );
        const stoppedThread = yield* resolveThreadShell(event.payload.threadId);
        const stoppedSession = stoppedThread?.session;
        if (
          !stoppedSession ||
          stoppedSession.status === "stopped" ||
          stoppedSession.status === "ready" ||
          (event.payload.turnId !== undefined &&
            stoppedSession.activeTurnId !== null &&
            stoppedSession.activeTurnId !== event.payload.turnId)
        ) {
          return;
        }

        yield* setThreadSession({
          threadId: event.payload.threadId,
          session: {
            ...stoppedSession,
            status: "stopped",
            activeTurnId: null,
            lastError: detail,
            updatedAt: event.payload.createdAt,
          },
          createdAt: event.payload.createdAt,
        });
        yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.turn.interrupt.failed",
          summary: "Provider turn interrupt failed",
          detail,
          turnId: event.payload.turnId ?? null,
          createdAt: event.payload.createdAt,
        });
      });
    };

    // Orchestration turn ids are not provider turn ids, so interrupt by session.
    yield* providerService
      .interruptTurn({ threadId: event.payload.threadId })
      .pipe(Effect.catchCause(recoverInterruptFailure));
  });

  const processApprovalResponseRequested = Effect.fn("processApprovalResponseRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.approval-response-requested" }>,
  ) {
    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }
    const hasSession = thread.session && thread.session.status !== "stopped";
    if (!hasSession) {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.approval.respond.failed",
        summary: "Provider approval response failed",
        detail: "No active provider session is bound to this thread.",
        turnId: null,
        createdAt: event.payload.createdAt,
        requestId: event.payload.requestId,
      });
    }

    yield* providerService
      .respondToRequest({
        threadId: event.payload.threadId,
        requestId: event.payload.requestId,
        decision: event.payload.decision,
      })
      .pipe(
        Effect.catchCause((cause) =>
          appendProviderFailureActivity({
            threadId: event.payload.threadId,
            kind: "provider.approval.respond.failed",
            summary: "Provider approval response failed",
            detail: isUnknownPendingApprovalRequestError(cause)
              ? stalePendingRequestDetail("approval", event.payload.requestId)
              : Cause.pretty(cause),
            turnId: null,
            createdAt: event.payload.createdAt,
            requestId: event.payload.requestId,
          }),
        ),
      );
  });

  const processUserInputResponseRequested = Effect.fn("processUserInputResponseRequested")(
    function* (
      event: Extract<ProviderIntentEvent, { type: "thread.user-input-response-requested" }>,
    ) {
      const thread = yield* resolveThreadShell(event.payload.threadId);
      if (!thread) {
        return;
      }
      const hasSession = thread.session && thread.session.status !== "stopped";
      if (!hasSession) {
        return yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.user-input.respond.failed",
          summary: "Provider user input response failed",
          detail: "No active provider session is bound to this thread.",
          turnId: null,
          createdAt: event.payload.createdAt,
          requestId: event.payload.requestId,
        });
      }

      yield* providerService
        .respondToUserInput({
          threadId: event.payload.threadId,
          requestId: event.payload.requestId,
          answers: event.payload.answers,
          ...(event.payload.attachmentsByQuestionId
            ? { attachmentsByQuestionId: event.payload.attachmentsByQuestionId }
            : {}),
        })
        .pipe(
          Effect.catchCause((cause) =>
            appendProviderFailureActivity({
              threadId: event.payload.threadId,
              kind: "provider.user-input.respond.failed",
              summary: "Provider user input response failed",
              detail: isUnknownPendingUserInputRequestError(cause)
                ? stalePendingRequestDetail("user-input", event.payload.requestId)
                : Cause.pretty(cause),
              turnId: null,
              createdAt: event.payload.createdAt,
              requestId: event.payload.requestId,
            }),
          ),
        );
    },
  );

  const processSessionStopRequested = Effect.fn("processSessionStopRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.session-stop-requested" }>,
  ) {
    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }

    const now = event.payload.createdAt;
    const wasCompacting = compactingThreadIds.has(thread.id);
    stoppingThreadIds.add(thread.id);
    const clearStopping = Effect.sync(() => void stoppingThreadIds.delete(thread.id));
    yield* (
      thread.session && thread.session.status !== "stopped"
        ? providerService.stopSession({ threadId: thread.id })
        : Effect.void
    ).pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.interrupt;
          }
          const detail = formatFailureDetail(cause);
          return Effect.sync(() => {
            stoppingThreadIds.delete(thread.id);
            return wasCompacting && !compactingThreadIds.has(thread.id);
          }).pipe(
            Effect.flatMap((compactionSettled) =>
              compactionSettled ? restoreCompaction(thread.id) : Effect.void,
            ),
            Effect.andThen(
              appendProviderFailureActivity({
                threadId: thread.id,
                kind: "provider.session.stop.failed",
                summary: "Provider session stop failed",
                detail,
                turnId: null,
                createdAt: now,
              }),
            ),
          );
        },
        onSuccess: () =>
          setThreadSession({
            threadId: thread.id,
            session: {
              threadId: thread.id,
              status: "stopped",
              providerName: thread.session?.providerName ?? null,
              ...(thread.session?.providerInstanceId !== undefined
                ? { providerInstanceId: thread.session.providerInstanceId }
                : {}),
              runtimeMode: thread.session?.runtimeMode ?? DEFAULT_RUNTIME_MODE,
              activeTurnId: null,
              lastError: thread.session?.lastError ?? null,
              updatedAt: now,
            },
            createdAt: now,
          }),
      }),
      Effect.ensuring(clearStopping),
    );
  });

  const processDomainEvent = Effect.fn("processDomainEvent")(function* (
    event: ProviderIntentEvent,
  ) {
    yield* Effect.annotateCurrentSpan({
      "orchestration.event_type": event.type,
      "orchestration.thread_id": event.payload.threadId,
      ...(event.commandId ? { "orchestration.command_id": event.commandId } : {}),
    });
    yield* increment(orchestrationEventsProcessedTotal, {
      eventType: event.type,
    });
    switch (event.type) {
      case "thread.meta-updated":
        yield* threadTitleRegenerationWorker.enqueue(event);
        return;
      case "thread.runtime-mode-set": {
        const thread = yield* resolveThreadShell(event.payload.threadId);
        if (!thread?.session || thread.session.status === "stopped") {
          return;
        }
        yield* turnRequestExecutor.ensureSessionForThread(event.payload.threadId, event.occurredAt);
        return;
      }
      case "thread.turn-start-requested":
        yield* processTurnStartRequested(event);
        return;
      case "thread.turn-interrupt-requested":
        yield* processTurnInterruptRequested(event);
        return;
      case "thread.approval-response-requested":
        yield* processApprovalResponseRequested(event);
        return;
      case "thread.user-input-response-requested":
        yield* processUserInputResponseRequested(event);
        return;
      case "thread.session-stop-requested":
        yield* processSessionStopRequested(event);
        return;
      case "thread.settled": {
        const thread = yield* projectionSnapshotQuery.getThreadShellById(event.payload.threadId);
        if (
          Option.isNone(thread) ||
          thread.value.session == null ||
          thread.value.session.status === "stopped"
        ) {
          return;
        }
        yield* orchestrationEngine.dispatch({
          type: "thread.session.stop",
          commandId: CommandId.make(`session-stop-for-settle:${event.commandId ?? event.eventId}`),
          threadId: event.payload.threadId,
          createdAt: event.occurredAt,
          onlyIfSettled: true,
        });
        return;
      }
    }
  });

  const processDomainEventSafely = (event: ProviderIntentEvent) =>
    processDomainEvent(event).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logWarning("provider command reactor failed to process event", {
          eventType: event.type,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processDomainEventSafely);

  const recoverEpicReviewRepairDeliveries = Effect.fn(
    "ProviderCommandReactor.recoverEpicReviewRepairDeliveries",
  )(function* () {
    const recoveredAt = DateTime.formatIso(yield* DateTime.now);
    const rows = yield* sql<{
      requestId: string;
      attempt: number;
      threadId: string;
      messageId: string;
      turnRequestCommandId: string;
      providerInstanceId: string;
      claimedAt: string | null;
      cancellationAt: string | null;
      resultStatus: string | null;
      turnRequestEventSequence: number | null;
      receiptTurnId: string | null;
      projectedTurnId: string | null;
      turnState: string | null;
    }>`SELECT intent.request_id AS "requestId",intent.attempt,
      intent.thread_id AS "threadId",intent.message_id AS "messageId",
      intent.turn_request_command_id AS "turnRequestCommandId",
      intent.provider_instance_id AS "providerInstanceId",
      claim.claimed_at AS "claimedAt",
      cancellation.cancelled_at AS "cancellationAt",
      json_extract(result.result_json,'$.status') AS "resultStatus",
      command_receipt.result_sequence AS "turnRequestEventSequence",
      receipt.provider_turn_id AS "receiptTurnId",turn.turn_id AS "projectedTurnId",
      turn.state AS "turnState"
      FROM main.agent_control_epic_review_repair_intents intent
      LEFT JOIN main.agent_control_epic_review_repair_delivery_claims claim
        ON claim.request_id=intent.request_id AND claim.attempt=intent.attempt
      LEFT JOIN main.agent_control_epic_review_repair_delivery_receipts receipt
        ON receipt.request_id=intent.request_id AND receipt.attempt=intent.attempt
      LEFT JOIN main.agent_control_epic_review_repair_cancellations cancellation
        ON cancellation.request_id=intent.request_id AND cancellation.attempt=intent.attempt
      LEFT JOIN main.agent_control_epic_review_repair_results result
        ON result.request_id=intent.request_id AND result.attempt=intent.attempt
      LEFT JOIN main.orchestration_command_receipts command_receipt
        ON command_receipt.command_id=intent.turn_request_command_id
        AND command_receipt.status='accepted'
      LEFT JOIN main.projection_turns turn
        ON turn.thread_id=intent.thread_id AND turn.pending_message_id=intent.message_id
      WHERE (result.request_id IS NULL OR json_extract(result.result_json,'$.status')='blocked')
      ORDER BY intent.request_id,intent.attempt`;
    const sessions = yield* providerService.listSessions();
    yield* Effect.forEach(
      rows,
      (row) =>
        Effect.gen(function* () {
          if (row.claimedAt === null && row.cancellationAt === null) {
            if (row.resultStatus !== null || row.turnRequestEventSequence === null) return;
            const retainedEvent = yield* Stream.runHead(
              orchestrationEngine.readEvents(row.turnRequestEventSequence - 1, 1),
            );
            if (
              Option.isSome(retainedEvent) &&
              retainedEvent.value.sequence === row.turnRequestEventSequence &&
              retainedEvent.value.type === "thread.turn-start-requested" &&
              retainedEvent.value.commandId === row.turnRequestCommandId &&
              retainedEvent.value.payload.threadId === row.threadId &&
              retainedEvent.value.payload.messageId === row.messageId
            ) {
              // Accepted command receipts are idempotent and do not republish
              // their hot event. Re-enter the normal worker so its durable
              // claim remains the sole provider-invocation boundary.
              yield* worker.enqueue(retainedEvent.value);
            }
            return;
          }
          const activeSession = sessions.find(
            (session) =>
              String(session.threadId) === row.threadId && session.activeTurnId !== undefined,
          );
          const retainReceipt = Effect.fn("ProviderCommandReactor.retainRecoveredReviewReceipt")(
            function* (providerTurnId: string) {
              yield* sql`INSERT INTO main.agent_control_epic_review_repair_delivery_receipts(
                request_id,attempt,provider_turn_id,accepted_at)
                VALUES (${row.requestId},${row.attempt},${providerTurnId},${recoveredAt})
                ON CONFLICT(request_id,attempt) DO NOTHING`;
              const retained = yield* sql<{
                providerTurnId: string;
              }>`SELECT provider_turn_id AS "providerTurnId"
                FROM main.agent_control_epic_review_repair_delivery_receipts
                WHERE request_id=${row.requestId} AND attempt=${row.attempt}`;
              if (retained.length !== 1 || retained[0]!.providerTurnId !== providerTurnId)
                return yield* Effect.die(
                  new Error("Recovered Epic review repair turn conflicts with its receipt."),
                );
              return providerTurnId;
            },
          );
          let acceptedTurnId = row.receiptTurnId;
          if (acceptedTurnId === null && row.projectedTurnId !== null)
            acceptedTurnId = yield* retainReceipt(row.projectedTurnId);
          // Review repair threads are sealed to their one immutable command.
          // Before any terminal result exists, a live turn on the claimed
          // provider instance is therefore the adapter-accepted review turn
          // whose post-invocation receipt was lost to the crash.
          if (
            acceptedTurnId === null &&
            row.resultStatus === null &&
            row.claimedAt !== null &&
            activeSession?.providerInstanceId === row.providerInstanceId
          )
            acceptedTurnId = yield* retainReceipt(String(activeSession.activeTurnId!));
          const hasExactActiveSession =
            acceptedTurnId !== null &&
            activeSession !== undefined &&
            activeSession.providerInstanceId === row.providerInstanceId &&
            String(activeSession.activeTurnId) === acceptedTurnId;
          const retireOrphaned = () => {
            const retire = Option.isSome(providerResourceCoordinator)
              ? providerResourceCoordinator.value.retireOrphaned
              : undefined;
            if (retire === undefined)
              return Effect.die(new Error("Shared provider orphan retirement is unavailable."));
            return retire(`automatic:epic-review:${row.requestId}:${row.attempt}`);
          };
          const bindOrphaned = () => {
            const bind = Option.isSome(providerResourceCoordinator)
              ? providerResourceCoordinator.value.bindOrphaned
              : undefined;
            if (bind === undefined) return Effect.void;
            return bind(
              `automatic:epic-review:${row.requestId}:${row.attempt}`,
              acceptedTurnId!,
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning(
                  "Epic review recovery could not bind retained capacity to its proven turn",
                  { threadId: row.threadId, cause: Cause.pretty(cause) },
                ),
              ),
            );
          };
          const interrupt = () =>
            orchestrationEngine.dispatch({
              type: "thread.turn.interrupt",
              commandId: CommandId.make(`epic-review-interrupt:${row.requestId}:${row.attempt}`),
              threadId: ThreadId.make(row.threadId),
              createdAt: row.cancellationAt ?? row.claimedAt ?? recoveredAt,
            });
          const interruptProvider = () =>
            !hasExactActiveSession || activeSession === undefined
              ? Effect.void
              : providerService.interruptTurn({ threadId: ThreadId.make(row.threadId) }).pipe(
                  Effect.catchCause((interruptCause) =>
                    providerService.stopSession({ threadId: ThreadId.make(row.threadId) }).pipe(
                      Effect.catchCause((stopCause) =>
                        Effect.logWarning(
                          "Epic review recovery could not stop the ambiguous provider session",
                          {
                            threadId: row.threadId,
                            interruptCause: Cause.pretty(interruptCause),
                            stopCause: Cause.pretty(stopCause),
                          },
                        ),
                      ),
                    ),
                  ),
                );
          const recordInterrupt = () =>
            hasExactActiveSession
              ? interrupt().pipe(
                  Effect.catchCause((cause) =>
                    Effect.logWarning(
                      "Epic review recovery could not retain its interrupt command",
                      {
                        threadId: row.threadId,
                        cause: Cause.pretty(cause),
                      },
                    ),
                  ),
                )
              : Effect.void;
          const terminalProjection =
            row.turnState === "completed" ||
            row.turnState === "error" ||
            row.turnState === "interrupted";
          if (hasExactActiveSession) yield* bindOrphaned();
          if (row.cancellationAt !== null) {
            yield* interruptProvider();
            yield* recordInterrupt();
            if (
              !hasExactActiveSession &&
              (terminalProjection || (row.claimedAt === null && acceptedTurnId === null))
            )
              yield* retireOrphaned();
            return;
          }
          if (acceptedTurnId !== null) {
            if (hasExactActiveSession) return;
            if (terminalProjection) {
              yield* retireOrphaned();
              return;
            }
          }
          const result = {
            status: "blocked" as const,
            candidateCommitSha: null,
            code: "review-repair-delivery-ambiguous",
            message:
              "The server restarted without terminal proof for the authorized provider turn. The attempt is fenced and requires explicit recovery before retrying.",
          };
          const resultJson = canonicalJson(result);
          yield* interruptProvider();
          yield* recordInterrupt();
          yield* sql`INSERT INTO main.agent_control_epic_review_repair_results(
            request_id,attempt,result_json,result_digest,completed_at)
            VALUES (${row.requestId},${row.attempt},${resultJson},${sha256Utf8(resultJson)},${recoveredAt})
            ON CONFLICT(request_id,attempt) DO NOTHING`;
          if (activeSession === undefined)
            yield* setThreadSessionErrorOnTurnStartFailure({
              threadId: ThreadId.make(row.threadId),
              detail: result.message,
              createdAt: recoveredAt,
            });
        }),
      { concurrency: 1, discard: true },
    );
  });

  const start: ProviderCommandReactorShape["start"] = Effect.fn("start")(function* () {
    const interruptedTitleRegenerations = yield* findInterruptedThreadTitleRegenerations().pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logWarning(
          "provider command reactor failed to find interrupted title regenerations",
          { cause: Cause.pretty(cause) },
        ).pipe(Effect.as([]));
      }),
    );
    const processEvent = Effect.fn("processEvent")(function* (event: OrchestrationEvent) {
      if (
        (event.type === "thread.meta-updated" && event.payload.regenerateTitle === true) ||
        event.type === "thread.runtime-mode-set" ||
        event.type === "thread.turn-start-requested" ||
        event.type === "thread.turn-interrupt-requested" ||
        event.type === "thread.approval-response-requested" ||
        event.type === "thread.user-input-response-requested" ||
        event.type === "thread.session-stop-requested" ||
        event.type === "thread.settled"
      ) {
        return yield* worker.enqueue(event);
      }
    });

    // Acquire the hot subscription before recovery so events arriving during
    // the scan are retained. Consume it only after the snapshot is classified;
    // otherwise a fresh claim can look like a pre-restart orphan.
    const domainEvents = yield* orchestrationEngine.subscribeDomainEvents;
    yield* Effect.addFinalizer(() => hooks.onDomainEventSubscriptionRelease?.() ?? Effect.void);
    yield* hooks.afterDomainEventSubscription?.() ?? Effect.void;
    yield* recoverEpicReviewRepairDeliveries().pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Effect.logWarning("provider command reactor could not recover Epic review repair", {
              cause: Cause.pretty(cause),
            }),
      ),
    );
    yield* forkParked(Stream.runForEach(domainEvents, processEvent));

    // The domain event stream is hot, so work pending before this reactor
    // starts cannot be resumed. Correlated completions only clear the request
    // captured here, leaving any newer request untouched.
    const clearInterrupted = clearInterruptedThreadTitleRegenerations(
      interruptedTitleRegenerations,
    ).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logWarning(
          "provider command reactor failed to clear interrupted title regenerations",
          {
            cause: Cause.pretty(cause),
          },
        );
      }),
    );
    const activation = yield* ServerActivation;
    if (activation === undefined) {
      yield* clearInterrupted;
    } else {
      yield* forkParked(clearInterrupted);
    }
  });

  return {
    start,
    drain: Effect.gen(function* () {
      yield* worker.drain;
      yield* threadTitleRegenerationWorker.drain;
    }),
  } satisfies ProviderCommandReactorShape;
});

export const ProviderCommandReactorCore = Layer.effect(ProviderCommandReactor, make);

export const ProviderCommandReactorLiveWithHooks = ProviderCommandReactorCore.pipe(
  Layer.provideMerge(ProviderTurnRequestExecutorLive),
  Layer.provideMerge(AgentControlInitialPlanningHandoffStoreLive),
);

export const ProviderCommandReactorLive = ProviderCommandReactorLiveWithHooks;
