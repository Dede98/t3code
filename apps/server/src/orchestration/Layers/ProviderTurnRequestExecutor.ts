import {
  CommandId,
  type ModelSelection,
  type OrchestrationSession,
  type ProviderSession,
  ProviderDriverKind,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { resolveThreadWorkspaceCwd } from "../../checkpointing/Utils.ts";
import { ProviderAdapterRequestError } from "../../provider/Errors.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  ProviderTurnRequestExecutor,
  type ProviderTurnRequestExecutorShape,
} from "../Services/ProviderTurnRequestExecutor.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";

const isProviderDriverKind = Schema.is(ProviderDriverKind);

export function providerErrorLabel(value: string | undefined): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : "unknown";
}

export function providerErrorLabelFromInstanceHint(input: { readonly instanceId: string }): string {
  return providerErrorLabel(input.instanceId);
}

const mapProviderSessionStatusToOrchestrationStatus = (
  status: "connecting" | "ready" | "running" | "error" | "closed",
): OrchestrationSession["status"] => {
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
};

const toNonEmptyProviderInput = (value: string | undefined): string | undefined => {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : undefined;
};

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerService = yield* ProviderService;
  const providerRegistry = yield* ProviderRegistry;
  const threadModelSelections = new Map<string, ModelSelection>();
  const serverCommandId = (tag: string) =>
    crypto.randomUUIDv4.pipe(
      Effect.orDie,
      Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)),
    );

  const resolveProject = Effect.fnUntraced(function* (projectId: ProjectId) {
    return yield* projectionSnapshotQuery
      .getProjectShellById(projectId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const resolveThread = Effect.fnUntraced(function* (threadId: ThreadId) {
    return yield* projectionSnapshotQuery
      .getThreadDetailById(threadId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

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
    if (!requiresNewThread) return;
    return yield* new ProviderAdapterRequestError({
      provider: providerErrorLabelFromInstanceHint({
        instanceId: String(requestedModelSelection.instanceId),
      }),
      method: "thread.turn.start",
      detail: `Thread '${input.threadId}' cannot switch models after the conversation has started. Start a new thread to use '${requestedModelSelection.model}'.`,
    });
  });

  const ensureSessionForThread: ProviderTurnRequestExecutorShape["ensureSessionForThread"] =
    Effect.fn("ProviderTurnRequestExecutor.ensureSessionForThread")(
      function* (threadId, createdAt, options) {
        const thread = yield* resolveThread(threadId);
        if (!thread) {
          return yield* Effect.die(new Error(`Thread '${threadId}' was not found in read model.`));
        }

        const desiredRuntimeMode = thread.runtimeMode;
        const requestedModelSelection = options?.modelSelection;
        const resolveActiveSession = (threadId: ThreadId) =>
          providerService
            .listSessions()
            .pipe(
              Effect.map((sessions) => sessions.find((session) => session.threadId === threadId)),
            );
        const activeSession = yield* resolveActiveSession(threadId);
        const activeThreadSession =
          thread.session !== null && thread.session.status !== "stopped" && activeSession
            ? thread.session
            : null;
        let currentInstanceId = thread.modelSelection.instanceId;
        if (activeThreadSession !== null && activeSession !== undefined) {
          if (
            activeThreadSession.providerInstanceId === undefined ||
            activeSession.providerInstanceId === undefined
          ) {
            return yield* new ProviderAdapterRequestError({
              provider: providerErrorLabel(activeThreadSession.providerName ?? undefined),
              method: "thread.turn.start",
              detail: `Thread '${threadId}' has an active provider session without a provider instance id.`,
            });
          }
          currentInstanceId = activeSession.providerInstanceId;
        }
        const desiredModelSelection = requestedModelSelection ?? thread.modelSelection;
        const desiredInstanceId = desiredModelSelection.instanceId;
        const currentInfo = yield* providerService.getInstanceInfo(currentInstanceId).pipe(
          Effect.mapError(
            () =>
              new ProviderAdapterRequestError({
                provider: providerErrorLabelFromInstanceHint({
                  instanceId: String(currentInstanceId),
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
        if (thread.session !== null) {
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
          thread.session !== null &&
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
            thread.session.activeTurnId !== null ||
            thread.session.status === "starting" ||
            thread.session.status === "running"
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
        const startProviderSession = (input?: { readonly resumeCursor?: unknown }) =>
          providerService.startSession(threadId, {
            threadId,
            provider: preferredProvider,
            providerInstanceId: desiredInstanceId,
            ...(effectiveCwd ? { cwd: effectiveCwd } : {}),
            modelSelection: desiredModelSelection,
            ...(input?.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
            runtimeMode: desiredRuntimeMode,
          });
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
                status: mapProviderSessionStatusToOrchestrationStatus(session.status),
                providerName: session.provider,
                providerInstanceId: session.providerInstanceId,
                runtimeMode: desiredRuntimeMode,
                activeTurnId: null,
                lastError: session.lastError ?? null,
                updatedAt: session.updatedAt,
              },
              createdAt,
            });
          });
        const existingSessionThreadId =
          thread.session && thread.session.status !== "stopped" && activeSession ? thread.id : null;
        if (existingSessionThreadId === null && activeSession !== undefined) {
          if (
            activeSession.status === "connecting" ||
            activeSession.status === "running" ||
            activeSession.activeTurnId !== undefined
          ) {
            return yield* new ProviderAdapterRequestError({
              provider: providerErrorLabel(activeSession.provider),
              method: "thread.turn.start",
              detail: `Thread '${threadId}' has an unbound provider session with unresolved active work.`,
            });
          }
          if (activeSession.status === "ready") {
            const compatible =
              activeSession.providerInstanceId === desiredInstanceId &&
              activeSession.runtimeMode === desiredRuntimeMode &&
              activeSession.cwd === effectiveCwd &&
              activeSession.model === desiredModelSelection.model &&
              Equal.equals(thread.modelSelection, desiredModelSelection);
            if (!compatible) {
              return yield* new ProviderAdapterRequestError({
                provider: providerErrorLabel(activeSession.provider),
                method: "thread.turn.start",
                detail: `Thread '${threadId}' has an unbound provider session that conflicts with its persisted model, runtime, or workspace authority.`,
              });
            }
            yield* bindSessionToThread(activeSession);
            threadModelSelections.set(threadId, desiredModelSelection);
            return threadId;
          }
        }
        if (existingSessionThreadId) {
          const runtimeModeChanged = thread.runtimeMode !== thread.session?.runtimeMode;
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
            return existingSessionThreadId;
          }
          const resumeCursor = shouldRestartForModelChange
            ? undefined
            : (activeSession?.resumeCursor ?? undefined);
          const restartedSession = yield* startProviderSession(
            resumeCursor !== undefined ? { resumeCursor } : undefined,
          );
          yield* bindSessionToThread(restartedSession);
          return restartedSession.threadId;
        }
        const startedSession = yield* startProviderSession(undefined);
        yield* bindSessionToThread(startedSession);
        return startedSession.threadId;
      },
    );

  const execute: ProviderTurnRequestExecutorShape["execute"] = Effect.fn(
    "ProviderTurnRequestExecutor.execute",
  )(function* (input) {
    const thread = yield* resolveThread(input.threadId);
    if (!thread) {
      return yield* Effect.die(
        new Error(`Thread '${input.threadId}' was not found in read model.`),
      );
    }
    yield* ensureSessionForThread(
      input.threadId,
      input.createdAt,
      input.modelSelection === undefined ? {} : { modelSelection: input.modelSelection },
    );
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
    yield* Effect.annotateCurrentSpan(
      input.providerDeliveryId === undefined
        ? {}
        : { "provider.delivery_id": input.providerDeliveryId },
    );
    return yield* providerService.sendTurn({
      threadId: input.threadId,
      ...(normalizedInput ? { input: normalizedInput } : {}),
      ...(normalizedAttachments.length > 0 ? { attachments: normalizedAttachments } : {}),
      ...(modelForTurn !== undefined ? { modelSelection: modelForTurn } : {}),
      ...(input.interactionMode !== undefined ? { interactionMode: input.interactionMode } : {}),
    });
  });

  return ProviderTurnRequestExecutor.of({
    ensureSessionForThread,
    execute,
  });
});

export const ProviderTurnRequestExecutorLive = Layer.effect(ProviderTurnRequestExecutor, make);
