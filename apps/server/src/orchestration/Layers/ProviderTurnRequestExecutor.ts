import {
  CommandId,
  ModelSelection,
  type OrchestrationSession,
  type ProviderSession,
  ProviderDriverKind,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { decodeCanonicalUtf8Bytes } from "../../agentControl/initialPlanning/eventEvidence.ts";
import type { ProviderAdmissionPermit } from "../../agentControl/providerAdmission/model.ts";
import { resolveThreadWorkspaceCwd } from "../../checkpointing/Utils.ts";
import { ProviderAdapterRequestError } from "../../provider/Errors.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import {
  canonicalProviderModelSelectionEvidence,
  type ProviderSessionAttestation,
} from "../../provider/Services/ProviderAdapter.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  ProviderTurnRequestExecutor,
  ProviderTurnDeliveryError,
  type ProviderTurnRequestExecutorShape,
} from "../Services/ProviderTurnRequestExecutor.ts";
import { ProviderTurnRequestExecutorHooks } from "../Services/ProviderTurnRequestExecutorHooks.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";

const isProviderDriverKind = Schema.is(ProviderDriverKind);

export function providerErrorLabel(value: string | undefined): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : "unknown";
}

export function providerErrorLabelFromInstanceHint(input: { readonly instanceId: string }): string {
  return providerErrorLabel(input.instanceId);
}

export function mapProviderTurnDeliveryCause(
  cause: Cause.Cause<unknown>,
  certainty: ProviderTurnDeliveryError["certainty"],
): Cause.Cause<ProviderTurnDeliveryError> {
  return Cause.fromReasons(
    cause.reasons.map((reason) =>
      Cause.isFailReason(reason)
        ? Cause.makeFailReason(
            new ProviderTurnDeliveryError({
              certainty,
              cause: reason.error,
            }),
          ).annotate(Context.makeUnsafe(reason.annotations))
        : reason,
    ),
  );
}

export interface InitialPlanningSessionEvidenceRow {
  readonly providerDeliveryId: string;
  readonly threadId: string;
  readonly providerInstanceId: string;
  readonly runtimeMode: string;
  readonly cwd: string;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly sessionCreatedAt: string;
  readonly resumeCursorJson: string;
}

export const buildInitialPlanningSessionEvidence = (input: {
  readonly providerDeliveryId: string;
  readonly attestation: ProviderSessionAttestation;
  readonly resumeCursorJson: string;
}): InitialPlanningSessionEvidenceRow => {
  const modelEvidence = canonicalProviderModelSelectionEvidence(
    input.attestation.effectiveModelSelection,
  );
  return {
    providerDeliveryId: input.providerDeliveryId,
    threadId: String(input.attestation.threadId),
    providerInstanceId: String(input.attestation.providerInstanceId),
    runtimeMode: input.attestation.runtimeMode,
    cwd: input.attestation.cwd,
    modelSelectionJson: modelEvidence.modelSelectionJson,
    modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
    sessionCreatedAt: input.attestation.sessionCreatedAt,
    resumeCursorJson: input.resumeCursorJson,
  };
};

export const isInitialPlanningSessionEvidenceRow = (
  row: InitialPlanningSessionEvidenceRow,
  expected: InitialPlanningSessionEvidenceRow,
): boolean =>
  row.providerDeliveryId === expected.providerDeliveryId &&
  row.threadId === expected.threadId &&
  row.providerInstanceId === expected.providerInstanceId &&
  row.runtimeMode === expected.runtimeMode &&
  row.cwd === expected.cwd &&
  row.modelSelectionJson === expected.modelSelectionJson &&
  row.modelSelectionFingerprint === expected.modelSelectionFingerprint &&
  row.sessionCreatedAt === expected.sessionCreatedAt &&
  row.resumeCursorJson === expected.resumeCursorJson;

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
  const hooks = yield* ProviderTurnRequestExecutorHooks;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerService = yield* ProviderService;
  const providerRegistry = yield* ProviderRegistry;
  const sql = yield* SqlClient.SqlClient;
  const threadModelSelections = new Map<string, ModelSelection>();
  const failAfterAdmissionQuarantine = <E>(
    permit: ProviderAdmissionPermit,
    cause: Cause.Cause<E>,
  ) =>
    Effect.gen(function* () {
      const quarantine = providerService.quarantineAdmissionIfEntered;
      const quarantineExit = yield* Effect.exit(
        Effect.uninterruptible(
          quarantine === undefined
            ? Effect.fail(
                new ProviderAdapterRequestError({
                  provider: String(permit.providerInstanceId),
                  method: "thread.turn.start",
                  detail: "Durable provider admission quarantine authority is unavailable.",
                }),
              )
            : quarantine(permit),
        ),
      );
      return yield* Effect.failCause(
        Exit.isFailure(quarantineExit) ? Cause.combine(cause, quarantineExit.cause) : cause,
      );
    });
  const serverCommandId = (tag: string) =>
    hooks.makeServerCommandId?.(tag) ??
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

  const ensureSessionForThreadRaw: ProviderTurnRequestExecutorShape["ensureSessionForThread"] =
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
          providerService.startSession(
            threadId,
            {
              threadId,
              provider: preferredProvider,
              providerInstanceId: desiredInstanceId,
              ...(effectiveCwd ? { cwd: effectiveCwd } : {}),
              modelSelection: desiredModelSelection,
              ...(input?.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
              runtimeMode: desiredRuntimeMode,
            },
            options?.providerAdmissionPermit === undefined
              ? undefined
              : { providerAdmissionPermit: options.providerAdmissionPermit },
          );
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
              createdAt: session.updatedAt,
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

  const ensureSessionForThread: ProviderTurnRequestExecutorShape["ensureSessionForThread"] = (
    threadId,
    createdAt,
    options,
  ) =>
    ensureSessionForThreadRaw(threadId, createdAt, options).pipe(
      Effect.catchCause((cause) =>
        options?.providerAdmissionPermit === undefined
          ? Effect.failCause(cause)
          : failAfterAdmissionQuarantine(options.providerAdmissionPermit, cause),
      ),
    );

  const encodeResumeCursorJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
  const decodeResumeCursorJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
  const sessionEvidenceError = (provider: string, detail: string) =>
    new ProviderAdapterRequestError({
      provider,
      method: "thread.turn.start",
      detail,
    });
  const prepareTurnDeliveryRaw: ProviderTurnRequestExecutorShape["prepareTurnDelivery"] = Effect.fn(
    "ProviderTurnRequestExecutor.prepareTurnDelivery",
  )(function* (input) {
    const thread = yield* resolveThread(input.threadId);
    if (!thread) {
      return yield* Effect.die(
        new Error(`Thread '${input.threadId}' was not found in read model.`),
      );
    }
    const durable =
      input.providerDeliveryId !== undefined || input.durableDeliveryKind !== undefined;
    const admissionPermit = input.providerAdmissionPermit;
    if (durable && admissionPermit === undefined) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabelFromInstanceHint({
          instanceId: String(input.modelSelection?.instanceId ?? thread.modelSelection.instanceId),
        }),
        method: "thread.turn.start",
        detail:
          "Automated durable provider delivery requires a committed capacity admission permit.",
      });
    }
    if (admissionPermit !== undefined) {
      const selection = input.modelSelection ?? thread.modelSelection;
      const modelEvidence = canonicalProviderModelSelectionEvidence(selection);
      if (
        input.providerDeliveryId !== admissionPermit.providerDeliveryId ||
        String(input.threadId) !== admissionPermit.threadId ||
        selection.instanceId !== admissionPermit.providerInstanceId ||
        modelEvidence.modelSelectionJson !== admissionPermit.modelSelectionJson ||
        modelEvidence.modelSelectionFingerprint !== admissionPermit.modelSelectionFingerprint ||
        input.durableDeliveryKind !== admissionPermit.stage
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: providerErrorLabelFromInstanceHint({
            instanceId: String(selection.instanceId),
          }),
          method: "thread.turn.start",
          detail: "Durable provider delivery conflicts with its capacity admission permit.",
        });
      }
    }
    let sessionResumeCursorJson: string | undefined;
    const sessionsBefore = yield* providerService.listSessions();
    const sessionBefore = sessionsBefore.find((session) => session.threadId === input.threadId);
    const attestationBefore =
      sessionBefore === undefined
        ? undefined
        : yield* (
            providerService.getSessionAttestation?.(input.threadId) ??
              Effect.as(Effect.void, undefined as ProviderSessionAttestation | undefined)
          );
    const durableDeliveryKind = input.durableDeliveryKind ?? "initial-planning";
    const durableStageLabel =
      durableDeliveryKind === "implementation"
        ? "Implementation"
        : durableDeliveryKind === "verification"
          ? "Verification"
          : "Initial Planning";
    const existingEvidence =
      input.providerDeliveryId === undefined
        ? []
        : durableDeliveryKind === "implementation"
          ? yield* sql<{
              readonly providerDeliveryId: string;
              readonly threadId: string;
              readonly providerInstanceId: string;
              readonly runtimeMode: string;
              readonly cwd: string;
              readonly modelSelectionJson: string;
              readonly modelSelectionFingerprint: string;
              readonly sessionCreatedAt: string;
              readonly resumeCursorJson: string;
            }>`
              SELECT provider_delivery_id AS "providerDeliveryId", thread_id AS "threadId",
                provider_instance_id AS "providerInstanceId", runtime_mode AS "runtimeMode",
                cwd, model_selection_json AS "modelSelectionJson",
                model_selection_fingerprint AS "modelSelectionFingerprint",
                session_created_at AS "sessionCreatedAt", resume_cursor_json AS "resumeCursorJson"
              FROM agent_control_implementation_session_evidence
              WHERE provider_delivery_id = ${input.providerDeliveryId}
            `.pipe(
              Effect.mapError(() =>
                sessionEvidenceError(
                  providerErrorLabel(sessionBefore?.provider),
                  `Implementation session evidence for '${input.threadId}' is unavailable.`,
                ),
              ),
            )
          : durableDeliveryKind === "verification"
            ? yield* sql<{
                readonly providerDeliveryIdBytes: unknown;
                readonly threadIdBytes: unknown;
                readonly providerInstanceIdBytes: unknown;
                readonly runtimeModeBytes: unknown;
                readonly cwdBytes: unknown;
                readonly modelSelectionBytes: unknown;
                readonly modelSelectionFingerprintBytes: unknown;
                readonly sessionCreatedAtBytes: unknown;
                readonly resumeCursorBytes: unknown;
              }>`
                SELECT CAST(provider_delivery_id AS BLOB) AS "providerDeliveryIdBytes",
                  CAST(thread_id AS BLOB) AS "threadIdBytes",
                  CAST(provider_instance_id AS BLOB) AS "providerInstanceIdBytes",
                  CAST(runtime_mode AS BLOB) AS "runtimeModeBytes",
                  CAST(cwd AS BLOB) AS "cwdBytes",
                  CAST(model_selection_json AS BLOB) AS "modelSelectionBytes",
                  CAST(model_selection_fingerprint AS BLOB)
                    AS "modelSelectionFingerprintBytes",
                  CAST(session_created_at AS BLOB) AS "sessionCreatedAtBytes",
                  CAST(resume_cursor_json AS BLOB) AS "resumeCursorBytes"
                FROM main.agent_control_verification_session_evidence
                WHERE provider_delivery_id = ${input.providerDeliveryId}
              `.pipe(
                Effect.flatMap((rows) =>
                  Effect.try({
                    try: (): ReadonlyArray<InitialPlanningSessionEvidenceRow> =>
                      rows.map((row) => ({
                        providerDeliveryId: decodeCanonicalUtf8Bytes(row.providerDeliveryIdBytes),
                        threadId: decodeCanonicalUtf8Bytes(row.threadIdBytes),
                        providerInstanceId: decodeCanonicalUtf8Bytes(row.providerInstanceIdBytes),
                        runtimeMode: decodeCanonicalUtf8Bytes(row.runtimeModeBytes),
                        cwd: decodeCanonicalUtf8Bytes(row.cwdBytes),
                        modelSelectionJson: decodeCanonicalUtf8Bytes(row.modelSelectionBytes),
                        modelSelectionFingerprint: decodeCanonicalUtf8Bytes(
                          row.modelSelectionFingerprintBytes,
                        ),
                        sessionCreatedAt: decodeCanonicalUtf8Bytes(row.sessionCreatedAtBytes),
                        resumeCursorJson: decodeCanonicalUtf8Bytes(row.resumeCursorBytes),
                      })),
                    catch: () =>
                      sessionEvidenceError(
                        providerErrorLabel(sessionBefore?.provider),
                        `Verification session evidence for '${input.threadId}' is invalid UTF-8.`,
                      ),
                  }),
                ),
                Effect.mapError(() =>
                  sessionEvidenceError(
                    providerErrorLabel(sessionBefore?.provider),
                    `Verification session evidence for '${input.threadId}' is unavailable.`,
                  ),
                ),
              )
            : yield* sql<{
                readonly providerDeliveryId: string;
                readonly threadId: string;
                readonly providerInstanceId: string;
                readonly runtimeMode: string;
                readonly cwd: string;
                readonly modelSelectionJson: string;
                readonly modelSelectionFingerprint: string;
                readonly sessionCreatedAt: string;
                readonly resumeCursorJson: string;
              }>`
            SELECT
              provider_delivery_id AS "providerDeliveryId",
              thread_id AS "threadId",
              provider_instance_id AS "providerInstanceId",
              runtime_mode AS "runtimeMode",
              cwd,
              model_selection_json AS "modelSelectionJson",
              model_selection_fingerprint AS "modelSelectionFingerprint",
              session_created_at AS "sessionCreatedAt",
              resume_cursor_json AS "resumeCursorJson"
            FROM agent_control_initial_planning_session_evidence
            WHERE provider_delivery_id = ${input.providerDeliveryId}
          `.pipe(
                Effect.mapError(() =>
                  sessionEvidenceError(
                    providerErrorLabel(sessionBefore?.provider),
                    `${durableStageLabel} session evidence for '${input.threadId}' is unavailable.`,
                  ),
                ),
              );
    const deliveryAuthority =
      input.providerDeliveryId === undefined
        ? []
        : durableDeliveryKind === "implementation"
          ? yield* sql<{ readonly state: string }>`
              SELECT state FROM agent_control_implementation_deliveries
              WHERE provider_delivery_id = ${input.providerDeliveryId}
            `.pipe(
              Effect.mapError(() =>
                sessionEvidenceError(
                  providerErrorLabel(sessionBefore?.provider),
                  `Implementation delivery authority for '${input.threadId}' is unavailable.`,
                ),
              ),
            )
          : durableDeliveryKind === "verification"
            ? yield* sql<{ readonly stateBytes: unknown }>`
                SELECT CAST(state AS BLOB) AS "stateBytes"
                FROM main.agent_control_verification_deliveries
                WHERE provider_delivery_id = ${input.providerDeliveryId}
              `.pipe(
                Effect.flatMap((rows) =>
                  Effect.try({
                    try: () =>
                      rows.map((row) => ({ state: decodeCanonicalUtf8Bytes(row.stateBytes) })),
                    catch: () =>
                      sessionEvidenceError(
                        providerErrorLabel(sessionBefore?.provider),
                        `Verification delivery authority for '${input.threadId}' is invalid UTF-8.`,
                      ),
                  }),
                ),
                Effect.mapError(() =>
                  sessionEvidenceError(
                    providerErrorLabel(sessionBefore?.provider),
                    `Verification delivery authority for '${input.threadId}' is unavailable.`,
                  ),
                ),
              )
            : yield* sql<{ readonly state: string }>`
            SELECT state
            FROM agent_control_initial_planning_deliveries
            WHERE provider_delivery_id = ${input.providerDeliveryId}
          `.pipe(
                Effect.mapError(() =>
                  sessionEvidenceError(
                    providerErrorLabel(sessionBefore?.provider),
                    `Initial Planning delivery authority for '${input.threadId}' is unavailable.`,
                  ),
                ),
              );
    if (input.providerDeliveryId !== undefined) {
      if (sessionBefore === undefined && existingEvidence.length === 1) {
        if (input.modelSelection === undefined) {
          return yield* sessionEvidenceError(
            providerErrorLabelFromInstanceHint({
              instanceId: String(thread.modelSelection.instanceId),
            }),
            `${durableStageLabel} session '${input.threadId}' lacks complete persisted runtime authority.`,
          );
        }
        const modelSelection = input.modelSelection;
        const persisted = existingEvidence[0]!;
        const project = yield* resolveProject(thread.projectId);
        const effectiveCwd = resolveThreadWorkspaceCwd({
          thread,
          projects: project ? [project] : [],
        });
        const modelEvidence = canonicalProviderModelSelectionEvidence(modelSelection);
        const evidenceIsAuthoritative =
          persisted.providerDeliveryId === input.providerDeliveryId &&
          persisted.threadId === String(input.threadId) &&
          persisted.providerInstanceId === String(modelSelection.instanceId) &&
          persisted.runtimeMode === thread.runtimeMode &&
          (effectiveCwd === undefined || persisted.cwd === effectiveCwd) &&
          persisted.modelSelectionJson === modelEvidence.modelSelectionJson &&
          persisted.modelSelectionFingerprint === modelEvidence.modelSelectionFingerprint &&
          persisted.sessionCreatedAt.trim().length > 0;
        if (!evidenceIsAuthoritative) {
          return yield* sessionEvidenceError(
            providerErrorLabelFromInstanceHint({
              instanceId: String(modelSelection.instanceId),
            }),
            `${durableStageLabel} session '${input.threadId}' conflicts with persisted model evidence.`,
          );
        }
        yield* decodeResumeCursorJson(persisted.resumeCursorJson).pipe(
          Effect.mapError(() =>
            sessionEvidenceError(
              providerErrorLabelFromInstanceHint({
                instanceId: String(modelSelection.instanceId),
              }),
              `${durableStageLabel} session '${input.threadId}' has invalid persisted resume evidence.`,
            ),
          ),
        );
      }
      if (
        sessionBefore !== undefined &&
        existingEvidence.length !== 1 &&
        !(existingEvidence.length === 0 && deliveryAuthority[0]?.state === "claimed")
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: providerErrorLabel(sessionBefore.provider),
          method: "thread.turn.start",
          detail: `${durableStageLabel} session '${input.threadId}' has no complete persisted model evidence.`,
        });
      }
      if (sessionBefore !== undefined && existingEvidence.length === 1) {
        if (input.modelSelection === undefined || attestationBefore === undefined) {
          return yield* sessionEvidenceError(
            providerErrorLabel(sessionBefore.provider),
            `${durableStageLabel} session '${input.threadId}' lacks complete runtime authority.`,
          );
        }
        const resumeCursorJson = yield* encodeResumeCursorJson(attestationBefore.resumeCursor).pipe(
          Effect.mapError(() =>
            sessionEvidenceError(
              providerErrorLabel(sessionBefore.provider),
              `${durableStageLabel} session '${input.threadId}' has invalid resume evidence.`,
            ),
          ),
        );
        const persisted = existingEvidence[0]!;
        const expected = buildInitialPlanningSessionEvidence({
          providerDeliveryId: input.providerDeliveryId,
          attestation: attestationBefore,
          resumeCursorJson,
        });
        if (!isInitialPlanningSessionEvidenceRow(persisted, expected)) {
          return yield* sessionEvidenceError(
            providerErrorLabel(sessionBefore.provider),
            `${durableStageLabel} session '${input.threadId}' conflicts with persisted model evidence.`,
          );
        }
      }
    }

    yield* ensureSessionForThread(input.threadId, input.createdAt, {
      ...(input.modelSelection === undefined ? {} : { modelSelection: input.modelSelection }),
      ...(admissionPermit === undefined ? {} : { providerAdmissionPermit: admissionPermit }),
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
    if (activeSession === undefined) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabelFromInstanceHint({
          instanceId: String(input.modelSelection?.instanceId ?? thread.modelSelection.instanceId),
        }),
        method: "thread.turn.start",
        detail: `Thread '${input.threadId}' has no active provider session after preparation.`,
      });
    }
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
    const sessionAttestation =
      input.providerDeliveryId === undefined
        ? undefined
        : yield* (
            providerService.getSessionAttestation?.(input.threadId) ??
              Effect.as(Effect.void, undefined as ProviderSessionAttestation | undefined)
          );
    if (input.providerDeliveryId !== undefined) {
      if (
        input.modelSelection === undefined ||
        sessionAttestation === undefined ||
        sessionAttestation.providerInstanceId !== input.modelSelection.instanceId
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: providerErrorLabel(activeSession.provider),
          method: "thread.turn.start",
          detail: `${durableStageLabel} session '${input.threadId}' lacks complete runtime authority.`,
        });
      }
      const modelEvidence = canonicalProviderModelSelectionEvidence(
        sessionAttestation.effectiveModelSelection,
      );
      if (
        modelEvidence.modelSelectionJson !== sessionAttestation.modelSelectionJson ||
        modelEvidence.modelSelectionFingerprint !== sessionAttestation.modelSelectionFingerprint
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: providerErrorLabel(activeSession.provider),
          method: "thread.turn.start",
          detail: `${durableStageLabel} session '${input.threadId}' has invalid model evidence.`,
        });
      }
      const resumeCursorJson = yield* encodeResumeCursorJson(sessionAttestation.resumeCursor).pipe(
        Effect.mapError(() =>
          sessionEvidenceError(
            providerErrorLabel(activeSession.provider),
            `${durableStageLabel} session '${input.threadId}' has invalid resume evidence.`,
          ),
        ),
      );
      sessionResumeCursorJson = resumeCursorJson;
      const expected = buildInitialPlanningSessionEvidence({
        providerDeliveryId: input.providerDeliveryId,
        attestation: sessionAttestation,
        resumeCursorJson,
      });
      if (existingEvidence.length === 1) {
        const persisted = existingEvidence[0]!;
        if (!isInitialPlanningSessionEvidenceRow(persisted, expected)) {
          return yield* new ProviderAdapterRequestError({
            provider: providerErrorLabel(activeSession.provider),
            method: "thread.turn.start",
            detail: `${durableStageLabel} session '${input.threadId}' conflicts with persisted model evidence.`,
          });
        }
      } else {
        const insertSessionEvidence =
          durableDeliveryKind === "implementation"
            ? sql`
                INSERT INTO agent_control_implementation_session_evidence (
                  provider_delivery_id, thread_id, provider_instance_id, runtime_mode,
                  cwd, model_selection_json, model_selection_fingerprint,
                  session_created_at, resume_cursor_json, recorded_at
                ) VALUES (
                  ${expected.providerDeliveryId}, ${expected.threadId},
                  ${expected.providerInstanceId}, ${expected.runtimeMode}, ${expected.cwd},
                  ${expected.modelSelectionJson}, ${expected.modelSelectionFingerprint},
                  ${expected.sessionCreatedAt}, ${expected.resumeCursorJson}, ${input.createdAt}
                )
              `
            : durableDeliveryKind === "verification"
              ? sql`
                  INSERT INTO main.agent_control_verification_session_evidence (
                    provider_delivery_id, thread_id, provider_instance_id, runtime_mode,
                    cwd, model_selection_json, model_selection_fingerprint,
                    session_created_at, resume_cursor_json, recorded_at
                  ) VALUES (
                    ${expected.providerDeliveryId}, ${expected.threadId},
                    ${expected.providerInstanceId}, ${expected.runtimeMode}, ${expected.cwd},
                    ${expected.modelSelectionJson}, ${expected.modelSelectionFingerprint},
                    ${expected.sessionCreatedAt}, ${expected.resumeCursorJson}, ${input.createdAt}
                  )
                `
              : sql`
                INSERT INTO agent_control_initial_planning_session_evidence (
                  provider_delivery_id, thread_id, provider_instance_id, runtime_mode,
                  cwd, model_selection_json, model_selection_fingerprint,
                  session_created_at, resume_cursor_json, recorded_at
                ) VALUES (
                  ${expected.providerDeliveryId}, ${expected.threadId},
                  ${expected.providerInstanceId}, ${expected.runtimeMode}, ${expected.cwd},
                  ${expected.modelSelectionJson}, ${expected.modelSelectionFingerprint},
                  ${expected.sessionCreatedAt}, ${expected.resumeCursorJson}, ${input.createdAt}
                )
              `;
        yield* (
          durableDeliveryKind === "implementation" || durableDeliveryKind === "verification"
            ? sql.withTransaction(insertSessionEvidence)
            : insertSessionEvidence
        ).pipe(
          Effect.mapError(() =>
            sessionEvidenceError(
              providerErrorLabel(activeSession.provider),
              `${durableStageLabel} session '${input.threadId}' evidence could not be persisted.`,
            ),
          ),
        );
      }
    }
    yield* Effect.annotateCurrentSpan(
      input.providerDeliveryId === undefined
        ? {}
        : { "provider.delivery_id": input.providerDeliveryId },
    );
    return {
      input: {
        threadId: input.threadId,
        ...(normalizedInput ? { input: normalizedInput } : {}),
        ...(normalizedAttachments.length > 0 ? { attachments: normalizedAttachments } : {}),
        ...(modelForTurn !== undefined ? { modelSelection: modelForTurn } : {}),
        ...(input.interactionMode !== undefined ? { interactionMode: input.interactionMode } : {}),
      },
      entryState: {
        adapterEntered: false,
        externalOperationStarted: false,
        adapterReturned: false,
      },
      ...(input.providerDeliveryId === undefined
        ? {}
        : {
            providerDeliveryId: input.providerDeliveryId,
            durableDeliveryKind,
            ...(admissionPermit === undefined ? {} : { providerAdmissionPermit: admissionPermit }),
            ...(sessionAttestation === undefined ? {} : { sessionAttestation }),
            ...(sessionResumeCursorJson === undefined ? {} : { sessionResumeCursorJson }),
          }),
    };
  });

  const prepareTurnDelivery: ProviderTurnRequestExecutorShape["prepareTurnDelivery"] = (input) =>
    prepareTurnDeliveryRaw(input).pipe(
      Effect.catchCause((cause) =>
        input.providerAdmissionPermit === undefined
          ? Effect.failCause(cause)
          : failAfterAdmissionQuarantine(input.providerAdmissionPermit, cause),
      ),
    );

  const sendPreparedTurn: ProviderTurnRequestExecutorShape["sendPreparedTurn"] = Effect.fn(
    "ProviderTurnRequestExecutor.sendPreparedTurn",
  )(function* (prepared) {
    if (prepared.durableDeliveryKind !== undefined) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabelFromInstanceHint({
          instanceId: String(prepared.input.modelSelection?.instanceId ?? "unknown"),
        }),
        method: "thread.turn.start",
        detail: "Durable provider delivery must use the guarded pre-invoke boundary.",
      });
    }
    return yield* providerService.sendTurn(prepared.input);
  });

  const sendPreparedTurnAtPreInvokeBoundary: ProviderTurnRequestExecutorShape["sendPreparedTurnAtPreInvokeBoundary"] =
    Effect.fn("ProviderTurnRequestExecutor.sendPreparedTurnAtPreInvokeBoundary")(
      function* (prepared, boundary) {
        const sendAtBoundary = providerService.sendTurnAtPreInvokeBoundary;
        const attestation = prepared.sessionAttestation;
        const providerAdmissionPermit = prepared.providerAdmissionPermit;
        if (
          sendAtBoundary === undefined ||
          attestation === undefined ||
          providerAdmissionPermit === undefined
        ) {
          return yield* new ProviderTurnDeliveryError({
            certainty: "not-attempted",
            cause: new Error("Initial Planning provider pre-invoke boundary is unavailable."),
          });
        }
        const entryState = prepared.entryState ?? {
          adapterEntered: false,
          externalOperationStarted: false,
          adapterReturned: false,
        };
        const result = yield* sendAtBoundary(prepared.input, {
          expected: attestation,
          providerAdmissionPermit,
          beforeDeliveryCas: boundary.beforeDeliveryCas,
          persistDeliveryAttempted: boundary.persistDeliveryAttempted,
          afterDeliveryCas: boundary.afterDeliveryCas,
          onAdapterEntered: () => {
            entryState.adapterEntered = true;
          },
          onExternalOperationStarted: () => {
            entryState.externalOperationStarted = true;
          },
          onNativeInvocationStarted: () => {
            entryState.externalOperationStarted = true;
          },
        }).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              entryState.adapterReturned = true;
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.failCause(
              mapProviderTurnDeliveryCause(
                cause,
                entryState.externalOperationStarted ? "acceptance-unknown" : "not-attempted",
              ),
            ),
          ),
        );
        return { certainty: "accepted", result };
      },
    );

  const execute: ProviderTurnRequestExecutorShape["execute"] = Effect.fn(
    "ProviderTurnRequestExecutor.execute",
  )(function* (input) {
    return yield* sendPreparedTurn(yield* prepareTurnDelivery(input));
  });

  return ProviderTurnRequestExecutor.of({
    ensureSessionForThread,
    prepareTurnDelivery,
    sendPreparedTurn,
    sendPreparedTurnAtPreInvokeBoundary,
    execute,
  });
});

export const ProviderTurnRequestExecutorLive = Layer.effect(ProviderTurnRequestExecutor, make);
