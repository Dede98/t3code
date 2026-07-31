/**
 * ProviderAdapter - Provider-specific runtime adapter contract.
 *
 * Defines the provider-native session/protocol operations that `ProviderService`
 * routes to after resolving the target provider. Implementations should focus
 * on provider behavior only and avoid cross-provider orchestration concerns.
 *
 * @module ProviderAdapter
 */
import type {
  ApprovalRequestId,
  ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderUserInputAnswers,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderSessionStartInput,
  ModelSelection,
  ProviderInstanceId,
  ProviderThreadContinuationSyncResult,
  ThreadId,
  ProviderTurnStartResult,
  TurnId,
} from "@t3tools/contracts";
import type { ProviderUsageSnapshot } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";
import * as NodeCrypto from "node:crypto";

export type ProviderSessionModelSwitchMode = "in-session" | "unsupported";

export interface ProviderAdapterCapabilities {
  /**
   * Declares whether changing the model on an existing session is supported.
   */
  readonly sessionModelSwitch: ProviderSessionModelSwitchMode;
}

export interface ProviderThreadTurnSnapshot {
  readonly id: TurnId;
  readonly items: ReadonlyArray<unknown>;
}

export interface ProviderThreadSnapshot {
  readonly threadId: ThreadId;
  readonly turns: ReadonlyArray<ProviderThreadTurnSnapshot>;
}

export interface ProviderSessionAttestation {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly runtimeMode: ProviderSession["runtimeMode"];
  readonly cwd: string;
  readonly effectiveModelSelection: ModelSelection | null;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly sessionCreatedAt: string;
  readonly resumeCursor: unknown;
}

export interface ProviderTurnAttestation {
  readonly providerInstanceId: ProviderInstanceId;
  readonly effectiveModelSelection: ModelSelection;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
}

export const canonicalProviderModelSelectionEvidence = (selection: ModelSelection | null) => {
  const effectiveModelSelection: ModelSelection | null =
    selection === null
      ? null
      : {
          instanceId: selection.instanceId,
          model: selection.model,
          ...(selection.options === undefined
            ? {}
            : {
                options: [...selection.options]
                  .sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0))
                  .map((option) => ({ id: option.id, value: option.value })),
              }),
        };
  const modelSelectionJson = JSON.stringify(effectiveModelSelection);
  return {
    effectiveModelSelection,
    modelSelectionJson,
    modelSelectionFingerprint: NodeCrypto.createHash("sha256")
      .update(modelSelectionJson, "utf8")
      .digest("hex"),
  } as const;
};

export const attestProviderNativeTurnConfiguration = (
  effectiveModelSelection: ModelSelection,
): ProviderTurnAttestation => {
  const evidence = canonicalProviderModelSelectionEvidence(effectiveModelSelection);
  return {
    providerInstanceId: effectiveModelSelection.instanceId,
    effectiveModelSelection,
    modelSelectionJson: evidence.modelSelectionJson,
    modelSelectionFingerprint: evidence.modelSelectionFingerprint,
  };
};

export type ProviderSessionWithAttestation = ProviderSession & {
  /**
   * Server-internal evidence emitted from the adapter configuration that was
   * actually bound. It is deliberately not part of transport contracts.
   */
  readonly initialPlanningAttestation?: ProviderSessionAttestation;
};

export const attestProviderSessionNativeConfiguration = (
  session: ProviderSession,
  effectiveModelSelection: ModelSelection | null,
): ProviderSessionWithAttestation => {
  if (
    session.providerInstanceId === undefined ||
    session.cwd === undefined ||
    (effectiveModelSelection !== null &&
      (effectiveModelSelection.instanceId !== session.providerInstanceId ||
        effectiveModelSelection.model !== session.model))
  ) {
    return session;
  }
  const modelEvidence = canonicalProviderModelSelectionEvidence(effectiveModelSelection);
  return {
    ...session,
    initialPlanningAttestation: {
      threadId: session.threadId,
      providerInstanceId: session.providerInstanceId,
      runtimeMode: session.runtimeMode,
      cwd: session.cwd,
      ...modelEvidence,
      sessionCreatedAt: session.createdAt,
      resumeCursor: session.resumeCursor ?? null,
    },
  };
};

export interface ProviderAdapterTurnEntry {
  readonly adapterEntered: () => Effect.Effect<void>;
  /**
   * ACP adapters call this only after their runtime has synchronously started
   * the native prompt request. Other adapters retain `startExternal`.
   */
  readonly nativeInvocationStarted?: () => Effect.Effect<void>;
  readonly startExternal: <A, E, R>(
    operation: () => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}

export interface ProviderAdapterPreparedTurn<TError> {
  readonly attestation: ProviderTurnAttestation;
  readonly invoke: (
    entry: ProviderAdapterTurnEntry,
  ) => Effect.Effect<ProviderTurnStartResult, TError>;
}

export class ProviderContinuationSyncCapabilityError extends Schema.TaggedErrorClass<ProviderContinuationSyncCapabilityError>()(
  "ProviderContinuationSyncCapabilityError",
  {
    code: Schema.Literals(["transcript-not-found", "sync-failed"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface ProviderAdapterShape<TError> {
  /**
   * Provider kind implemented by this adapter.
   */
  readonly provider: ProviderDriverKind;
  readonly capabilities: ProviderAdapterCapabilities;

  /** Read the current account limits without requiring an active chat. */
  readonly readUsage?: () => Effect.Effect<ProviderUsageSnapshot, TError>;

  /**
   * Mirror a provider-native thread transcript into portable continuation
   * storage without sending a model turn. Providers that do not support
   * portable continuation omit this capability.
   */
  readonly syncContinuation?: (input: {
    readonly threadId: ThreadId;
    readonly resumeCursor: unknown;
    readonly cwd?: string;
  }) => Effect.Effect<
    ProviderThreadContinuationSyncResult["state"],
    TError | ProviderContinuationSyncCapabilityError
  >;

  /**
   * Start a provider-backed session.
   */
  readonly startSession: (
    input: ProviderSessionStartInput,
  ) => Effect.Effect<ProviderSessionWithAttestation, TError>;

  /**
   * Send a turn to an active provider session.
   */
  readonly sendTurn: (
    input: ProviderSendTurnInput,
  ) => Effect.Effect<ProviderTurnStartResult, TError>;

  /**
   * Fully prepares the provider-native turn without starting external work.
   * Initial Planning requires this boundary so its durable CAS can be
   * distinguished from actual adapter and external-operation entry.
   */
  readonly prepareTurn?: (
    input: ProviderSendTurnInput,
  ) => Effect.Effect<ProviderAdapterPreparedTurn<TError>, TError>;

  /**
   * Interrupt an active turn.
   */
  readonly interruptTurn: (threadId: ThreadId, turnId?: TurnId) => Effect.Effect<void, TError>;

  /**
   * Respond to an interactive approval request.
   */
  readonly respondToRequest: (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ) => Effect.Effect<void, TError>;

  /**
   * Respond to a structured user-input request.
   */
  readonly respondToUserInput: (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    answers: ProviderUserInputAnswers,
  ) => Effect.Effect<void, TError>;

  /**
   * Stop one provider session.
   */
  readonly stopSession: (threadId: ThreadId) => Effect.Effect<void, TError>;

  /**
   * List currently active provider sessions for this adapter.
   */
  readonly listSessions: () => Effect.Effect<ReadonlyArray<ProviderSession>>;

  /**
   * Check whether this adapter owns an active session id.
   */
  readonly hasSession: (threadId: ThreadId) => Effect.Effect<boolean>;

  /**
   * Read a provider thread snapshot.
   */
  readonly readThread: (threadId: ThreadId) => Effect.Effect<ProviderThreadSnapshot, TError>;

  /**
   * Roll back a provider thread by N turns.
   */
  readonly rollbackThread: (
    threadId: ThreadId,
    numTurns: number,
  ) => Effect.Effect<ProviderThreadSnapshot, TError>;

  /**
   * Stop all sessions owned by this adapter.
   */
  readonly stopAll: () => Effect.Effect<void, TError>;

  /**
   * Canonical runtime event stream emitted by this adapter.
   */
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}
