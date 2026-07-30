import {
  AgentControlThreadBinding,
  AgentControlThreadMaterializeCommand,
  ModelSelection,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationState,
  type CommandId,
} from "@t3tools/contracts";
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { fingerprintAgentControlThreadMaterializationCommand } from "../../orchestration/agentControlThreadMaterializationIntent.ts";
import {
  deriveAgentControlBoundTransitionCommandId,
  deriveAgentControlControlledThreadActivationCommandId,
  deriveAgentControlMaterializingTransitionCommandId,
  deriveAgentControlThreadMaterializationCommandId,
  sha256AgentControlIdentity,
} from "./identity.ts";

export interface PersistedControlledThreadCoordinatorFingerprintEvidence {
  readonly coordinatorCommandId: string;
  readonly requestFingerprint: string;
  readonly coordinatorCommandFingerprint: string;
  readonly policyBindingFingerprint: string;
  readonly runtimeObservationFingerprint: string;
  readonly projectId: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly roleId: string;
  readonly stageKind: string;
  readonly stageOrdinal: number;
  readonly attemptOrdinal: number;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly worktreeReservationId: string;
  readonly materializationCommandId: string;
  readonly materializationCommandFingerprint: string;
  readonly title: string;
  readonly modelSelectionJson: string;
  readonly runtimeMode: string;
  readonly interactionMode: string;
  readonly branch: string;
  readonly worktreePath: string;
  readonly bindingJson: string;
  readonly materializedAt: string;
}

const decodeMaterializationCommand = Schema.decodeUnknownEffect(
  AgentControlThreadMaterializeCommand,
);
const decodeModelSelectionJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const decodeBindingJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlThreadBinding),
);

const validateControlledThreadCoordinatorFingerprints = Effect.fn(
  "validateCanonicalControlledThreadCoordinatorFingerprints",
)(function* (
  crypto: Crypto.Crypto,
  evidence: PersistedControlledThreadCoordinatorFingerprintEvidence,
) {
  const [modelSelection, binding] = yield* Effect.all([
    decodeModelSelectionJson(evidence.modelSelectionJson),
    decodeBindingJson(evidence.bindingJson),
  ]);
  const command = yield* decodeMaterializationCommand({
    type: "thread.agent-control.materialize",
    commandId: evidence.materializationCommandId,
    controlledThreadReservationId: evidence.controlledThreadReservationId,
    threadId: evidence.threadId,
    projectId: evidence.projectId,
    taskId: evidence.taskId,
    taskRevision: evidence.taskRevision,
    githubIntakeSequence: evidence.githubIntakeSequence,
    sourceIdentityFingerprint: evidence.sourceIdentityFingerprint,
    stageRunId: evidence.stageRunId,
    attemptId: evidence.attemptId,
    roleId: evidence.roleId,
    stageKind: evidence.stageKind,
    stageOrdinal: evidence.stageOrdinal,
    attemptOrdinal: evidence.attemptOrdinal,
    leaseId: evidence.leaseId,
    fenceToken: evidence.fenceToken,
    worktreeReservationId: evidence.worktreeReservationId,
    title: evidence.title,
    modelSelection,
    runtimeMode: evidence.runtimeMode,
    interactionMode: evidence.interactionMode,
    branch: evidence.branch,
    worktreePath: evidence.worktreePath,
    binding,
    createdAt: evidence.materializedAt,
  });
  const materializationFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
    crypto,
    command,
  );
  return (
    materializationFingerprint === evidence.materializationCommandFingerprint &&
    evidence.requestFingerprint ===
      deriveAgentControlControlledThreadCoordinatorRequestFingerprint({
        commandId: evidence.coordinatorCommandId,
        projectId: evidence.projectId,
        controlledThreadReservationId: evidence.controlledThreadReservationId,
      }) &&
    evidence.coordinatorCommandFingerprint ===
      deriveAgentControlControlledThreadCoordinatorFingerprint(
        {
          commandId: evidence.coordinatorCommandId,
          projectId: evidence.projectId,
          controlledThreadReservationId: evidence.controlledThreadReservationId,
        },
        materializationFingerprint,
        evidence.leaseHolderId,
        evidence.policyBindingFingerprint,
        evidence.runtimeObservationFingerprint,
      )
  );
});

export const validateCanonicalControlledThreadCoordinatorFingerprints = (
  crypto: Crypto.Crypto,
  evidence: PersistedControlledThreadCoordinatorFingerprintEvidence,
): Effect.Effect<boolean> =>
  validateControlledThreadCoordinatorFingerprints(crypto, evidence).pipe(
    Effect.orElseSucceed(() => false),
  );

export const deriveAgentControlControlledThreadCoordinatorRequestFingerprint = (input: {
  readonly commandId: string;
  readonly projectId: string;
  readonly controlledThreadReservationId: string;
}) =>
  sha256AgentControlIdentity([
    "agent-control-controlled-thread-materialization-coordinator-request-v1",
    input.commandId,
    input.projectId,
    input.controlledThreadReservationId,
  ]);

export const deriveAgentControlControlledThreadCoordinatorFingerprint = (
  input: {
    readonly commandId: string;
    readonly projectId: string;
    readonly controlledThreadReservationId: string;
  },
  commandFingerprint: string,
  leaseHolderId: string,
  policyBindingFingerprint: string,
  runtimeObservationFingerprint: string,
) =>
  sha256AgentControlIdentity([
    "agent-control-controlled-thread-materialization-coordinator-v1",
    input.commandId,
    input.projectId,
    input.controlledThreadReservationId,
    leaseHolderId,
    commandFingerprint,
    policyBindingFingerprint,
    runtimeObservationFingerprint,
  ]);

const hasSamePreparedBinding = (
  prepared: Extract<
    AgentControlControlledThreadReservationEvent,
    { readonly type: "agentControl.controlledThreadReservation.prepared" }
  >["payload"],
  successor:
    | Extract<
        AgentControlControlledThreadReservationEvent,
        { readonly type: "agentControl.controlledThreadReservation.materializing" }
      >["payload"]
    | Extract<
        AgentControlControlledThreadReservationEvent,
        { readonly type: "agentControl.controlledThreadReservation.bound" }
      >["payload"],
) =>
  successor.controlledThreadReservationId === prepared.controlledThreadReservationId &&
  successor.threadId === prepared.threadId &&
  successor.projectId === prepared.projectId &&
  successor.taskId === prepared.taskId &&
  successor.taskRevision === prepared.taskRevision &&
  successor.githubIntakeSequence === prepared.githubIntakeSequence &&
  successor.sourceIdentityFingerprint === prepared.sourceIdentityFingerprint &&
  successor.stageRunId === prepared.stageRunId &&
  successor.attemptId === prepared.attemptId &&
  successor.roleId === prepared.roleId &&
  successor.stageKind === prepared.stageKind &&
  successor.stageOrdinal === prepared.stageOrdinal &&
  successor.attemptOrdinal === prepared.attemptOrdinal &&
  successor.leaseId === prepared.leaseId &&
  successor.fenceToken === prepared.fenceToken &&
  successor.worktreeReservationId === prepared.worktreeReservationId &&
  successor.preparedAt === prepared.preparedAt;

/**
 * Validate reservation successor identity only from immutable prepared@1
 * evidence. The coordinator replay composes this with its deeper intent,
 * receipt, marker, orchestration-event, and thread-projection validator.
 */
export const validateCanonicalControlledThreadSuccessors = Effect.fn(
  "validateCanonicalControlledThreadSuccessors",
)(function* (input: {
  readonly prepareCommandId: CommandId;
  readonly state: AgentControlControlledThreadReservationState;
  readonly history: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
}) {
  const prepared = input.history[0];
  if (
    prepared?.type !== "agentControl.controlledThreadReservation.prepared" ||
    prepared.streamVersion !== 1 ||
    prepared.commandId !== input.prepareCommandId ||
    prepared.correlationId !== input.prepareCommandId ||
    prepared.causationEventId !== null ||
    prepared.aggregateId !== input.state.controlledThreadReservationId ||
    prepared.payload.controlledThreadReservationId !== input.state.controlledThreadReservationId
  ) {
    return false;
  }
  if (input.history.length === 1) {
    return input.state.status === "prepared" && input.state.revision === 1;
  }
  // materializing@2 is never a complete replayable result on its own.
  if (input.history.length !== 3 || input.state.status !== "bound" || input.state.revision !== 3) {
    return false;
  }

  const reservationId = prepared.payload.controlledThreadReservationId;
  const coordinatorCommandId = yield* deriveAgentControlControlledThreadActivationCommandId(
    input.prepareCommandId,
    reservationId,
  );
  const [materializingTransitionCommandId, materializationCommandId, boundTransitionCommandId] =
    yield* Effect.all([
      deriveAgentControlMaterializingTransitionCommandId(coordinatorCommandId, reservationId),
      deriveAgentControlThreadMaterializationCommandId(coordinatorCommandId, reservationId),
      deriveAgentControlBoundTransitionCommandId(coordinatorCommandId, reservationId),
    ]);
  const materializing = input.history[1];
  const bound = input.history[2];
  if (
    materializing?.type !== "agentControl.controlledThreadReservation.materializing" ||
    materializing.streamVersion !== 2 ||
    materializing.aggregateId !== reservationId ||
    materializing.commandId !== materializingTransitionCommandId ||
    materializing.correlationId !== coordinatorCommandId ||
    materializing.causationEventId !== null ||
    materializing.payload.coordinatorCommandId !== coordinatorCommandId ||
    materializing.payload.materializingTransitionCommandId !== materializingTransitionCommandId ||
    materializing.payload.materializationCommandId !== materializationCommandId ||
    !hasSamePreparedBinding(prepared.payload, materializing.payload) ||
    bound?.type !== "agentControl.controlledThreadReservation.bound" ||
    bound.streamVersion !== 3 ||
    bound.aggregateId !== reservationId ||
    bound.commandId !== boundTransitionCommandId ||
    bound.correlationId !== coordinatorCommandId ||
    bound.causationEventId !== null ||
    bound.payload.coordinatorCommandId !== coordinatorCommandId ||
    bound.payload.materializingTransitionCommandId !== materializingTransitionCommandId ||
    bound.payload.materializationCommandId !== materializationCommandId ||
    bound.payload.boundTransitionCommandId !== boundTransitionCommandId ||
    !hasSamePreparedBinding(prepared.payload, bound.payload) ||
    bound.payload.coordinatorCommandFingerprint !==
      materializing.payload.coordinatorCommandFingerprint ||
    bound.payload.materializationCommandFingerprint !==
      materializing.payload.materializationCommandFingerprint ||
    bound.payload.leaseHolderId !== materializing.payload.leaseHolderId ||
    bound.payload.materializingAt !== materializing.payload.materializingAt ||
    prepared.sequence >= materializing.sequence ||
    materializing.sequence >= bound.sequence
  ) {
    return false;
  }

  return (
    input.state.sequence === bound.sequence &&
    input.state.coordinatorCommandId === coordinatorCommandId &&
    input.state.coordinatorCommandFingerprint ===
      materializing.payload.coordinatorCommandFingerprint &&
    input.state.materializingTransitionCommandId === materializingTransitionCommandId &&
    input.state.materializationCommandId === materializationCommandId &&
    input.state.materializationCommandFingerprint ===
      materializing.payload.materializationCommandFingerprint &&
    input.state.boundTransitionCommandId === boundTransitionCommandId &&
    input.state.orchestrationResultSequence === bound.payload.orchestrationResultSequence &&
    input.state.leaseHolderId === materializing.payload.leaseHolderId &&
    input.state.materializingAt === materializing.payload.materializingAt &&
    input.state.materializedAt === bound.payload.materializedAt &&
    input.state.boundAt === bound.payload.boundAt
  );
});
