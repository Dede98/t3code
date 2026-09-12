import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION,
  type AgentControlVerificationPromptTemplateVersion,
} from "../prompt.ts";

export interface AgentControlVerificationTurnObservation {
  readonly handoffId: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
}

export interface AgentControlVerificationTurnCoordinatorHooksShape {
  readonly promptTemplateVersion: AgentControlVerificationPromptTemplateVersion;
  readonly afterAdmissionReplay: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterMaterializingProjection: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterOrchestrationMaterialization: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterBoundProjection: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterHandoffAccepted: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly beforeMaterializationMarker: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterOuterCommit: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterPublication: (
    observation: AgentControlVerificationTurnObservation,
  ) => Effect.Effect<void>;
}

export class AgentControlVerificationTurnCoordinatorHooks extends Context.Service<
  AgentControlVerificationTurnCoordinatorHooks,
  AgentControlVerificationTurnCoordinatorHooksShape
>()("t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnCoordinatorHooks") {}

const noop: AgentControlVerificationTurnCoordinatorHooksShape = {
  promptTemplateVersion: AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION,
  afterAdmissionReplay: () => Effect.void,
  afterMaterializingProjection: () => Effect.void,
  afterOrchestrationMaterialization: () => Effect.void,
  afterBoundProjection: () => Effect.void,
  afterHandoffAccepted: () => Effect.void,
  beforeMaterializationMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};

export const AgentControlVerificationTurnCoordinatorHooksNoop = Layer.succeed(
  AgentControlVerificationTurnCoordinatorHooks,
  AgentControlVerificationTurnCoordinatorHooks.of(noop),
);
