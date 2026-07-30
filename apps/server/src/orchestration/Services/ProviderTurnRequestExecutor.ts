import type {
  ChatAttachment,
  ModelSelection,
  ProviderTurnStartResult,
  ProviderSendTurnInput,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { OrchestrationDispatchError } from "../Errors.ts";
import type { ProviderServiceError } from "../../provider/Errors.ts";
import type { ProviderSessionAttestation } from "../../provider/Services/ProviderAdapter.ts";
import * as Schema from "effect/Schema";

export interface ProviderTurnRequestExecutorInput {
  readonly threadId: ThreadId;
  readonly messageText: string;
  readonly attachments?: ReadonlyArray<ChatAttachment>;
  readonly modelSelection?: ModelSelection;
  readonly interactionMode?: "default" | "plan";
  readonly createdAt: string;
  /** Persisted correlation only; current adapters do not expose idempotency keys. */
  readonly providerDeliveryId?: string;
}

export interface PreparedProviderTurnRequest {
  readonly input: ProviderSendTurnInput;
  readonly providerDeliveryId?: string;
  readonly sessionAttestation?: ProviderSessionAttestation;
  readonly sessionResumeCursorJson?: string;
}

export type ProviderTurnAcceptanceCertainty =
  | "not-attempted"
  | "definitely-rejected-before-acceptance"
  | "acceptance-unknown"
  | "accepted";

export class ProviderTurnDeliveryError extends Schema.TaggedErrorClass<ProviderTurnDeliveryError>()(
  "ProviderTurnDeliveryError",
  {
    certainty: Schema.Literals([
      "not-attempted",
      "definitely-rejected-before-acceptance",
      "acceptance-unknown",
    ]),
    cause: Schema.Defect(),
  },
) {}

export interface ProviderTurnRequestExecutorShape {
  readonly ensureSessionForThread: (
    threadId: ThreadId,
    createdAt: string,
    options?: { readonly modelSelection?: ModelSelection },
  ) => Effect.Effect<ThreadId, ProviderServiceError | OrchestrationDispatchError>;
  readonly execute: (
    input: ProviderTurnRequestExecutorInput,
  ) => Effect.Effect<ProviderTurnStartResult, ProviderServiceError | OrchestrationDispatchError>;
  readonly prepareTurnDelivery: (
    input: ProviderTurnRequestExecutorInput,
  ) => Effect.Effect<
    PreparedProviderTurnRequest,
    ProviderServiceError | OrchestrationDispatchError
  >;
  readonly sendPreparedTurn: (
    prepared: PreparedProviderTurnRequest,
  ) => Effect.Effect<ProviderTurnStartResult, ProviderServiceError>;
  readonly sendPreparedTurnAtPreInvokeBoundary: (
    prepared: PreparedProviderTurnRequest,
    boundary: {
      readonly beforeDeliveryCas: () => Effect.Effect<void>;
      readonly persistDeliveryAttempted: (
        attestation: ProviderSessionAttestation,
      ) => Effect.Effect<void, ProviderServiceError>;
      readonly afterDeliveryCas: () => Effect.Effect<void>;
      readonly onAdapterInvoke: () => Effect.Effect<void>;
      readonly afterAdapterReturn: () => Effect.Effect<void>;
    },
  ) => Effect.Effect<
    { readonly certainty: "accepted"; readonly result: ProviderTurnStartResult },
    ProviderTurnDeliveryError
  >;
}

export class ProviderTurnRequestExecutor extends Context.Service<
  ProviderTurnRequestExecutor,
  ProviderTurnRequestExecutorShape
>()("t3/orchestration/Services/ProviderTurnRequestExecutor") {}
