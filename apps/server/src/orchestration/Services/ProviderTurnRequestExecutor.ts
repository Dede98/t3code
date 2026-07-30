import type {
  ChatAttachment,
  ModelSelection,
  ProviderTurnStartResult,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { OrchestrationDispatchError } from "../Errors.ts";
import type { ProviderServiceError } from "../../provider/Errors.ts";

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

export interface ProviderTurnRequestExecutorShape {
  readonly ensureSessionForThread: (
    threadId: ThreadId,
    createdAt: string,
    options?: { readonly modelSelection?: ModelSelection },
  ) => Effect.Effect<ThreadId, ProviderServiceError | OrchestrationDispatchError>;
  readonly execute: (
    input: ProviderTurnRequestExecutorInput,
  ) => Effect.Effect<ProviderTurnStartResult, ProviderServiceError | OrchestrationDispatchError>;
}

export class ProviderTurnRequestExecutor extends Context.Service<
  ProviderTurnRequestExecutor,
  ProviderTurnRequestExecutorShape
>()("t3/orchestration/Services/ProviderTurnRequestExecutor") {}
