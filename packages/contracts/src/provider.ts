import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ThreadId } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import {
  getProviderAttachmentLimitError,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  ChatAttachment,
} from "./chatAttachment.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProviderInteractionMode } from "./providerPolicy.ts";

export const ProviderSendTurnInput = Schema.Struct({
  threadId: ThreadId,
  /** Internal recovery: permits empty turns only for adapters supporting promptless continuation. */
  continuation: Schema.optional(Schema.Boolean),
  input: Schema.optional(
    TrimmedNonEmptyString.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  ),
  attachments: Schema.optional(
    Schema.Array(ChatAttachment).check(
      Schema.makeFilter((attachments) => getProviderAttachmentLimitError(attachments) ?? true),
    ),
  ),
  modelSelection: Schema.optional(ModelSelection),
  interactionMode: Schema.optional(ProviderInteractionMode),
});
export type ProviderSendTurnInput = typeof ProviderSendTurnInput.Type;

export const ProviderThreadContinuationSyncInput = Schema.Struct({
  threadId: ThreadId,
});
export type ProviderThreadContinuationSyncInput = typeof ProviderThreadContinuationSyncInput.Type;

export const ProviderThreadContinuationSyncResult = Schema.Struct({
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  state: Schema.Literals(["imported", "already-synced"]),
});
export type ProviderThreadContinuationSyncResult = typeof ProviderThreadContinuationSyncResult.Type;

export const ProviderThreadContinuationSyncErrorCode = Schema.Literals([
  "thread-not-bound",
  "unsupported-provider",
  "feature-disabled",
  "turn-active",
  "resume-state-missing",
  "transcript-not-found",
  "sync-failed",
]);
export type ProviderThreadContinuationSyncErrorCode =
  typeof ProviderThreadContinuationSyncErrorCode.Type;

export class ProviderThreadContinuationSyncError extends Schema.TaggedError<ProviderThreadContinuationSyncError>()(
  "ProviderThreadContinuationSyncError",
  {
    code: ProviderThreadContinuationSyncErrorCode,
    detail: TrimmedNonEmptyString,
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export const ProviderUploadFeedbackInput = Schema.Struct({
  threadId: ThreadId,
  reason: Schema.optional(TrimmedNonEmptyString),
});
export type ProviderUploadFeedbackInput = typeof ProviderUploadFeedbackInput.Type;

export const ProviderUploadFeedbackResult = Schema.Struct({
  feedbackId: TrimmedNonEmptyString,
});
export type ProviderUploadFeedbackResult = typeof ProviderUploadFeedbackResult.Type;

export class ProviderUploadFeedbackError extends Schema.TaggedError<ProviderUploadFeedbackError>()(
  "ProviderUploadFeedbackError",
  {
    threadId: ThreadId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to upload feedback for thread ${this.threadId}.`;
  }
}
