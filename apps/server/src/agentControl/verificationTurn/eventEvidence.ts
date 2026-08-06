import type {
  MessageId,
  ModelSelection,
  SourceProposedPlanReference,
  ThreadId,
} from "@t3tools/contracts";

import type { JsonValue } from "../initialPlanning/eventEvidence.ts";

export const verificationMessagePayload = (input: {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly promptText: string;
  readonly createdAt: string;
}): JsonValue => ({
  attachments: [],
  createdAt: input.createdAt,
  messageId: input.messageId,
  role: "user",
  streaming: false,
  text: input.promptText,
  threadId: input.threadId,
  turnId: null,
  updatedAt: input.createdAt,
});

export const verificationTurnRequestPayload = (input: {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: "approval-required";
  readonly sourceProposedPlan: SourceProposedPlanReference;
  readonly createdAt: string;
}): JsonValue => ({
  createdAt: input.createdAt,
  interactionMode: "default",
  messageId: input.messageId,
  modelSelection: input.modelSelection as JsonValue,
  runtimeMode: input.runtimeMode,
  sourceProposedPlan: input.sourceProposedPlan as JsonValue,
  threadId: input.threadId,
});
