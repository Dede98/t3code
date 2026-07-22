import {
  AgentControlRejectedCommandErrorCode,
  CommandId,
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { AgentControlCommandAuthority } from "../../agentControl/AgentControlCommandAuthority.ts";
import type { AgentControlRepositoryError } from "../../agentControl/Errors.ts";

const AgentControlCommandReceiptBase = Schema.Struct({
  commandId: CommandId,
  commandFingerprint: Schema.String,
  authority: AgentControlCommandAuthority,
  aggregateKind: Schema.Literal("project-controller"),
  aggregateId: ProjectId,
  resultSequence: NonNegativeInt,
  resultStreamVersion: NonNegativeInt,
  acceptedAt: IsoDateTime,
});

export const AgentControlCommandReceipt = Schema.Union([
  Schema.Struct({
    ...AgentControlCommandReceiptBase.fields,
    status: Schema.Literal("accepted"),
    eventCreated: Schema.Boolean,
    errorCode: Schema.Null,
  }),
  Schema.Struct({
    ...AgentControlCommandReceiptBase.fields,
    status: Schema.Literal("rejected"),
    eventCreated: Schema.Literal(false),
    errorCode: AgentControlRejectedCommandErrorCode,
  }),
]);
export type AgentControlCommandReceipt = typeof AgentControlCommandReceipt.Type;

export interface AgentControlCommandReceiptRepositoryShape {
  /** Immutable insert: command identity can never be overwritten. */
  readonly insert: (
    receipt: AgentControlCommandReceipt,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly getByCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<Option.Option<AgentControlCommandReceipt>, AgentControlRepositoryError>;
}

export class AgentControlCommandReceiptRepository extends Context.Service<
  AgentControlCommandReceiptRepository,
  AgentControlCommandReceiptRepositoryShape
>()("t3/persistence/Services/AgentControlCommandReceipts/AgentControlCommandReceiptRepository") {}
