import * as Schema from "effect/Schema";

import { IsoDateTime, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ExternalMcpServerId } from "./settings.ts";

export const ExternalMcpOAuthFlowId = TrimmedNonEmptyString.check(Schema.isMaxLength(128));

export const ExternalMcpOAuthStatus = Schema.Struct({
  state: Schema.Literals(["signed-out", "connected", "needs-login"]),
  issuer: Schema.NullOr(Schema.String),
  expiresAt: Schema.NullOr(IsoDateTime),
  message: Schema.NullOr(Schema.String),
  flow: Schema.NullOr(
    Schema.Struct({
      flowId: ExternalMcpOAuthFlowId,
      authorizationUrl: Schema.String,
      /** False when the user must paste the redirect URL back into T3 Code. */
      serverReceivesCallback: Schema.Boolean,
      expiresAt: IsoDateTime,
    }),
  ),
});
export type ExternalMcpOAuthStatus = typeof ExternalMcpOAuthStatus.Type;

/** Every OAuth server in settings, keyed by server id. */
export const ExternalMcpOAuthStatuses = Schema.Record(ExternalMcpServerId, ExternalMcpOAuthStatus);
export type ExternalMcpOAuthStatuses = typeof ExternalMcpOAuthStatuses.Type;

export const ExternalMcpOAuthStartInput = Schema.Struct({
  serverId: ExternalMcpServerId,
  /** "server" listens on the server's loopback; only useful when the browser runs there too. */
  callbackMode: Schema.Literals(["server", "client"]),
});
export type ExternalMcpOAuthStartInput = typeof ExternalMcpOAuthStartInput.Type;

export const ExternalMcpOAuthCompleteInput = Schema.Struct({
  serverId: ExternalMcpServerId,
  flowId: ExternalMcpOAuthFlowId,
  callbackUrl: Schema.String.check(Schema.isMaxLength(16_384)),
});
export type ExternalMcpOAuthCompleteInput = typeof ExternalMcpOAuthCompleteInput.Type;

export const ExternalMcpOAuthCancelInput = Schema.Struct({
  serverId: ExternalMcpServerId,
  flowId: ExternalMcpOAuthFlowId,
});
export type ExternalMcpOAuthCancelInput = typeof ExternalMcpOAuthCancelInput.Type;

export const ExternalMcpOAuthServerInput = Schema.Struct({ serverId: ExternalMcpServerId });
export type ExternalMcpOAuthServerInput = typeof ExternalMcpOAuthServerInput.Type;

export class ExternalMcpOAuthError extends Schema.TaggedError<ExternalMcpOAuthError>()(
  "ExternalMcpOAuthError",
  { message: Schema.String },
) {}
