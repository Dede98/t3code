import * as Schema from "effect/Schema";

import { IsoDateTime, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";

export const ProviderUsageStatus = Schema.Literals(["allowed", "warning", "rejected"]);
export type ProviderUsageStatus = typeof ProviderUsageStatus.Type;

export const ProviderUsageWindow = Schema.Struct({
  id: TrimmedNonEmptyString,
  label: TrimmedNonEmptyString,
  usedPercent: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  resetsAt: Schema.NullOr(IsoDateTime),
  durationMinutes: Schema.optional(Schema.Number.check(Schema.isGreaterThan(0))),
});
export type ProviderUsageWindow = typeof ProviderUsageWindow.Type;

export const ProviderUsageSnapshot = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  driver: ProviderDriverKind,
  observedAt: IsoDateTime,
  source: Schema.Literals(["runtime-event", "refresh"]),
  status: ProviderUsageStatus,
  windows: Schema.Array(ProviderUsageWindow),
  overageStatus: Schema.optional(ProviderUsageStatus),
  overageResetsAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  isUsingOverage: Schema.optional(Schema.Boolean),
});
export type ProviderUsageSnapshot = typeof ProviderUsageSnapshot.Type;

export const ProviderUsageRefreshFailure = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  message: TrimmedNonEmptyString,
});
export type ProviderUsageRefreshFailure = typeof ProviderUsageRefreshFailure.Type;

export const ProviderUsageRefreshResult = Schema.Struct({
  refreshedAt: IsoDateTime,
  usage: Schema.Array(ProviderUsageSnapshot),
  failures: Schema.Array(ProviderUsageRefreshFailure),
});
export type ProviderUsageRefreshResult = typeof ProviderUsageRefreshResult.Type;

export const ProviderUsageStreamSnapshotEvent = Schema.Struct({
  version: Schema.Literal(1),
  type: Schema.Literal("snapshot"),
  usage: Schema.Array(ProviderUsageSnapshot),
});
export type ProviderUsageStreamSnapshotEvent = typeof ProviderUsageStreamSnapshotEvent.Type;

export const ProviderUsageStreamUpdatedEvent = Schema.Struct({
  version: Schema.Literal(1),
  type: Schema.Literal("updated"),
  usage: ProviderUsageSnapshot,
});
export type ProviderUsageStreamUpdatedEvent = typeof ProviderUsageStreamUpdatedEvent.Type;

export const ProviderUsageStreamRemovedEvent = Schema.Struct({
  version: Schema.Literal(1),
  type: Schema.Literal("removed"),
  providerInstanceId: ProviderInstanceId,
});
export type ProviderUsageStreamRemovedEvent = typeof ProviderUsageStreamRemovedEvent.Type;

export const ProviderUsageStreamEvent = Schema.Union([
  ProviderUsageStreamSnapshotEvent,
  ProviderUsageStreamUpdatedEvent,
  ProviderUsageStreamRemovedEvent,
]);
export type ProviderUsageStreamEvent = typeof ProviderUsageStreamEvent.Type;
