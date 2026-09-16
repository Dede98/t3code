import * as Schema from "effect/Schema";

export type ResourceAdmissionKind = "providerTurn" | "localCheck";
export type ResourceAdmissionPriority = "interactive" | "background";

export interface ResourceAdmissionSettings {
  readonly providerMaxConcurrent: number;
  readonly interactiveReserve: number;
  readonly backgroundMaxGrantDelayMs: number;
  readonly maxInteractiveGrantBurst: number;
  readonly localCheckMaxConcurrent: number;
  readonly cpuPauseUtilization: number;
  readonly cpuResumeUtilization: number;
  readonly availableMemoryPauseBytes: number;
  readonly availableMemoryResumeBytes: number;
  readonly gpuMaxConcurrent: number;
  readonly missingTelemetryPolicy: "defer-background" | "allow";
}

export const defaultResourceAdmissionSettings: ResourceAdmissionSettings = {
  providerMaxConcurrent: 4,
  interactiveReserve: 1,
  backgroundMaxGrantDelayMs: 30_000,
  maxInteractiveGrantBurst: 3,
  localCheckMaxConcurrent: 1,
  cpuPauseUtilization: 0.85,
  cpuResumeUtilization: 0.7,
  availableMemoryPauseBytes: 1.5 * 1024 * 1024 * 1024,
  availableMemoryResumeBytes: 2 * 1024 * 1024 * 1024,
  gpuMaxConcurrent: 0,
  missingTelemetryPolicy: "defer-background",
};

export interface ResourcePressureSample {
  readonly sampledAtMs: number;
  readonly telemetry: "available" | "unavailable";
  readonly cpuUtilization: number | null;
  readonly availableMemoryBytes: number | null;
  readonly gpu:
    | { readonly status: "reliable"; readonly available: number }
    | { readonly status: "unavailable" };
}

export interface ResourceAdmissionParentAuthority {
  readonly reservationId: string;
  readonly reservationFenceToken: number;
}

export interface ResourceAdmissionRequest {
  /** Stable operation identity. Retrying it never consumes another slot. */
  readonly requestId: string;
  readonly kind: ResourceAdmissionKind;
  readonly priority: ResourceAdmissionPriority;
  /** Provider account/capacity identity, not a provider-instance display name. */
  readonly accountScope?: string | undefined;
  readonly ownerId: string;
  readonly ownerFenceToken: number;
  /** Stable provider-session or local-check identity used during startup reconciliation. */
  readonly executionKey?: string;
  readonly gpuRequired?: boolean;
  readonly parent?: ResourceAdmissionParentAuthority;
}

export interface ResourceReservationAuthority {
  readonly reservationId: string;
  readonly ownerId: string;
  readonly ownerFenceToken: number;
  readonly reservationFenceToken: number;
}

export type ResourceAdmissionWaitReason =
  | "provider-limit"
  | "local-check-limit"
  | "cpu-pressure"
  | "memory-pressure"
  | "telemetry-unavailable"
  | "gpu-capacity"
  | "recovery-capacity"
  | "interactive-priority";

export type ResourceAdmissionDecision =
  | {
      readonly _tag: "Admitted";
      readonly authority: ResourceReservationAuthority;
      /** False for a same-kind child that shares its parent's slot. */
      readonly accounted: boolean;
    }
  | {
      readonly _tag: "Waiting";
      readonly requestId: string;
      readonly reason: ResourceAdmissionWaitReason;
    }
  | {
      readonly _tag: "Rejected";
      readonly requestId: string;
      readonly reason: "gpu-unavailable" | "invalid-request" | "ownership-conflict";
      readonly message: string;
    };

export interface ResourceAdmissionGrant {
  readonly requestId: string;
  readonly authority: ResourceReservationAuthority;
  readonly accounted: boolean;
}

export interface ResourceAdmissionUpdate<A> {
  readonly result: A;
  /** Every waiter atomically promoted by this operation, including other owners. */
  readonly newlyAdmitted: ReadonlyArray<ResourceAdmissionGrant>;
  readonly ledgerRevision: number | null;
  readonly pressureSampledAtMs: number;
}

export interface ResourceAdmissionSnapshotEntry {
  readonly requestId: string;
  readonly kind: ResourceAdmissionKind;
  readonly priority: ResourceAdmissionPriority;
  readonly accountScope: string | null;
  readonly state: "waiting" | "admitted" | "canceled" | "released";
  readonly waitReason: ResourceAdmissionWaitReason | null;
  readonly accounted: boolean;
  readonly gpuRequired: boolean;
  readonly ownerId: string;
  readonly ownerFenceToken: number;
  readonly executionKey: string | null;
  readonly reservationFenceToken: number | null;
  readonly requestedAtMs: number;
  readonly admittedAtMs: number | null;
  readonly activity:
    | "possible"
    | "active"
    | "inactive"
    | "unknown"
    | "orphaned-active"
    | "orphaned-unknown";
  readonly activityObservedAtMs: number;
  readonly parentReservationId: string | null;
}

export interface ResourceAdmissionSnapshot {
  readonly entries: ReadonlyArray<ResourceAdmissionSnapshotEntry>;
  readonly pressure: {
    readonly telemetryAvailable: boolean;
    readonly cpuPaused: boolean;
    readonly memoryPaused: boolean;
    readonly sampledAtMs: number | null;
  };
  readonly enforcement: {
    readonly slotAccounting: "host-process-atomic";
    readonly cpu: "observed-soft-threshold";
    readonly memory: "observed-soft-threshold";
    readonly gpu: "slot-accounting-only" | "unavailable";
    readonly osHardLimits: "unsupported";
  };
  /** Conservative machine-wide composition of every environment that touched this ledger. */
  readonly effectiveSettings: ResourceAdmissionSettings | null;
}

export const PersistedReservation = Schema.Struct({
  requestId: Schema.String,
  kind: Schema.Union([Schema.Literal("providerTurn"), Schema.Literal("localCheck")]),
  priority: Schema.Union([Schema.Literal("interactive"), Schema.Literal("background")]),
  accountScope: Schema.NullOr(Schema.String),
  ownerId: Schema.String,
  ownerFenceToken: Schema.Int,
  executionKey: Schema.NullOr(Schema.String),
  reservationFenceToken: Schema.NullOr(Schema.Int),
  requestedAtMs: Schema.Int,
  sequence: Schema.Int,
  admittedAtMs: Schema.NullOr(Schema.Int),
  state: Schema.Union([
    Schema.Literal("waiting"),
    Schema.Literal("admitted"),
    Schema.Literal("canceled"),
    Schema.Literal("released"),
  ]),
  accounted: Schema.Boolean,
  gpuRequired: Schema.Boolean,
  activity: Schema.Union([
    Schema.Literal("possible"),
    Schema.Literal("active"),
    Schema.Literal("inactive"),
    Schema.Literal("unknown"),
    Schema.Literal("orphaned-active"),
    Schema.Literal("orphaned-unknown"),
  ]),
  activityObservedAtMs: Schema.Int,
  parentReservationId: Schema.NullOr(Schema.String),
  parentReservationFenceToken: Schema.NullOr(Schema.Int),
});

export type PersistedReservation = typeof PersistedReservation.Type;

const PersistedResourceAdmissionSettings = Schema.Struct({
  providerMaxConcurrent: Schema.Int,
  interactiveReserve: Schema.Int,
  backgroundMaxGrantDelayMs: Schema.Int,
  maxInteractiveGrantBurst: Schema.Int,
  localCheckMaxConcurrent: Schema.Int,
  cpuPauseUtilization: Schema.Finite,
  cpuResumeUtilization: Schema.Finite,
  availableMemoryPauseBytes: Schema.Int,
  availableMemoryResumeBytes: Schema.Int,
  gpuMaxConcurrent: Schema.Int,
  missingTelemetryPolicy: Schema.Union([
    Schema.Literal("defer-background"),
    Schema.Literal("allow"),
  ]),
});

export const ResourceAdmissionLedgerState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  revision: Schema.Int,
  nextSequence: Schema.Int,
  nextReservationFenceToken: Schema.Int,
  reservations: Schema.Record(Schema.String, PersistedReservation),
  interactiveGrantBursts: Schema.Record(Schema.String, Schema.Int),
  settingsRegistrations: Schema.Record(
    Schema.String,
    Schema.Struct({
      pid: Schema.Int,
      settings: PersistedResourceAdmissionSettings,
    }),
  ),
  effectiveSettings: Schema.NullOr(PersistedResourceAdmissionSettings),
  pressure: Schema.Struct({
    telemetryAvailable: Schema.Boolean,
    cpuPaused: Schema.Boolean,
    memoryPaused: Schema.Boolean,
    sampledAtMs: Schema.NullOr(Schema.Int),
  }),
});

export type ResourceAdmissionLedgerState = typeof ResourceAdmissionLedgerState.Type;

export const emptyResourceAdmissionLedgerState = (): ResourceAdmissionLedgerState => ({
  schemaVersion: 1,
  revision: 0,
  nextSequence: 1,
  nextReservationFenceToken: 1,
  reservations: {},
  interactiveGrantBursts: {},
  settingsRegistrations: {},
  effectiveSettings: null,
  pressure: {
    telemetryAvailable: false,
    cpuPaused: false,
    memoryPaused: false,
    sampledAtMs: null,
  },
});
