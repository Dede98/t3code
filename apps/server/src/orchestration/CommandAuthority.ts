import * as Schema from "effect/Schema";

/**
 * Server-owned authority attached to every orchestration command dispatch.
 *
 * This type deliberately lives outside the public contracts package so no
 * transport payload can select its own authority.
 */
export const OrchestrationCommandAuthority = Schema.Literals(["client", "system", "agent-control"]);
export type OrchestrationCommandAuthority = typeof OrchestrationCommandAuthority.Type;

/**
 * Marker assigned to receipts created before command authority was persisted.
 * Legacy receipts fail closed instead of guessing which authority created them.
 */
export const PersistedOrchestrationCommandAuthority = Schema.Union([
  OrchestrationCommandAuthority,
  Schema.Literal("legacy"),
]);
export type PersistedOrchestrationCommandAuthority =
  typeof PersistedOrchestrationCommandAuthority.Type;
