import * as Schema from "effect/Schema";

/**
 * Server-owned Agent Control command authority.
 *
 * Selecting a dispatch function assigns authority; no wire command contains
 * this field. The controller authority is intentionally distinct from the
 * manual orchestration engine's authority model.
 */
export const AgentControlCommandAuthority = Schema.Literals(["human", "controller", "system"]);
export type AgentControlCommandAuthority = typeof AgentControlCommandAuthority.Type;
