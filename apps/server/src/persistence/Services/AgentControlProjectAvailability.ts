import type { ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  AgentControlPersistenceSqlError,
  AgentControlProjectUnavailableError,
} from "../../agentControl/Errors.ts";

export interface AgentControlProjectAvailabilityShape {
  readonly ensureAvailable: (
    projectId: ProjectId,
  ) => Effect.Effect<void, AgentControlPersistenceSqlError | AgentControlProjectUnavailableError>;
}

export class AgentControlProjectAvailability extends Context.Service<
  AgentControlProjectAvailability,
  AgentControlProjectAvailabilityShape
>()("t3/persistence/Services/AgentControlProjectAvailability") {}
