import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import type { ReactorStartupActivation } from "../../reactorStartupActivation.ts";

import type { AgentControlGithubObserveStartupError } from "../github/Services/AgentControlGithubObserveReactor.ts";
import type { AgentControlTaskIntakeStartupError } from "../task/Services/AgentControlTaskIntakeReactor.ts";
import type { AgentControlVerificationAdmissionError } from "../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import type { AgentControlRunOnceError } from "../runOnce/model.ts";
import type { AgentControlArmedError } from "../armed/model.ts";
import type { ProviderAdmissionError } from "../providerAdmission/Services/ProviderAdmissionStore.ts";

export class AgentControlReactorStartupError extends Schema.TaggedErrorClass<AgentControlReactorStartupError>()(
  "AgentControlReactorStartupError",
  {
    reason: Schema.Literals(["already-started-different-scope", "lifecycle-closed"]),
  },
) {}

/** Top-level lifecycle boundary for all Agent Control reactors. */
export interface AgentControlReactorShape {
  /** First terminal fail-closed error from a started Agent Control runtime. */
  readonly awaitFailure: Effect.Effect<never, AgentControlArmedError | ProviderAdmissionError>;
  readonly start: (
    activation?: ReactorStartupActivation,
  ) => Effect.Effect<
    void,
    | AgentControlGithubObserveStartupError
    | AgentControlTaskIntakeStartupError
    | AgentControlVerificationAdmissionError
    | AgentControlRunOnceError
    | AgentControlArmedError
    | ProviderAdmissionError
    | AgentControlReactorStartupError,
    Scope.Scope
  >;
}

export class AgentControlReactor extends Context.Service<
  AgentControlReactor,
  AgentControlReactorShape
>()("t3/agentControl/Services/AgentControlReactor") {}
