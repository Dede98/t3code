import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  OrchestrationReactor,
  type OrchestrationReactorShape,
} from "../Services/OrchestrationReactor.ts";
import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import * as AgentAwarenessRelay from "../../relay/AgentAwarenessRelay.ts";
import { AgentControlInitialPlanningConsumer } from "../../agentControl/initialPlanning/Services/AgentControlInitialPlanningConsumer.ts";
import { AgentControlImplementationTurnConsumer } from "../../agentControl/implementationTurn/Services/AgentControlImplementationTurnConsumer.ts";
import { AgentControlVerificationTurnConsumer } from "../../agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumer.ts";

export const makeOrchestrationReactor = Effect.gen(function* () {
  const providerRuntimeIngestion = yield* ProviderRuntimeIngestionService;
  const providerCommandReactor = yield* ProviderCommandReactor;
  const checkpointReactor = yield* CheckpointReactor;
  const threadDeletionReactor = yield* ThreadDeletionReactor;
  const agentAwarenessRelay = yield* AgentAwarenessRelay.AgentAwarenessRelay;
  const initialPlanningConsumer = yield* AgentControlInitialPlanningConsumer;
  const implementationTurnConsumer = yield* AgentControlImplementationTurnConsumer;
  const verificationTurnConsumer = yield* AgentControlVerificationTurnConsumer;

  const start: OrchestrationReactorShape["start"] = Effect.fn("start")(function* () {
    yield* providerRuntimeIngestion.start();
    yield* verificationTurnConsumer.start();
    yield* providerCommandReactor.start();
    yield* checkpointReactor.start();
    yield* threadDeletionReactor.start();
    yield* agentAwarenessRelay.start();
    yield* initialPlanningConsumer.start();
    yield* implementationTurnConsumer.start();
  });

  return {
    start,
  } satisfies OrchestrationReactorShape;
});

export const OrchestrationReactorLive = Layer.effect(
  OrchestrationReactor,
  makeOrchestrationReactor,
);
