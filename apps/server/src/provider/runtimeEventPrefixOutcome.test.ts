import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import { AgentControlVerificationStageStarterError } from "../agentControl/verificationTurn/Services/AgentControlVerificationStageStarter.ts";
import { makeDurablePrefixOutcomeTracker } from "./runtimeEventPrefixOutcome.ts";

it.effect("keeps an isolated durable-prefix failure across every later marker", () =>
  Effect.gen(function* () {
    const tracker = yield* makeDurablePrefixOutcomeTracker;
    const firstFailure = new Error("first-prefix-failure");
    const laterFailure = new Error("later-prefix-failure");
    yield* tracker.recordIsolatedFailure(Cause.fail(firstFailure));

    for (const id of [1, 2]) {
      const acknowledgement = yield* Deferred.make<void, Error>();
      yield* tracker.acknowledge(acknowledgement, id === 1 ? Exit.void : Exit.fail(laterFailure));
      const exit = yield* Effect.exit(Deferred.await(acknowledgement));
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.isTrue(
          exit.cause.reasons.some(
            (reason) => Cause.isFailReason(reason) && reason.error === firstFailure,
          ),
        );
        if (id === 2) {
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === laterFailure,
            ),
          );
        }
      }
    }
  }),
);

it.effect("acknowledges a fully durable prefix successfully", () =>
  Effect.gen(function* () {
    const tracker = yield* makeDurablePrefixOutcomeTracker;
    const acknowledgement = yield* Deferred.make<void, Error>();
    yield* tracker.acknowledge(acknowledgement);
    assert.isTrue(Exit.isSuccess(yield* Effect.exit(Deferred.await(acknowledgement))));
  }),
);

it.effect("combines a sticky prefix Fail with Stage-Starter Fail and cleanup Die", () =>
  Effect.gen(function* () {
    const tracker = yield* makeDurablePrefixOutcomeTracker;
    const prefixFailure = new Error("stored-verification-prefix-failure");
    const stageFailure = new AgentControlVerificationStageStarterError({
      handoffId: "verification-stage-prefix-combine",
      operation: "append-stage-started",
      reason: "persistence",
    });
    const cleanupDefect = new Error("verification-prefix-cleanup-defect");
    yield* tracker.recordIsolatedFailure(Cause.fail(prefixFailure));
    const acknowledgement = yield* Deferred.make<void, Error>();
    yield* tracker.acknowledge(
      acknowledgement,
      Exit.failCause(Cause.combine(Cause.fail(stageFailure), Cause.die(cleanupDefect))),
    );

    const exit = yield* Effect.exit(Deferred.await(acknowledgement));
    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) {
      assert.isTrue(
        exit.cause.reasons.some(
          (reason) => Cause.isFailReason(reason) && reason.error === prefixFailure,
        ),
      );
      assert.isTrue(
        exit.cause.reasons.some(
          (reason) => Cause.isFailReason(reason) && reason.error === stageFailure,
        ),
      );
      assert.isTrue(
        exit.cause.reasons.some(
          (reason) => Cause.isDieReason(reason) && reason.defect === cleanupDefect,
        ),
      );
    }
  }),
);
