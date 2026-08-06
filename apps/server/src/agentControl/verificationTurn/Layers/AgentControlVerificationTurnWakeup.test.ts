import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";
import { AgentControlVerificationTurnWakeupLive } from "./AgentControlVerificationTurnWakeup.ts";

it.effect("acquires a wakeup subscription before the stream is evaluated", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const wakeup = yield* AgentControlVerificationTurnWakeup;
      const subscribed = yield* wakeup.subscribe;

      yield* wakeup.wake("verification-handoff-ready");

      const received = yield* Stream.runHead(subscribed);
      assert.deepStrictEqual(received, Option.some("verification-handoff-ready"));
    }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
  ),
);
