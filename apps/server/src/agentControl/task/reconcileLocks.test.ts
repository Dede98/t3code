import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { makeAgentControlTaskReconcileLocks } from "./reconcileLocks.ts";

it.effect("does not retain locks for projects that fail availability preflight", () =>
  Effect.gen(function* () {
    const locks = yield* makeAgentControlTaskReconcileLocks<ProjectId>();
    for (let index = 0; index < 1_000; index += 1) {
      for (const reason of ["project-missing", "project-deleted"] as const) {
        const result = yield* Effect.result(
          locks.withLock(ProjectId.make(`${reason}-${index}`), Effect.fail(reason), Effect.void),
        );
        assert.equal(result._tag, "Failure");
      }
    }
    assert.equal(yield* locks.size, 0);
  }),
);

it.effect("rechecks availability in the pass after successful lock preflight", () =>
  Effect.gen(function* () {
    const locks = yield* makeAgentControlTaskReconcileLocks<ProjectId>();
    const projectId = ProjectId.make("availability-switch");
    let available = true;
    const result = yield* Effect.result(
      locks.withLock(
        projectId,
        Effect.sync(() => {
          assert.equal(available, true);
          available = false;
        }),
        Effect.suspend(() => (available ? Effect.succeed("ran") : Effect.fail("project-deleted"))),
      ),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure, "project-deleted");
    assert.equal(yield* locks.size, 1);
  }),
);
