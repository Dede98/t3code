// @effect-diagnostics nodeBuiltinImport:off - this ownership test kills only its captured child and awaits its exit event.
import * as NodeChildProcess from "node:child_process";
import * as NodeOS from "node:os";
import { AgentControlStageRunLeaseHolderId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { acquireStageRunLeaseRuntimeOwnership } from "./runtimeOwnership.ts";

const acquire = (name: string) =>
  acquireStageRunLeaseRuntimeOwnership({
    holderId: AgentControlStageRunLeaseHolderId.make(`holder-${name}`),
    ownerToken: `incarnation-${name}`,
  });

it.effect("keeps live competing runtimes foreign and reclaims only a closed holder", () =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const original = yield* acquire("original").pipe(Effect.provideService(Scope.Scope, scope));
    const concurrent = yield* acquire("concurrent");
    assert.notEqual(original, concurrent);
    yield* Scope.close(scope, Exit.void);
    const restarted = yield* acquire("restarted");
    assert.equal(restarted, original);
    assert.notEqual(yield* acquire("later-competitor"), restarted);
  }).pipe(Effect.scoped, Effect.provide(SqlitePersistenceMemory)),
);

it.effect("reclaims a registered holder after its actual process exits without releasing", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const child = NodeChildProcess.spawn(process.execPath, ["-e", "process.stdin.resume()"], {
      stdio: "pipe",
    });
    const exited = new Promise<void>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", () => resolve());
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        child.kill("SIGKILL");
      }),
    );
    assert.isNumber(child.pid);
    const oldHolder = AgentControlStageRunLeaseHolderId.make("holder-crashed");
    yield* sql`
      INSERT INTO agent_control_lease_runtime_owners
        (holder_id, owner_token, hostname, pid, status)
      VALUES (${oldHolder}, 'incarnation-crashed', ${NodeOS.hostname()}, ${child.pid!}, 'active')
    `;
    assert.notEqual(yield* acquire("while-alive"), oldHolder);
    child.kill("SIGKILL");
    yield* Effect.promise(() => exited);
    assert.equal(yield* acquire("after-crash"), oldHolder);
    assert.deepStrictEqual(
      yield* sql`
      SELECT owner_token, pid, status FROM agent_control_lease_runtime_owners
      WHERE holder_id = ${oldHolder}
    `,
      [{ owner_token: "incarnation-after-crash", pid: process.pid, status: "active" }],
    );
  }).pipe(Effect.scoped, Effect.provide(SqlitePersistenceMemory)),
);

it.effect("prunes ambiguous idle holders without claiming a foreign-host lineage", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const [holder, hostname, status] of [
      ["first", NodeOS.hostname(), "closed"],
      ["second", NodeOS.hostname(), "closed"],
      ["foreign", "other-host", "closed"],
    ]) {
      yield* sql`
        INSERT INTO agent_control_lease_runtime_owners
          (holder_id, owner_token, hostname, pid, status)
        VALUES (${holder!}, ${holder!}, ${hostname!}, ${process.pid}, ${status!})
      `;
    }
    assert.equal(yield* acquire("ambiguous"), "holder-ambiguous");
    assert.deepStrictEqual(
      yield* sql`
      SELECT holder_id FROM agent_control_lease_runtime_owners WHERE status = 'closed'
      ORDER BY holder_id
    `,
      [{ holder_id: "foreign" }],
    );
  }).pipe(Effect.scoped, Effect.provide(SqlitePersistenceMemory)),
);

it.effect("an old scope finalizer cannot release a newer incarnation", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const holder = yield* acquire("stale").pipe(Effect.provideService(Scope.Scope, scope));
    yield* sql`
      UPDATE agent_control_lease_runtime_owners SET owner_token = 'newer-incarnation'
      WHERE holder_id = ${holder}
    `;
    yield* Scope.close(scope, Exit.void);
    assert.deepStrictEqual(
      yield* sql`
      SELECT status FROM agent_control_lease_runtime_owners WHERE holder_id = ${holder}
    `,
      [{ status: "active" }],
    );
  }).pipe(Effect.scoped, Effect.provide(SqlitePersistenceMemory)),
);
