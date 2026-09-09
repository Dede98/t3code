import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { ProviderAdmissionStore } from "../Services/ProviderAdmissionStore.ts";
import { ProviderAdmissionStoreLive } from "./ProviderAdmissionStore.ts";

it.effect("reopens after a usage refresh polls a provider without queued admissions", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Effect.gen(function* () {
      const store = yield* ProviderAdmissionStore;
      assert.isNull(
        yield* store.admitOldest({
          providerInstanceId: "codex",
          ownerId: "usage-refresh",
          now: "2026-09-09T12:00:00.000Z",
          leaseExpiresAt: "2026-09-09T12:01:00.000Z",
        }),
      );
    }).pipe(Effect.provide(ProviderAdmissionStoreLive));
    yield* Effect.service(ProviderAdmissionStore).pipe(
      Effect.provide(Layer.fresh(ProviderAdmissionStoreLive)),
    );
    yield* sql`UPDATE agent_control_provider_capacity_current SET revision=2 WHERE provider_instance_id='codex'`;
    const exit = yield* Effect.service(ProviderAdmissionStore).pipe(
      Effect.provide(Layer.fresh(ProviderAdmissionStoreLive)),
      Effect.exit,
    );
    assert.equal(exit._tag, "Failure");
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);
