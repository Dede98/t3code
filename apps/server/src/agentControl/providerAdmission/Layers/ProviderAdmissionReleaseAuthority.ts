import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ProviderAdmissionReleaseAuthority } from "../Services/ProviderAdmissionReleaseAuthority.ts";
import { ProviderAdmissionRuntime } from "../Services/ProviderAdmissionRuntime.ts";
import { ProviderAdmissionStore } from "../Services/ProviderAdmissionStore.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

const make = Effect.gen(function* () {
  const store = yield* ProviderAdmissionStore;
  const runtime = yield* ProviderAdmissionRuntime;

  const recover = Effect.gen(function* () {
    const released = yield* store.catchUpFinalized;
    yield* Effect.forEach(released, runtime.capacityReleased, { discard: true });
    const observedAt = yield* nowIso;
    const entered = yield* store.listEnteredWithoutRelease;
    yield* Effect.forEach(
      entered,
      (permit) =>
        store.quarantine({
          permit,
          reason: "owner-lost-after-entry",
          observedAt,
        }),
      { discard: true },
    );
  });

  return ProviderAdmissionReleaseAuthority.of({
    releaseInTransaction: store.releaseFromFinalizationInTransaction,
    signalCommitted: (providerInstanceId) =>
      providerInstanceId === null ? Effect.void : runtime.capacityReleased(providerInstanceId),
    recover,
  });
});

export const ProviderAdmissionReleaseAuthorityLive = Layer.effect(
  ProviderAdmissionReleaseAuthority,
  make,
);
