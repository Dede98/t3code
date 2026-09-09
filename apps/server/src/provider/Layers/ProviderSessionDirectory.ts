import { ProviderDriverKind, type ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { increment, providerSessionBindingsQuarantinedTotal } from "../../observability/Metrics.ts";
import { ProviderSessionDirectoryPersistenceError } from "../Errors.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
  type ProviderRuntimeBindingWithMetadata,
  type ProviderSessionDirectoryShape,
} from "../Services/ProviderSessionDirectory.ts";
const decodeProviderDriverKindValue = Schema.decodeUnknownEffect(ProviderDriverKind);

function toPersistenceError(operation: string) {
  return (cause: unknown) =>
    new ProviderSessionDirectoryPersistenceError({
      operation,
      detail: `Failed to execute ${operation}.`,
      cause,
    });
}

function decodeProviderDriverKind(
  providerName: string,
  operation: string,
): Effect.Effect<ProviderDriverKind, ProviderSessionDirectoryPersistenceError> {
  return decodeProviderDriverKindValue(providerName).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderSessionDirectoryPersistenceError({
          operation,
          detail: `Unknown persisted provider '${providerName}'.`,
          reason: "binding-unknown-provider-driver",
          cause,
        }),
    ),
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function mergeRuntimePayload(
  existing: unknown | null,
  next: unknown | null | undefined,
): unknown | null {
  if (next === undefined) {
    return existing ?? null;
  }
  if (isRecord(existing) && isRecord(next)) {
    return { ...existing, ...next };
  }
  return next;
}

function toRuntimeBinding(
  runtime: ProviderSessionRuntime.ProviderSessionRuntime,
  operation: string,
): Effect.Effect<ProviderRuntimeBindingWithMetadata, ProviderSessionDirectoryPersistenceError> {
  const providerInstanceId = runtime.providerInstanceId;
  if (providerInstanceId === null) {
    return Effect.fail(
      new ProviderSessionDirectoryPersistenceError({
        operation,
        detail: `Persisted provider binding for thread '${runtime.threadId}' has no provider instance id and cannot be routed safely.`,
        reason: "binding-missing-provider-instance-id",
      }),
    );
  }
  return decodeProviderDriverKind(runtime.providerName, operation).pipe(
    Effect.map(
      (provider) =>
        ({
          threadId: runtime.threadId,
          provider,
          providerInstanceId,
          adapterKey: runtime.adapterKey,
          runtimeMode: runtime.runtimeMode,
          status: runtime.status,
          resumeCursor: runtime.resumeCursor,
          runtimePayload: runtime.runtimePayload,
          lastSeenAt: runtime.lastSeenAt,
        }) satisfies ProviderRuntimeBindingWithMetadata,
    ),
  );
}

const makeProviderSessionDirectory = Effect.gen(function* () {
  const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

  const getBinding = (threadId: ThreadId) =>
    repository.getByThreadId({ threadId }).pipe(
      Effect.mapError(toPersistenceError("ProviderSessionDirectory.getBinding:getByThreadId")),
      Effect.flatMap((runtime) =>
        Option.match(runtime, {
          onNone: () => Effect.succeed(Option.none<ProviderRuntimeBinding>()),
          onSome: (value) =>
            toRuntimeBinding(value, "ProviderSessionDirectory.getBinding").pipe(
              Effect.map((binding) => Option.some(binding)),
            ),
        }),
      ),
    );

  const upsert: ProviderSessionDirectoryShape["upsert"] = Effect.fn(function* (binding, options) {
    const existing = yield* repository
      .getByThreadId({ threadId: binding.threadId })
      .pipe(Effect.mapError(toPersistenceError("ProviderSessionDirectory.upsert:getByThreadId")));

    const existingRuntime = Option.getOrUndefined(existing);

    const now = DateTime.formatIso(yield* DateTime.now);
    const providerChanged =
      existingRuntime !== undefined && existingRuntime.providerName !== binding.provider;
    yield* repository
      .upsert(
        {
          threadId: binding.threadId,
          providerName: binding.provider,
          providerInstanceId: binding.providerInstanceId,
          adapterKey:
            binding.adapterKey ??
            (providerChanged
              ? binding.provider
              : (existingRuntime?.adapterKey ?? binding.provider)),
          runtimeMode: binding.runtimeMode ?? existingRuntime?.runtimeMode ?? "full-access",
          status: binding.status ?? existingRuntime?.status ?? "running",
          lastSeenAt: now,
          resumeCursor:
            binding.resumeCursor !== undefined
              ? binding.resumeCursor
              : (existingRuntime?.resumeCursor ?? null),
          runtimePayload: mergeRuntimePayload(
            existingRuntime?.runtimePayload ?? null,
            binding.runtimePayload,
          ),
        },
        options,
      )
      .pipe(Effect.mapError(toPersistenceError("ProviderSessionDirectory.upsert:upsert")));
  });

  const getProvider: ProviderSessionDirectoryShape["getProvider"] = (threadId) =>
    getBinding(threadId).pipe(
      Effect.flatMap((binding) =>
        Option.match(binding, {
          onSome: (value) => Effect.succeed(value.provider),
          onNone: () =>
            Effect.fail(
              new ProviderSessionDirectoryPersistenceError({
                operation: "ProviderSessionDirectory.getProvider",
                detail: `No persisted provider binding found for thread '${threadId}'.`,
              }),
            ),
        }),
      ),
    );

  const recordImportedTranscript: ProviderSessionDirectoryShape["recordImportedTranscript"] = (
    input,
  ) =>
    repository
      .recordImportedTranscript(input)
      .pipe(
        Effect.mapError(toPersistenceError("ProviderSessionDirectory.recordImportedTranscript")),
      );

  const listThreadIds: ProviderSessionDirectoryShape["listThreadIds"] = () =>
    repository.list().pipe(
      Effect.mapError(toPersistenceError("ProviderSessionDirectory.listThreadIds:list")),
      Effect.map((rows) => rows.map((row) => row.threadId)),
    );

  const listBindings: ProviderSessionDirectoryShape["listBindings"] = () =>
    repository.list().pipe(
      Effect.mapError(toPersistenceError("ProviderSessionDirectory.listBindings:list")),
      Effect.flatMap((rows) =>
        Effect.forEach(
          rows,
          (row) =>
            toRuntimeBinding(row, "ProviderSessionDirectory.listBindings").pipe(
              Effect.map(Option.some),
              Effect.catch((error) =>
                Effect.logWarning("provider.session.binding.quarantined", {
                  threadId: row.threadId,
                  persistedProvider: row.providerName,
                  operation: "list-bindings",
                  reason: error.reason,
                  detail: error.detail,
                }).pipe(
                  Effect.andThen(
                    increment(providerSessionBindingsQuarantinedTotal, {
                      operation: "list-bindings",
                      reason: error.reason ?? "decode-failed",
                    }),
                  ),
                  Effect.as(Option.none<ProviderRuntimeBindingWithMetadata>()),
                ),
              ),
            ),
          { concurrency: "unbounded" },
        ),
      ),
      Effect.map((bindings) =>
        bindings.flatMap((binding) =>
          Option.match(binding, {
            onNone: () => [],
            onSome: (value) => [value],
          }),
        ),
      ),
    );

  return {
    upsert,
    recordImportedTranscript,
    getProvider,
    getBinding,
    listThreadIds,
    listBindings,
  } satisfies ProviderSessionDirectoryShape;
});

export const ProviderSessionDirectoryLive = Layer.effect(
  ProviderSessionDirectory,
  makeProviderSessionDirectory,
);
