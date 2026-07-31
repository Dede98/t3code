import { it } from "@effect/vitest";
import { describe, expect } from "vite-plus/test";
import * as EffectAcpErrors from "effect-acp/errors";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import {
  acpPermissionOutcome,
  mapAcpEffectToAdapterError,
  mapAcpToAdapterError,
} from "./AcpAdapterSupport.ts";

const cursor = ProviderDriverKind.make("cursor");
const grok = ProviderDriverKind.make("grok");
class SemanticAnnotation extends Context.Service<SemanticAnnotation, { readonly label: string }>()(
  "t3/provider/acp/AcpAdapterSupport.test/SemanticAnnotation",
) {}

function mappedExit(provider: typeof cursor, cause: Cause.Cause<EffectAcpErrors.AcpError>) {
  return Effect.exit(
    Effect.failCause(cause).pipe(
      mapAcpEffectToAdapterError(provider, "thread-1" as never, "session/prompt"),
    ),
  );
}

function expectMappedCause(
  provider: typeof cursor,
  cause: Cause.Cause<EffectAcpErrors.AcpError>,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const exit = yield* mappedExit(provider, cause);
    expect(Exit.isFailure(exit)).toBe(true);
    if (!Exit.isFailure(exit)) return;
    expect(exit.cause.reasons).toHaveLength(cause.reasons.length);
    for (const [index, actualReason] of exit.cause.reasons.entries()) {
      const expectedReason = cause.reasons[index]!;
      expect(actualReason._tag).toBe(expectedReason._tag);
      const actualAnnotations = new Map(actualReason.annotations);
      const expectedAnnotations = new Map(expectedReason.annotations);
      actualAnnotations.delete(Cause.StackTrace.key);
      expectedAnnotations.delete(Cause.StackTrace.key);
      expect(actualAnnotations).toEqual(expectedAnnotations);
      if (Cause.isFailReason(actualReason) && Cause.isFailReason(expectedReason)) {
        expect(actualReason.error._tag).toBe("ProviderAdapterRequestError");
        expect(actualReason.error.cause).toBe(expectedReason.error);
      } else if (Cause.isDieReason(actualReason) && Cause.isDieReason(expectedReason)) {
        expect(actualReason.defect).toBe(expectedReason.defect);
      } else if (Cause.isInterruptReason(actualReason) && Cause.isInterruptReason(expectedReason)) {
        expect(actualReason.fiberId).toBe(expectedReason.fiberId);
      }
    }
  });
}

function combinedCause(label: string): Cause.Cause<EffectAcpErrors.AcpError> {
  const annotation = { label };
  const annotations = Context.make(SemanticAnnotation, annotation);
  return Cause.fromReasons([
    Cause.makeFailReason(
      new EffectAcpErrors.AcpTransportError({
        operation: "call-rpc",
        detail: `${label} failure`,
        cause: new Error(`${label} failure origin`),
      }),
    ).annotate(annotations),
    Cause.makeDieReason(new Error(`${label} defect`)).annotate(annotations),
    Cause.makeInterruptReason(47_002).annotate(annotations),
  ]);
}

function failure(label: string): EffectAcpErrors.AcpTransportError {
  return new EffectAcpErrors.AcpTransportError({
    operation: "call-rpc",
    detail: `${label} failure`,
    cause: new Error(`${label} origin`),
  });
}

describe("AcpAdapterSupport", () => {
  it("maps ACP approval decisions to permission outcomes", () => {
    expect(acpPermissionOutcome("accept")).toBe("allow-once");
    expect(acpPermissionOutcome("acceptForSession")).toBe("allow-always");
    expect(acpPermissionOutcome("decline")).toBe("reject-once");
  });

  it("maps ACP request errors to provider adapter request errors", () => {
    const error = mapAcpToAdapterError(
      ProviderDriverKind.make("cursor"),
      "thread-1" as never,
      "session/prompt",
      new EffectAcpErrors.AcpRequestError({
        code: -32602,
        errorMessage: "Invalid params",
      }),
    );

    expect(error._tag).toBe("ProviderAdapterRequestError");
    expect(error.message).toContain("Invalid params");
  });

  it.effect("preserves a combined Cursor Cause while mapping only its Failure", () =>
    expectMappedCause(cursor, combinedCause("cursor-combined")),
  );

  it.effect("preserves individual Cursor Failure, Defect, and Interrupt Causes", () =>
    Effect.gen(function* () {
      const failure = new EffectAcpErrors.AcpTransportError({
        operation: "call-rpc",
        detail: "cursor failure",
        cause: new Error("cursor failure origin"),
      });
      yield* expectMappedCause(cursor, Cause.fromReasons([Cause.makeFailReason(failure)]));
      yield* expectMappedCause(
        cursor,
        Cause.fromReasons<EffectAcpErrors.AcpError>([
          Cause.makeDieReason(new Error("cursor defect")),
        ]),
      );
      yield* expectMappedCause(
        cursor,
        Cause.fromReasons<EffectAcpErrors.AcpError>([Cause.makeInterruptReason(47_001)]),
      );
    }),
  );

  it.effect("preserves a combined Grok Cause while mapping only its Failure", () =>
    expectMappedCause(grok, combinedCause("grok-combined")),
  );

  it.effect("maps two ordered Failures independently with their original objects nested", () =>
    Effect.gen(function* () {
      const first = failure("first");
      const second = failure("second");
      const cause = Cause.fromReasons([Cause.makeFailReason(first), Cause.makeFailReason(second)]);
      const exit = yield* mappedExit(cursor, cause);
      expect(Exit.isFailure(exit)).toBe(true);
      if (!Exit.isFailure(exit)) return;
      expect(exit.cause.reasons.map((reason) => reason._tag)).toEqual(["Fail", "Fail"]);
      const mappedFirst = exit.cause.reasons[0]!;
      const mappedSecond = exit.cause.reasons[1]!;
      expect(Cause.isFailReason(mappedFirst)).toBe(true);
      expect(Cause.isFailReason(mappedSecond)).toBe(true);
      if (Cause.isFailReason(mappedFirst) && Cause.isFailReason(mappedSecond)) {
        expect(mappedFirst.error.cause).toBe(first);
        expect(mappedSecond.error.cause).toBe(second);
      }
    }),
  );

  it.effect("preserves semantic and StackTrace annotations without creating a reason", () => {
    const semantic = Context.make(SemanticAnnotation, { label: "annotated" });
    const stackFrame = {
      name: "adapter-support-test",
      stack: () => undefined,
      parent: undefined,
    };
    const stackTrace = Context.makeUnsafe(
      new Map<string, unknown>([[Cause.StackTrace.key, stackFrame]]),
    );
    const original = failure("annotated");
    const cause = Cause.fromReasons([
      Cause.makeFailReason(original).annotate(semantic).annotate(stackTrace),
      Cause.makeDieReason(new Error("annotated defect")).annotate(semantic).annotate(stackTrace),
      Cause.makeInterruptReason(47_003).annotate(semantic).annotate(stackTrace),
    ]);
    return expectMappedCause(cursor, cause).pipe(
      Effect.andThen(
        Effect.gen(function* () {
          const exit = yield* mappedExit(cursor, cause);
          if (!Exit.isFailure(exit)) return;
          expect(exit.cause.reasons.map((reason) => reason._tag)).toEqual([
            "Fail",
            "Die",
            "Interrupt",
          ]);
          for (const reason of exit.cause.reasons) {
            expect(new Map(reason.annotations).get(Cause.StackTrace.key)).toBe(stackFrame);
          }
        }),
      ),
    );
  });
});
