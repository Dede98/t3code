import { assert, describe, it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Queue,
  Schema,
  SchemaTransformation,
  Scope,
  Stream,
} from "effect";
import { Rpc, RpcClient, RpcGroup, RpcSchema } from "effect/unstable/rpc";
import { RpcClientDefect, RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  FromClientEncoded,
  FromServerEncoded,
  RequestEncoded,
} from "effect/unstable/rpc/RpcMessage";
import { RequestId } from "effect/unstable/rpc/RpcMessage";

const UnaryRpc = Rpc.make("Unary", {
  payload: { label: Schema.String },
  success: Schema.Struct({ value: Schema.String }),
  error: Schema.String,
});

const NumbersRpc = Rpc.make("Numbers", {
  payload: { label: Schema.String },
  success: RpcSchema.Stream(Schema.Number, Schema.String),
});

const TestRpcs = RpcGroup.make(UnaryRpc, NumbersRpc);

type TestClient = RpcClient.RpcClient<typeof UnaryRpc | typeof NumbersRpc, RpcClientError>;

type SendRequest = (request: RequestEncoded) => Effect.Effect<void, RpcClientError>;

interface ProtocolHarness {
  readonly outbound: Queue.Queue<FromClientEncoded>;
  readonly protocol: RpcClient.Protocol["Service"];
  readonly respond: (message: FromServerEncoded) => Effect.Effect<void>;
}

const makeProtocolHarness = Effect.fn("makeProtocolHarness")(function* (sendRequest: SendRequest) {
  const outbound = yield* Queue.unbounded<FromClientEncoded>();
  const runReady = yield* Deferred.make<void>();
  let receive: ((message: FromServerEncoded) => Effect.Effect<void>) | undefined;

  const protocol = RpcClient.Protocol.of({
    run: (_clientId, handler) =>
      Effect.sync(() => {
        receive = handler;
      }).pipe(
        Effect.andThen(Deferred.succeed(runReady, undefined)),
        Effect.andThen(Effect.never),
        Effect.ensuring(
          Effect.sync(() => {
            receive = undefined;
          }),
        ),
      ),
    send: (_clientId, message) =>
      Queue.offer(outbound, message).pipe(
        Effect.andThen(message._tag === "Request" ? sendRequest(message) : Effect.void),
      ),
    supportsAck: false,
    supportsTransferables: false,
  });

  const respond = (message: FromServerEncoded) =>
    Deferred.await(runReady).pipe(
      Effect.andThen(
        Effect.suspend(() => {
          assert.isDefined(receive);
          return receive!(message);
        }),
      ),
    );

  return { outbound, protocol, respond } satisfies ProtocolHarness;
});

const requestIds = (...ids: ReadonlyArray<bigint>) => {
  let index = 0;
  return () => RequestId(ids[index++]!);
};

const makeClient = <Rpcs extends Rpc.Any>(
  group: RpcGroup.RpcGroup<Rpcs>,
  harness: ProtocolHarness,
  ids: ReadonlyArray<bigint>,
) =>
  RpcClient.make(group, {
    disableTracing: true,
    generateRequestId: requestIds(...ids),
  }).pipe(Effect.provideService(RpcClient.Protocol, harness.protocol));

const takeRequest = Effect.fn("takeRequest")(function* (harness: ProtocolHarness) {
  const message = yield* Queue.take(harness.outbound);
  assert.strictEqual(message._tag, "Request");
  return message as RequestEncoded;
});

const sendError = (label: string) =>
  new RpcClientError({
    reason: new RpcClientDefect({ message: label, cause: label }),
  });

const successResponse = (requestId: string, value: unknown): FromServerEncoded => ({
  _tag: "Exit",
  requestId,
  exit: { _tag: "Success", value },
});

const assertFailureCause = <E>(exit: Exit.Exit<unknown, unknown>, expected: Cause.Cause<E>) => {
  assert.isTrue(Exit.isFailure(exit));
  if (Exit.isFailure(exit)) {
    assert.strictEqual(exit.cause.reasons.length, expected.reasons.length);
    expected.reasons.forEach((expectedReason, index) => {
      const actualReason = exit.cause.reasons[index]!;
      assert.strictEqual(actualReason._tag, expectedReason._tag);
      assert.strictEqual(actualReason.annotations.size, expectedReason.annotations.size);
      for (const [key, value] of expectedReason.annotations) {
        assert.strictEqual(actualReason.annotations.get(key), value);
      }
      if (Cause.isFailReason(expectedReason)) {
        assert.isTrue(Cause.isFailReason(actualReason));
        if (Cause.isFailReason(actualReason)) {
          assert.strictEqual(actualReason.error, expectedReason.error);
        }
      } else if (Cause.isDieReason(expectedReason)) {
        assert.isTrue(Cause.isDieReason(actualReason));
        if (Cause.isDieReason(actualReason)) {
          assert.strictEqual(actualReason.defect, expectedReason.defect);
        }
      } else {
        assert.isTrue(Cause.isInterruptReason(actualReason));
        if (Cause.isInterruptReason(actualReason)) {
          assert.strictEqual(actualReason.fiberId, expectedReason.fiberId);
        }
      }
    });
  }
};

const makeObservedStruct = (value: unknown) => {
  let reads = 0;
  const encoded = Object.defineProperty({}, "value", {
    enumerable: true,
    get() {
      reads += 1;
      return value;
    },
  });
  return { encoded, reads: () => reads };
};

const runUnarySendFailure = Effect.fn("runUnarySendFailure")(function* (
  cause: Cause.Cause<RpcClientError>,
  requestId: bigint,
) {
  let transportCause: Cause.Cause<RpcClientError> | undefined;
  const harness = yield* makeProtocolHarness(() =>
    Effect.failCause(cause).pipe(
      Effect.tapCause((actual) =>
        Effect.sync(() => {
          transportCause = actual;
        }),
      ),
    ),
  );
  const client = yield* makeClient(TestRpcs, harness, [requestId]);
  const fiber = yield* client
    .Unary({ label: `request-${requestId}` })
    .pipe(Effect.forkChild({ startImmediately: true }));
  const request = yield* takeRequest(harness);
  const exit = yield* Fiber.await(fiber);
  assert.isDefined(transportCause);
  assertFailureCause(exit, transportCause!);
  return { harness, request, exit };
});

describe("Effect RPC request lifecycle cleanup", () => {
  it.effect("ignores a valid late success response after send failure", () =>
    Effect.gen(function* () {
      const cause = Cause.fail(sendError("late-success"));
      const { harness, request } = yield* runUnarySendFailure(cause, 101n);
      const observed = makeObservedStruct("late");

      yield* harness.respond(successResponse(request.id, observed.encoded));

      assert.strictEqual(observed.reads(), 0);
    }),
  );

  it.effect("ignores a valid late error response after send failure", () =>
    Effect.gen(function* () {
      const cause = Cause.fail(sendError("late-error"));
      const { harness, request } = yield* runUnarySendFailure(cause, 102n);
      let reads = 0;
      const reason: { readonly _tag: "Fail"; readonly error: unknown } = Object.defineProperty(
        { _tag: "Fail" as const },
        "error",
        {
          enumerable: true,
          get() {
            reads += 1;
            return "late-server-error";
          },
        },
      ) as { readonly _tag: "Fail"; readonly error: unknown };

      yield* harness.respond({
        _tag: "Exit",
        requestId: request.id,
        exit: { _tag: "Failure", cause: [reason] },
      });

      assert.strictEqual(reads, 0);
    }),
  );

  it.effect("preserves the identical send defect Cause", () =>
    Effect.gen(function* () {
      const defect = new Error("send-defect");
      const cause = Cause.die(defect) as Cause.Cause<RpcClientError>;

      yield* runUnarySendFailure(cause, 103n);
    }),
  );

  it.effect("preserves the identical send interrupt Cause and FiberId", () =>
    Effect.gen(function* () {
      const cause = Cause.interrupt(73_001) as Cause.Cause<RpcClientError>;
      const { exit } = yield* runUnarySendFailure(cause, 104n);

      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        const reason = exit.cause.reasons[0]!;
        assert.isTrue(Cause.isInterruptReason(reason));
        if (Cause.isInterruptReason(reason)) {
          assert.strictEqual(reason.fiberId, 73_001);
        }
      }
    }),
  );

  it.effect("preserves combined Cause order, identities, and annotations", () =>
    Effect.gen(function* () {
      class LifecycleAnnotation extends Context.Service<LifecycleAnnotation, string>()(
        "effect-acp/rpc-client-lifecycle.test/LifecycleAnnotation",
      ) {}

      const annotation = Context.make(LifecycleAnnotation, "combined-send");
      const failure = sendError("combined-failure");
      const defect = new Error("combined-defect");
      const reasons: ReadonlyArray<Cause.Reason<RpcClientError>> = [
        Cause.makeFailReason(failure).annotate(annotation),
        Cause.makeDieReason(defect).annotate(annotation) as Cause.Reason<RpcClientError>,
        Cause.makeInterruptReason(73_002).annotate(annotation) as Cause.Reason<RpcClientError>,
      ];
      const cause = Cause.fromReasons(reasons);
      const { exit } = yield* runUnarySendFailure(cause, 105n);

      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.deepEqual(
          exit.cause.reasons.map((reason) => reason._tag),
          ["Fail", "Die", "Interrupt"],
        );
        exit.cause.reasons.forEach((reason) => {
          assert.strictEqual(reason.annotations.get(LifecycleAnnotation.key), "combined-send");
        });
      }
    }),
  );

  it.effect("does not decode a schema-invalid late terminal response", () =>
    Effect.gen(function* () {
      const cause = Cause.fail(sendError("late-invalid"));
      const { harness, request } = yield* runUnarySendFailure(cause, 106n);
      const observed = makeObservedStruct(42);

      yield* harness.respond(successResponse(request.id, observed.encoded));

      assert.strictEqual(observed.reads(), 0);
    }),
  );

  it.effect("keeps the response winner when the delayed send failure loses", () =>
    Effect.gen(function* () {
      const firstSendStarted = yield* Deferred.make<void>();
      const releaseFirstSend = yield* Deferred.make<void>();
      const delayedCause = Cause.fail(sendError("response-wins"));
      let sends = 0;
      const harness = yield* makeProtocolHarness(() => {
        sends += 1;
        return sends === 1
          ? Deferred.succeed(firstSendStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFirstSend)),
              Effect.andThen(Effect.failCause(delayedCause)),
            )
          : Effect.void;
      });
      const client = yield* makeClient(TestRpcs, harness, [107n, 107n]);
      const first = yield* client
        .Unary({ label: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const firstRequest = yield* takeRequest(harness);
      yield* Deferred.await(firstSendStarted);

      yield* harness.respond(successResponse(firstRequest.id, { value: "first-ok" }));
      assert.deepEqual(yield* Fiber.join(first), { value: "first-ok" });

      const second = yield* client
        .Unary({ label: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const secondRequest = yield* takeRequest(harness);
      assert.strictEqual(secondRequest.id, firstRequest.id);
      yield* Deferred.succeed(releaseFirstSend, undefined);
      yield* harness.respond(successResponse(secondRequest.id, { value: "second-ok" }));

      assert.deepEqual(yield* Fiber.join(second), { value: "second-ok" });
    }),
  );

  it.effect("keeps the send winner and ignores the later response", () =>
    Effect.gen(function* () {
      const sendStarted = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const cause = Cause.fail(sendError("send-wins"));
      const harness = yield* makeProtocolHarness(() =>
        Deferred.succeed(sendStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseSend)),
          Effect.andThen(Effect.failCause(cause)),
        ),
      );
      const client = yield* makeClient(TestRpcs, harness, [108n]);
      const fiber = yield* client
        .Unary({ label: "send-wins" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const request = yield* takeRequest(harness);
      yield* Deferred.await(sendStarted);
      yield* Deferred.succeed(releaseSend, undefined);

      assertFailureCause(yield* Fiber.await(fiber), cause);
      const observed = makeObservedStruct("too-late");
      yield* harness.respond(successResponse(request.id, observed.encoded));
      assert.strictEqual(observed.reads(), 0);
    }),
  );

  it.effect("ends a permanently unanswered request when the client scope closes", () =>
    Effect.gen(function* () {
      const harness = yield* makeProtocolHarness(() => Effect.void);
      const outerScope = yield* Scope.Scope;
      const { fiber, request } = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeClient(TestRpcs, harness, [109n]);
          const fiber = yield* client
            .Unary({ label: "no-response" })
            .pipe(Effect.forkIn(outerScope, { startImmediately: true }));
          const request = yield* takeRequest(harness);
          return { fiber, request };
        }),
      );
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.isTrue(Cause.hasInterrupts(exit.cause));
      }
      assert.strictEqual(request.id, "109");
    }),
  );

  it.effect("keeps the client usable after a request-local send failure", () =>
    Effect.gen(function* () {
      const firstCause = Cause.fail(sendError("first-request"));
      const harness = yield* makeProtocolHarness((request) =>
        request.id === "110" ? Effect.failCause(firstCause) : Effect.void,
      );
      const client = yield* makeClient(TestRpcs, harness, [110n, 111n]);
      const first = yield* client
        .Unary({ label: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* takeRequest(harness);
      assertFailureCause(yield* Fiber.await(first), firstCause);

      const second = yield* client
        .Unary({ label: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const secondRequest = yield* takeRequest(harness);
      yield* harness.respond(successResponse(secondRequest.id, { value: "healthy" }));

      assert.deepEqual(yield* Fiber.join(second), { value: "healthy" });
    }),
  );

  it.effect("claims outer entries on ClientProtocolError", () =>
    Effect.gen(function* () {
      const harness = yield* makeProtocolHarness(() => Effect.never);
      const client = yield* makeClient(TestRpcs, harness, [112n]);
      const fiber = yield* client
        .Unary({ label: "protocol-error" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const request = yield* takeRequest(harness);
      const protocolError = sendError("client-protocol-error");

      yield* harness.respond({ _tag: "ClientProtocolError", error: protocolError });

      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        const reason = exit.cause.reasons[0]!;
        assert.isTrue(Cause.isFailReason(reason));
        if (Cause.isFailReason(reason)) {
          assert.strictEqual(reason.error, protocolError);
        }
      }
      const observed = makeObservedStruct("late-protocol-response");
      yield* harness.respond(successResponse(request.id, observed.encoded));
      assert.strictEqual(observed.reads(), 0);
    }),
  );

  it.effect("preserves stream chunk order through the terminal exit", () =>
    Effect.gen(function* () {
      const harness = yield* makeProtocolHarness(() => Effect.void);
      const client = (yield* makeClient(TestRpcs, harness, [113n])) as TestClient;
      const fiber = yield* client
        .Numbers({ label: "chunks" })
        .pipe(Stream.runCollect, Effect.forkChild({ startImmediately: true }));
      const request = yield* takeRequest(harness);

      yield* harness.respond({
        _tag: "Chunk",
        requestId: request.id,
        values: [1, 2],
      });
      yield* harness.respond({
        _tag: "Chunk",
        requestId: request.id,
        values: [3, 4],
      });
      yield* harness.respond(successResponse(request.id, null));

      assert.deepEqual(Array.from(yield* Fiber.join(fiber)), [1, 2, 3, 4]);
    }),
  );

  it.effect("ends a stream with the identical send-failure Cause", () =>
    Effect.gen(function* () {
      const cause = Cause.fail(sendError("stream-send-failure"));
      const harness = yield* makeProtocolHarness(() => Effect.failCause(cause));
      const client = (yield* makeClient(TestRpcs, harness, [114n])) as TestClient;
      const fiber = yield* client
        .Numbers({ label: "stream-failure" })
        .pipe(Stream.runCollect, Effect.forkChild({ startImmediately: true }));
      const request = yield* takeRequest(harness);

      assertFailureCause(yield* Fiber.await(fiber), cause);
      assert.strictEqual(yield* Queue.size(harness.outbound), 0);

      let lateChunkReads = 0;
      const lateValues = [9];
      Object.defineProperty(lateValues, 0, {
        get() {
          lateChunkReads += 1;
          return 9;
        },
      });
      yield* harness.respond({
        _tag: "Chunk",
        requestId: request.id,
        values: lateValues as [number],
      });
      yield* harness.respond(successResponse(request.id, null));
      assert.strictEqual(lateChunkReads, 0);
      assert.strictEqual(yield* Queue.size(harness.outbound), 0);
    }),
  );

  it.effect("keeps scope-close authoritative during controlled response decode", () =>
    Effect.gen(function* () {
      const decodeStarted = yield* Deferred.make<void>();
      const releaseDecode = yield* Deferred.make<void>();
      const ControlledSuccess = Schema.String.pipe(
        Schema.decodeTo(
          Schema.String,
          SchemaTransformation.transformOrFail({
            decode: (value) =>
              Deferred.succeed(decodeStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseDecode)),
                Effect.as(value),
              ),
            encode: Effect.succeed,
          }),
        ),
      );
      const ControlledRpc = Rpc.make("Controlled", {
        payload: { label: Schema.String },
        success: ControlledSuccess,
      });
      const ControlledRpcs = RpcGroup.make(ControlledRpc);
      const harness = yield* makeProtocolHarness(() => Effect.never);
      const clientScope = yield* Scope.make();
      const client = yield* makeClient(ControlledRpcs, harness, [115n]).pipe(
        Effect.provideService(Scope.Scope, clientScope),
      );
      const requestFiber = yield* client
        .Controlled({ label: "controlled-decode" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const request = yield* takeRequest(harness);
      const responseFiber = yield* harness
        .respond(successResponse(request.id, "decoded"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(decodeStarted);

      yield* Scope.close(clientScope, Exit.void);
      const requestExit = yield* Fiber.await(requestFiber);
      assert.isTrue(Exit.isFailure(requestExit));
      if (Exit.isFailure(requestExit)) {
        assert.isTrue(Cause.hasInterrupts(requestExit.cause));
      }

      yield* Deferred.succeed(releaseDecode, undefined);
      yield* Fiber.join(responseFiber);
      assert.deepEqual(yield* Fiber.await(requestFiber), requestExit);
    }),
  );

  for (const order of ["cleanup-first", "response-first", "simultaneous"] as const) {
    it.effect(`does not let old unary cleanup claim a reused id (${order})`, () =>
      Effect.gen(function* () {
        const firstExitClaimed = yield* Deferred.make<void>();
        const releaseFirstExit = yield* Deferred.make<void>();
        let exitClaims = 0;
        const hooks = RpcClient.RequestHooks.of({
          onRequestExit: () => {
            exitClaims += 1;
            return exitClaims === 1
              ? Deferred.succeed(firstExitClaimed, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseFirstExit)),
                )
              : Effect.void;
          },
        });
        const harness = yield* makeProtocolHarness(() => Effect.void);
        const client = yield* makeClient(TestRpcs, harness, [116n, 116n]).pipe(
          Effect.provideService(RpcClient.RequestHooks, hooks),
        );
        const first = yield* client
          .Unary({ label: "old" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        const firstRequest = yield* takeRequest(harness);
        const firstResponse = yield* harness
          .respond(successResponse(firstRequest.id, { value: "old-ok" }))
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(firstExitClaimed);

        const second = yield* client
          .Unary({ label: "new" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        const secondRequest = yield* takeRequest(harness);
        assert.strictEqual(secondRequest.id, firstRequest.id);
        const respondSecond = harness.respond(
          successResponse(secondRequest.id, { value: "new-ok" }),
        );
        let cleanupFiber: Fiber.Fiber<unknown, never>;
        let secondResponseFiber: Fiber.Fiber<unknown, never>;
        if (order === "cleanup-first") {
          cleanupFiber = yield* Fiber.interrupt(first).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          secondResponseFiber = yield* respondSecond.pipe(
            Effect.forkChild({ startImmediately: true }),
          );
        } else if (order === "response-first") {
          secondResponseFiber = yield* respondSecond.pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          cleanupFiber = yield* Fiber.interrupt(first).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
        } else {
          cleanupFiber = yield* Fiber.interrupt(first).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          secondResponseFiber = yield* respondSecond.pipe(
            Effect.forkChild({ startImmediately: true }),
          );
        }

        yield* Deferred.succeed(releaseFirstExit, undefined);
        yield* Fiber.join(cleanupFiber);
        yield* Fiber.join(secondResponseFiber);
        yield* Fiber.join(firstResponse);
        assert.deepEqual(yield* Fiber.join(second), { value: "new-ok" });
        assert.equal(yield* Queue.size(harness.outbound), 0);
        assert.equal(exitClaims, 2);
      }),
    );

    it.effect(`does not let old stream scope cleanup claim a reused id (${order})`, () =>
      Effect.gen(function* () {
        const firstExitClaimed = yield* Deferred.make<void>();
        const releaseFirstExit = yield* Deferred.make<void>();
        let exitClaims = 0;
        const hooks = RpcClient.RequestHooks.of({
          onRequestExit: () => {
            exitClaims += 1;
            return exitClaims === 1
              ? Deferred.succeed(firstExitClaimed, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseFirstExit)),
                )
              : Effect.void;
          },
        });
        const harness = yield* makeProtocolHarness(() => Effect.void);
        const client = (yield* makeClient(TestRpcs, harness, [117n, 117n]).pipe(
          Effect.provideService(RpcClient.RequestHooks, hooks),
        )) as TestClient;
        const oldScope = yield* Scope.make();
        const newScope = yield* Scope.make();
        const first = yield* client
          .Numbers({ label: "old-stream" })
          .pipe(
            Stream.runCollect,
            Effect.provideService(Scope.Scope, oldScope),
            Effect.forkChild({ startImmediately: true }),
          );
        const firstRequest = yield* takeRequest(harness);
        const firstTerminal = yield* harness
          .respond(successResponse(firstRequest.id, null))
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(firstExitClaimed);

        const second = yield* client
          .Numbers({ label: "new-stream" })
          .pipe(
            Stream.runCollect,
            Effect.provideService(Scope.Scope, newScope),
            Effect.forkChild({ startImmediately: true }),
          );
        const secondRequest = yield* takeRequest(harness);
        assert.strictEqual(secondRequest.id, firstRequest.id);
        const respondSecond = harness
          .respond({ _tag: "Chunk", requestId: secondRequest.id, values: [1, 2] })
          .pipe(Effect.andThen(harness.respond(successResponse(secondRequest.id, null))));
        let cleanupFiber: Fiber.Fiber<unknown, never>;
        let secondResponseFiber: Fiber.Fiber<unknown, never>;
        if (order === "cleanup-first") {
          cleanupFiber = yield* Scope.close(oldScope, Exit.void).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          secondResponseFiber = yield* respondSecond.pipe(
            Effect.forkChild({ startImmediately: true }),
          );
        } else if (order === "response-first") {
          secondResponseFiber = yield* respondSecond.pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          cleanupFiber = yield* Scope.close(oldScope, Exit.void).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
        } else {
          cleanupFiber = yield* Scope.close(oldScope, Exit.void).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          secondResponseFiber = yield* respondSecond.pipe(
            Effect.forkChild({ startImmediately: true }),
          );
        }

        yield* Deferred.succeed(releaseFirstExit, undefined);
        yield* Fiber.join(cleanupFiber);
        yield* Fiber.join(secondResponseFiber);
        yield* Fiber.join(firstTerminal);
        assert.deepEqual(Array.from(yield* Fiber.join(second)), [1, 2]);
        assert.equal(yield* Queue.size(harness.outbound), 0);
        assert.deepEqual(Array.from(yield* Fiber.join(first)), []);
        assert.equal(exitClaims, 2);
        yield* Scope.close(newScope, Exit.void);
      }),
    );
  }
});
