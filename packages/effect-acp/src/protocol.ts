import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Stdio from "effect/Stdio";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcClientError from "effect/unstable/rpc/RpcClientError";
import * as RpcMessage from "effect/unstable/rpc/RpcMessage";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import * as AcpSchema from "./_generated/schema.gen.ts";
import { CLIENT_METHODS } from "./_generated/meta.gen.ts";
import * as AcpError from "./errors.ts";
const isAcpError = Schema.is(AcpError.AcpError);

const mapCauseFailuresPreservingReasons = <E, E2>(
  cause: Cause.Cause<E>,
  mapFailure: (error: E) => E2,
): Cause.Cause<E2> =>
  Cause.fromReasons(
    cause.reasons.map((reason) => {
      if (!Cause.isFailReason(reason)) return reason;
      const mapped = mapFailure(reason.error);
      return (mapped as unknown) === reason.error
        ? (reason as unknown as Cause.Fail<E2>)
        : Cause.makeFailReason(mapped).annotate(Context.makeUnsafe(reason.annotations));
    }),
  );

export interface AcpProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly stage: "raw" | "decoded" | "decode_failed" | "enqueued";
  readonly payload: unknown;
}

export type AcpIncomingNotification =
  | {
      readonly _tag: "SessionUpdate";
      readonly method: typeof CLIENT_METHODS.session_update;
      readonly params: AcpSchema.SessionNotification;
    }
  | {
      readonly _tag: "ElicitationComplete";
      readonly method: typeof CLIENT_METHODS.session_elicitation_complete;
      readonly params: AcpSchema.ElicitationCompleteNotification;
    }
  | {
      readonly _tag: "ExtNotification";
      readonly method: string;
      readonly params: unknown;
    };

export interface AcpPatchedProtocolOptions {
  readonly stdio: Stdio.Stdio;
  readonly terminationError?: Effect.Effect<AcpError.AcpError>;
  readonly serverRequestMethods: ReadonlySet<string>;
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  readonly logger?: (event: AcpProtocolLogEvent) => Effect.Effect<void, never>;
  readonly onNotification?: (
    notification: AcpIncomingNotification,
  ) => Effect.Effect<void, AcpError.AcpError, never>;
  readonly onExtRequest?: (
    method: string,
    params: unknown,
  ) => Effect.Effect<unknown, AcpError.AcpError, never>;
  readonly onTermination?: (cause: AcpTransportCause) => Effect.Effect<void, never, never>;
  /** @internal Optional production-resource snapshot used by native lifecycle tests. */
  readonly onDebugLifecycleSnapshot?: (
    snapshot: Effect.Effect<AcpProtocolDebugLifecycleSnapshot>,
  ) => Effect.Effect<void, never>;
}

export type AcpTransportCause = Cause.Cause<AcpError.AcpError>;

export interface AcpPatchedProtocol {
  readonly clientProtocol: RpcClient.Protocol["Service"];
  readonly serverProtocol: RpcServer.Protocol["Service"];
  readonly incoming: Stream.Stream<AcpIncomingNotification>;
  readonly request: (method: string, payload: unknown) => Effect.Effect<unknown, AcpError.AcpError>;
  readonly notify: (method: string, payload: unknown) => Effect.Effect<void, AcpError.AcpError>;
  readonly getTerminalCause: Effect.Effect<AcpTransportCause | undefined>;
  readonly withOutgoingAck: <A, R>(
    method: string,
    outgoingAck: Deferred.Deferred<AcpOutgoingRequestEvidence, AcpError.AcpError>,
    effect: Effect.Effect<A, AcpError.AcpError, R>,
  ) => Effect.Effect<A, AcpError.AcpError, R>;
}

export interface AcpOutgoingRequestEvidence {
  readonly method: string;
  readonly requestId: string;
}

/** @internal Immutable view of the real protocol request/deferred lifecycle. */
export interface AcpProtocolDebugLifecycleSnapshot {
  readonly pendingRequestIds: ReadonlyArray<string>;
  readonly pendingResponseDeferreds: ReadonlyArray<string>;
  readonly pendingOutgoingAckDeferreds: ReadonlyArray<string>;
  readonly enqueuedRequestIds: ReadonlyArray<string>;
  readonly completedRequestIds: ReadonlyArray<string>;
  readonly successfulResponseRequestIds: ReadonlyArray<string>;
  readonly queueEnded: boolean;
  readonly protocolEnded: boolean;
}

interface AcpOutgoingAckRegistration {
  readonly method: string;
  readonly outgoingAck: Deferred.Deferred<AcpOutgoingRequestEvidence, AcpError.AcpError>;
}

class CurrentOutgoingAck extends Context.Reference<AcpOutgoingAckRegistration | undefined>(
  "effect-acp/protocol/CurrentOutgoingAck",
  {
    defaultValue: () => undefined,
  },
) {}

interface AcpPendingRequest {
  readonly deferred: Deferred.Deferred<unknown, AcpError.AcpError>;
  readonly method: string;
  readonly source: "extension" | "rpc-debug";
  readonly outgoingAck?: Deferred.Deferred<AcpOutgoingRequestEvidence, AcpError.AcpError>;
  readonly enqueued?: boolean;
}

const decodeSessionUpdate = Schema.decodeUnknownEffect(AcpSchema.SessionNotification);
const decodeElicitationComplete = Schema.decodeUnknownEffect(
  AcpSchema.ElicitationCompleteNotification,
);
const parserFactory = RpcSerialization.ndJsonRpc();

export const makeAcpPatchedProtocol = Effect.fn("makeAcpPatchedProtocol")(function* (
  options: AcpPatchedProtocolOptions,
): Effect.fn.Return<AcpPatchedProtocol, never, Scope.Scope> {
  const parser = parserFactory.makeUnsafe();
  const serverQueue = yield* Queue.unbounded<RpcMessage.FromClientEncoded>();
  const clientQueue = yield* Queue.unbounded<RpcMessage.FromServerEncoded>();
  const notificationQueue = yield* Queue.unbounded<AcpIncomingNotification>();
  const disconnects = yield* Queue.unbounded<number>();
  const outgoing = yield* Queue.unbounded<string | Uint8Array, Cause.Done<void>>();
  const nextRequestId = yield* Ref.make(1n);
  const terminalCause = yield* Ref.make<AcpTransportCause | undefined>(undefined);
  const extPending = yield* Ref.make(new Map<string, AcpPendingRequest>());
  const completedRequestIds = yield* Ref.make(new Set<string>());
  const successfulResponseRequestIds = yield* Ref.make(new Set<string>());
  const recordDebugCompletion = (requestId: string, successful: boolean) =>
    options.onDebugLifecycleSnapshot === undefined
      ? Effect.void
      : Ref.update(completedRequestIds, (ids) => new Set(ids).add(requestId)).pipe(
          Effect.andThen(
            successful
              ? Ref.update(successfulResponseRequestIds, (ids) => new Set(ids).add(requestId))
              : Effect.void,
          ),
        );

  const debugLifecycleSnapshot: Effect.Effect<AcpProtocolDebugLifecycleSnapshot> = Effect.gen(
    function* () {
      const pending = yield* Ref.get(extPending);
      const completed = yield* Ref.get(completedRequestIds);
      const successful = yield* Ref.get(successfulResponseRequestIds);
      const terminal = yield* Ref.get(terminalCause);
      const sorted = (values: Iterable<string>) => [...values].sort();
      return {
        pendingRequestIds: sorted(pending.keys()),
        pendingResponseDeferreds: sorted(
          [...pending].flatMap(([requestId, request]) =>
            Deferred.isDoneUnsafe(request.deferred) ? [] : [requestId],
          ),
        ),
        pendingOutgoingAckDeferreds: sorted(
          [...pending].flatMap(([requestId, request]) =>
            request.outgoingAck !== undefined && !Deferred.isDoneUnsafe(request.outgoingAck)
              ? [requestId]
              : [],
          ),
        ),
        enqueuedRequestIds: sorted(
          [...pending].flatMap(([requestId, request]) => (request.enqueued ? [requestId] : [])),
        ),
        completedRequestIds: sorted(completed),
        successfulResponseRequestIds: sorted(successful),
        queueEnded: outgoing.state._tag === "Done",
        protocolEnded: terminal !== undefined,
      };
    },
  );

  if (options.onDebugLifecycleSnapshot !== undefined) {
    yield* options.onDebugLifecycleSnapshot(debugLifecycleSnapshot);
  }

  const registerDebugRpcRequest = Effect.fn("registerDebugRpcRequest")(function* (
    message: RpcMessage.RequestEncoded,
  ) {
    if (options.onDebugLifecycleSnapshot === undefined || message.id === "") return;
    const deferred = yield* Deferred.make<unknown, AcpError.AcpError>();
    const outgoingAck = yield* Deferred.make<AcpOutgoingRequestEvidence, AcpError.AcpError>();
    yield* Ref.update(extPending, (pending) => {
      const existing = pending.get(message.id);
      if (existing !== undefined) {
        if (existing.source !== "extension" || existing.method !== message.tag) {
          throw new Error(`ACP protocol request id '${message.id}' is already pending.`);
        }
        return new Map(pending).set(message.id, {
          ...existing,
          outgoingAck,
          enqueued: false,
        });
      }
      return new Map(pending).set(message.id, {
        deferred,
        method: message.tag,
        source: "rpc-debug",
        outgoingAck,
        enqueued: false,
      });
    });
  });

  const markDebugRpcEnqueued = (requestId: string, method: string) =>
    Ref.modify(extPending, (pending) => {
      const request = pending.get(requestId);
      if (request?.outgoingAck === undefined) {
        return [Effect.void, pending] as const;
      }
      const next = new Map(pending).set(requestId, { ...request, enqueued: true });
      return [
        Deferred.succeed(request.outgoingAck, { requestId, method }).pipe(Effect.asVoid),
        next,
      ] as const;
    }).pipe(Effect.flatten);

  const completeDebugRpcFailure = (requestId: string, cause: AcpTransportCause) =>
    Ref.modify(extPending, (pending) => {
      const request = pending.get(requestId);
      if (request?.outgoingAck === undefined) return [Effect.void, pending] as const;
      const next = new Map(pending);
      next.delete(requestId);
      return [
        Ref.update(completedRequestIds, (ids) => new Set(ids).add(requestId)).pipe(
          Effect.andThen(Deferred.failCause(request.deferred, cause)),
          Effect.andThen(
            request.outgoingAck === undefined
              ? Effect.void
              : Deferred.failCause(request.outgoingAck, cause).pipe(Effect.asVoid),
          ),
          Effect.asVoid,
        ),
        next,
      ] as const;
    }).pipe(Effect.flatten);

  const logProtocol = (event: AcpProtocolLogEvent) => {
    if (event.direction === "incoming" && !options.logIncoming) {
      return Effect.void;
    }
    if (event.direction === "outgoing" && !options.logOutgoing) {
      return Effect.void;
    }
    return (
      options.logger?.(event) ??
      Effect.logDebug("ACP protocol event").pipe(Effect.annotateLogs({ event }))
    );
  };

  const offerOutgoing = Effect.fn("offerOutgoing")(function* (
    message: RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded,
  ) {
    const debugRequest =
      options.onDebugLifecycleSnapshot !== undefined &&
      message._tag === "Request" &&
      message.id !== ""
        ? message
        : undefined;
    if (debugRequest !== undefined) {
      yield* registerDebugRpcRequest(debugRequest);
    }

    return yield* Effect.gen(function* () {
      yield* logProtocol({
        direction: "outgoing",
        stage: "decoded",
        payload: message,
      });

      const method = message._tag === "Request" ? message.tag : undefined;
      const encodedRequestId =
        message._tag === "Request"
          ? message.id
          : "requestId" in message
            ? message.requestId
            : undefined;
      const requestId = encodedRequestId === "" ? undefined : encodedRequestId;
      const encoded = yield* Effect.try({
        try: () => parser.encode(message),
        catch: (cause) =>
          AcpError.AcpProtocolParseError.fromEncodingError(method, requestId, cause),
      });

      if (!encoded) {
        return yield* Effect.die(
          new Error("ACP protocol encoder returned no bytes for an outgoing message."),
        );
      }

      yield* logProtocol({
        direction: "outgoing",
        stage: "raw",
        payload: typeof encoded === "string" ? encoded : new TextDecoder().decode(encoded),
      });

      const outgoingAck = yield* CurrentOutgoingAck;
      const matchesOutgoingAck =
        outgoingAck !== undefined &&
        message._tag === "Request" &&
        message.id !== "" &&
        message.tag === outgoingAck.method;
      yield* Effect.uninterruptible(
        Queue.offer(outgoing, encoded).pipe(
          Effect.flatMap((offered) =>
            offered
              ? (matchesOutgoingAck
                  ? Deferred.succeed(outgoingAck.outgoingAck, {
                      method: message.tag,
                      requestId: message.id,
                    }).pipe(
                      Effect.flatMap((completed) =>
                        completed
                          ? Effect.void
                          : Effect.die(
                              new Error(
                                `ACP outgoing acknowledgement for '${message.tag}' was already completed before Queue.offer returned.`,
                              ),
                            ),
                      ),
                    )
                  : Effect.void
                ).pipe(
                  Effect.andThen(
                    debugRequest === undefined
                      ? Effect.void
                      : markDebugRpcEnqueued(debugRequest.id, debugRequest.tag),
                  ),
                )
              : Ref.get(terminalCause).pipe(
                  Effect.flatMap((cause) =>
                    cause === undefined
                      ? Effect.die(
                          new Error(
                            "ACP outgoing queue closed without a recorded terminal transport cause.",
                          ),
                        )
                      : Effect.failCause(cause),
                  ),
                ),
          ),
        ),
      );
      if (message._tag === "Request" && message.id !== "" && matchesOutgoingAck) {
        yield* logProtocol({
          direction: "outgoing",
          stage: "enqueued",
          payload: {
            _tag: "Request",
            tag: message.tag,
            id: message.id,
          },
        });
      }
    }).pipe(
      Effect.onError((cause) =>
        debugRequest === undefined
          ? Effect.void
          : completeDebugRpcFailure(debugRequest.id, cause as AcpTransportCause),
      ),
    );
  });

  const resolveExtPending = (
    requestId: string,
    onFound: (pendingRequest: AcpPendingRequest) => Effect.Effect<void>,
  ) =>
    Ref.modify(extPending, (pending) => {
      const pendingRequest = pending.get(requestId);
      if (!pendingRequest) {
        return [Effect.void, pending] as const;
      }
      const next = new Map(pending);
      next.delete(requestId);
      return [onFound(pendingRequest), next] as const;
    }).pipe(Effect.flatten);

  const removeExtPending = (requestId: string) =>
    Ref.update(extPending, (pending) => {
      if (!pending.has(requestId)) {
        return pending;
      }
      const next = new Map(pending);
      next.delete(requestId);
      return next;
    });

  const completeExtPendingFailure = (requestId: string, error: AcpError.AcpError) =>
    resolveExtPending(requestId, ({ deferred }) =>
      recordDebugCompletion(requestId, false).pipe(
        Effect.andThen(Deferred.fail(deferred, error)),
        Effect.asVoid,
      ),
    );

  const completeExtPendingSuccess = (requestId: string, value: unknown) =>
    resolveExtPending(requestId, ({ deferred }) =>
      recordDebugCompletion(requestId, true).pipe(
        Effect.andThen(Deferred.succeed(deferred, value)),
        Effect.asVoid,
      ),
    );

  const failAllExtPending = (cause: AcpTransportCause) =>
    Ref.getAndSet(extPending, new Map()).pipe(
      Effect.flatMap((pending) =>
        Effect.forEach(
          [...pending.values()],
          ({ deferred, outgoingAck }) =>
            Deferred.failCause(deferred, cause).pipe(
              Effect.andThen(
                outgoingAck === undefined
                  ? Effect.void
                  : Deferred.failCause(outgoingAck, cause).pipe(Effect.asVoid),
              ),
            ),
          { discard: true },
        ),
      ),
    );

  const completeDebugRpcResponse = (requestId: string, successful: boolean, response: unknown) =>
    Ref.modify(extPending, (pending) => {
      const request = pending.get(requestId);
      if (request?.source !== "rpc-debug") return [Effect.void, pending] as const;
      const next = new Map(pending);
      next.delete(requestId);
      return [
        recordDebugCompletion(requestId, successful).pipe(
          Effect.andThen(Deferred.succeed(request.deferred, response)),
          Effect.asVoid,
        ),
        next,
      ] as const;
    }).pipe(Effect.flatten);

  const dispatchNotification = (notification: AcpIncomingNotification) =>
    Queue.offer(notificationQueue, notification).pipe(
      Effect.andThen(
        options.onNotification
          ? options.onNotification(notification).pipe(Effect.catch(() => Effect.void))
          : Effect.void,
      ),
      Effect.asVoid,
    );

  const emitClientProtocolError = (cause: AcpTransportCause) =>
    Queue.offer(clientQueue, {
      _tag: "ClientProtocolError",
      error: new RpcClientError.RpcClientError({
        reason: new RpcClientError.RpcClientDefect({
          message: "ACP protocol terminated.",
          cause,
        }),
      }),
    }).pipe(Effect.asVoid);

  const handleTermination = Effect.fn("handleAcpProtocolTermination")(function* (
    cause: AcpTransportCause,
  ) {
    const first = yield* Ref.modify(terminalCause, (current) =>
      current === undefined ? [true, cause] : [false, current],
    );
    if (!first) return;

    yield* Queue.end(outgoing);
    yield* Queue.offer(disconnects, 0);
    yield* failAllExtPending(cause);
    yield* emitClientProtocolError(cause);
    if (options.onTermination) {
      yield* options.onTermination(cause);
    }
    yield* Ref.set(completedRequestIds, new Set());
    yield* Ref.set(successfulResponseRequestIds, new Set());
  }, Effect.uninterruptible);

  const intentionalEndCause = Effect.fail<AcpError.AcpError>(
    new AcpError.AcpInputStreamEndedError({}),
  ).pipe(
    Effect.exit,
    Effect.map((exit) => (Exit.isFailure(exit) ? exit.cause : Cause.empty)),
  );

  const failValueAsCause = (error: AcpError.AcpError) =>
    Effect.fail(error).pipe(
      Effect.exit,
      Effect.map((exit) => (Exit.isFailure(exit) ? exit.cause : Cause.empty)),
    );

  const classifyScopeCause = (cause: Cause.Cause<unknown>): AcpTransportCause =>
    Cause.map(cause, (error) =>
      isAcpError(error)
        ? error
        : new AcpError.AcpTransportError({
            operation: "call-rpc",
            detail: "ACP protocol scope ended with a transport failure.",
            cause: error,
          }),
    );

  const respondWithSuccess = (requestId: string, value: unknown) =>
    offerOutgoing({
      _tag: "Exit",
      requestId,
      exit: {
        _tag: "Success",
        value,
      },
    });

  const respondWithError = (requestId: string, error: AcpError.AcpRequestError) =>
    offerOutgoing({
      _tag: "Exit",
      requestId,
      exit: {
        _tag: "Failure",
        cause: [
          {
            _tag: "Fail",
            error: error.toProtocolError(),
          },
        ],
      },
    });

  const handleExtRequest = (message: RpcMessage.RequestEncoded) => {
    if (!options.onExtRequest) {
      return respondWithError(message.id, AcpError.AcpRequestError.methodNotFound(message.tag));
    }
    return options.onExtRequest(message.tag, message.payload).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          respondWithError(
            message.id,
            AcpError.AcpRequestError.fromExtensionHandlerError(error, message.tag),
          ),
        onSuccess: (value) => respondWithSuccess(message.id, value),
      }),
    );
  };

  const handleRequestEncoded = (message: RpcMessage.RequestEncoded) => {
    if (message.id === "") {
      if (message.tag === CLIENT_METHODS.session_update) {
        return decodeSessionUpdate(message.payload).pipe(
          Effect.map(
            (params) =>
              ({
                _tag: "SessionUpdate",
                method: CLIENT_METHODS.session_update,
                params,
              }) satisfies AcpIncomingNotification,
          ),
          Effect.mapError((cause) =>
            AcpError.AcpProtocolParseError.fromSchemaError(
              "decode-notification-payload",
              CLIENT_METHODS.session_update,
              cause,
            ),
          ),
          Effect.flatMap(dispatchNotification),
        );
      }
      if (message.tag === CLIENT_METHODS.session_elicitation_complete) {
        return decodeElicitationComplete(message.payload).pipe(
          Effect.map(
            (params) =>
              ({
                _tag: "ElicitationComplete",
                method: CLIENT_METHODS.session_elicitation_complete,
                params,
              }) satisfies AcpIncomingNotification,
          ),
          Effect.mapError((cause) =>
            AcpError.AcpProtocolParseError.fromSchemaError(
              "decode-notification-payload",
              CLIENT_METHODS.session_elicitation_complete,
              cause,
            ),
          ),
          Effect.flatMap(dispatchNotification),
        );
      }
      return dispatchNotification({
        _tag: "ExtNotification",
        method: message.tag,
        params: message.payload,
      });
    }

    if (!options.serverRequestMethods.has(message.tag)) {
      return handleExtRequest(message).pipe(
        Effect.catchTags({
          AcpProtocolParseError: (error) =>
            Effect.logWarning(error).pipe(
              Effect.annotateLogs({
                method: message.tag,
                requestId: message.id,
                operation: error.operation,
              }),
              Effect.andThen(
                respondWithError(
                  message.id,
                  AcpError.AcpRequestError.fromExtensionResponseEncodingError(
                    message.tag,
                    message.id,
                    error,
                  ),
                ),
              ),
            ),
        }),
        Effect.asVoid,
      );
    }

    return Queue.offer(serverQueue, message).pipe(Effect.asVoid);
  };

  const handleExitEncoded = (message: RpcMessage.ResponseExitEncoded) =>
    Ref.get(extPending).pipe(
      Effect.flatMap((pending) => {
        const pendingRequest = pending.get(message.requestId);
        if (!pendingRequest) {
          return Queue.offer(clientQueue, message).pipe(Effect.asVoid);
        }
        if (pendingRequest.source === "rpc-debug") {
          return completeDebugRpcResponse(
            message.requestId,
            message.exit._tag === "Success",
            message,
          ).pipe(Effect.andThen(Queue.offer(clientQueue, message)), Effect.asVoid);
        }
        if (message.exit._tag === "Success") {
          return completeExtPendingSuccess(message.requestId, message.exit.value);
        }
        const failure = message.exit.cause.find((entry) => entry._tag === "Fail");
        if (failure && isProtocolError(failure.error)) {
          return completeExtPendingFailure(
            message.requestId,
            AcpError.AcpRequestError.fromProtocolError(failure.error, {
              method: pendingRequest.method,
              requestId: message.requestId,
              cause: message.exit.cause,
            }),
          );
        }
        return completeExtPendingFailure(
          message.requestId,
          AcpError.AcpRequestError.fromExtensionResponseFailure(
            pendingRequest.method,
            message.requestId,
            message.exit.cause,
          ),
        );
      }),
    );

  const routeDecodedMessage = (
    message: RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded,
  ): Effect.Effect<void, AcpError.AcpError> => {
    switch (message._tag) {
      case "Request":
        return handleRequestEncoded(message);
      case "Exit":
        return handleExitEncoded(message);
      case "Chunk":
        return Ref.get(extPending).pipe(
          Effect.flatMap((pending) => {
            const pendingRequest = pending.get(message.requestId);
            return pendingRequest
              ? completeExtPendingFailure(
                  message.requestId,
                  AcpError.AcpRequestError.unsupportedStreamingResponse(
                    pendingRequest.method,
                    message.requestId,
                  ),
                )
              : Queue.offer(clientQueue, message).pipe(Effect.asVoid);
          }),
        );
      case "Defect":
      case "ClientProtocolError":
      case "Pong":
        return Queue.offer(clientQueue, message).pipe(Effect.asVoid);
      case "Ack":
      case "Interrupt":
      case "Ping":
      case "Eof":
        return Queue.offer(serverQueue, message).pipe(Effect.asVoid);
    }
  };

  yield* options.stdio.stdin.pipe(
    Stream.runForEach((data) =>
      logProtocol({
        direction: "incoming",
        stage: "raw",
        payload: typeof data === "string" ? data : new TextDecoder().decode(data),
      }).pipe(
        Effect.flatMap(() =>
          Effect.try({
            try: () =>
              parser.decode(data) as ReadonlyArray<
                RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded
              >,
            catch: (cause) =>
              new AcpError.AcpProtocolParseError({
                operation: "decode-wire-message",
                cause,
              }),
          }),
        ),
        Effect.tap((messages) =>
          logProtocol({
            direction: "incoming",
            stage: "decoded",
            payload: messages,
          }),
        ),
        Effect.tapErrorTag("AcpProtocolParseError", (error) =>
          logProtocol({
            direction: "incoming",
            stage: "decode_failed",
            payload: {
              operation: error.operation,
              ...(error.method === undefined ? {} : { method: error.method }),
              ...(error.requestId === undefined ? {} : { requestId: error.requestId }),
              ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
              ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
              ...(error.maximumPathDepth === undefined
                ? {}
                : { maximumPathDepth: error.maximumPathDepth }),
            },
          }),
        ),
        Effect.flatMap((messages) =>
          Effect.forEach(messages, routeDecodedMessage, {
            discard: true,
          }),
        ),
      ),
    ),
    Effect.matchCauseEffect({
      onFailure: (cause) =>
        handleTermination(
          mapCauseFailuresPreservingReasons(cause, (error) =>
            isAcpError(error)
              ? error
              : new AcpError.AcpTransportError({
                  operation: "read-input-stream",
                  cause: error,
                }),
          ),
        ),
      onSuccess: () =>
        Effect.exit(
          options.terminationError ?? Effect.succeed(new AcpError.AcpInputStreamEndedError({})),
        ).pipe(
          Effect.flatMap((exit) =>
            Exit.isSuccess(exit)
              ? failValueAsCause(exit.value).pipe(Effect.flatMap(handleTermination))
              : handleTermination(exit.cause),
          ),
        ),
    }),
    Effect.forkScoped,
  );

  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(options.stdio.stdout()),
    Effect.catchCause((cause) =>
      handleTermination(
        mapCauseFailuresPreservingReasons(cause, (error) =>
          isAcpError(error)
            ? error
            : new AcpError.AcpTransportError({
                operation: "call-rpc",
                detail: "ACP output stream failed.",
                cause: error,
              }),
        ),
      ),
    ),
    Effect.forkScoped,
  );

  const clientProtocol = RpcClient.Protocol.of({
    run: (_clientId, f) =>
      Stream.fromQueue(clientQueue).pipe(
        Stream.runForEach((message) => f(message)),
        Effect.forever,
      ),
    send: (_clientId, request) =>
      offerOutgoing(request).pipe(
        Effect.catchCause((cause) => {
          const failures = cause.reasons.filter(Cause.isFailReason);
          if (failures.length === 0) {
            return Effect.failCause(cause as Cause.Cause<never>);
          }
          const singleUnannotatedFailure =
            cause.reasons.length === 1 &&
            failures.length === 1 &&
            [...failures[0]!.annotations.keys()].every((key) => key === Cause.StackTrace.key);
          return Effect.fail(
            new RpcClientError.RpcClientError({
              reason: new RpcClientError.RpcClientDefect({
                message: "Failed to send ACP protocol message.",
                cause: singleUnannotatedFailure ? failures[0]!.error : cause,
              }),
            }),
          );
        }),
      ),
    supportsAck: true,
    supportsTransferables: false,
  });

  const serverProtocol = RpcServer.Protocol.of({
    run: (f) =>
      Stream.fromQueue(serverQueue).pipe(
        Stream.runForEach((message) => f(0, message)),
        Effect.forever,
      ),
    disconnects,
    send: (_clientId, response) => offerOutgoing(response).pipe(Effect.orDie),
    end: (_clientId) => intentionalEndCause.pipe(Effect.flatMap(handleTermination)),
    clientIds: Effect.succeed(new Set([0])),
    initialMessage: Effect.succeedNone,
    supportsAck: true,
    supportsTransferables: false,
    supportsSpanPropagation: true,
  });

  const sendNotification = Effect.fn("sendNotification")(function* (
    method: string,
    payload: unknown,
  ) {
    yield* offerOutgoing({
      _tag: "Request",
      id: "",
      tag: method,
      payload,
      headers: [],
    });
  });

  const sendRequest = Effect.fn("sendRequest")(function* (method: string, payload: unknown) {
    const requestId = yield* Ref.modify(
      nextRequestId,
      (current) => [current, current + 1n] as const,
    );
    const deferred = yield* Deferred.make<unknown, AcpError.AcpError>();
    yield* Ref.update(extPending, (pending) =>
      new Map(pending).set(String(requestId), { deferred, method, source: "extension" }),
    );
    yield* offerOutgoing({
      _tag: "Request",
      id: String(requestId),
      tag: method,
      payload,
      headers: [],
    }).pipe(Effect.onError(() => removeExtPending(String(requestId))));
    return yield* Deferred.await(deferred).pipe(
      Effect.onInterrupt(() => removeExtPending(String(requestId))),
    );
  });

  const withOutgoingAck: AcpPatchedProtocol["withOutgoingAck"] = (method, outgoingAck, effect) =>
    Effect.provideService(effect, CurrentOutgoingAck, { method, outgoingAck }).pipe(
      Effect.onExit((exit) =>
        Deferred.isDone(outgoingAck).pipe(
          Effect.flatMap((completed) => {
            if (completed) {
              return Effect.void;
            }
            return Exit.isFailure(exit)
              ? Deferred.failCause(outgoingAck, exit.cause).pipe(Effect.asVoid)
              : Deferred.die(
                  outgoingAck,
                  new Error(
                    `ACP request '${method}' completed before its outgoing acknowledgement.`,
                  ),
                ).pipe(Effect.asVoid);
          }),
        ),
      ),
    );

  yield* Effect.addFinalizer((exit) =>
    Exit.isFailure(exit)
      ? handleTermination(classifyScopeCause(exit.cause))
      : intentionalEndCause.pipe(Effect.flatMap(handleTermination)),
  );

  return {
    clientProtocol,
    serverProtocol,
    get incoming() {
      return Stream.fromQueue(notificationQueue);
    },
    request: sendRequest,
    notify: sendNotification,
    getTerminalCause: Ref.get(terminalCause),
    withOutgoingAck,
  } satisfies AcpPatchedProtocol;
});

function isProtocolError(
  value: unknown,
): value is { code: number; message: string; data?: unknown } {
  return (
    typeof value === "object" &&
    value !== null &&
    "code" in value &&
    typeof value.code === "number" &&
    "message" in value &&
    typeof value.message === "string"
  );
}
