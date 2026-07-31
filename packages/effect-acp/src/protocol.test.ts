import * as Path from "effect/Path";
import * as AcpError from "./errors.ts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Ref from "effect/Ref";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { it, assert } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";

import * as AcpSchema from "./_generated/schema.gen.ts";
import * as AcpProtocol from "./protocol.ts";
import {
  encodeJsonl,
  jsonRpcNotification,
  jsonRpcRequest,
  jsonRpcResponse,
} from "./_internal/shared.ts";
import { makeInMemoryStdio, makeTerminationError, makeChildStdio } from "./_internal/stdio.ts";

const SessionCancelNotification = jsonRpcNotification(
  "session/cancel",
  AcpSchema.CancelNotification,
);
const SessionUpdateNotification = jsonRpcNotification(
  "session/update",
  AcpSchema.SessionNotification,
);
const ElicitationCompleteNotification = jsonRpcNotification(
  "session/elicitation/complete",
  AcpSchema.ElicitationCompleteNotification,
);
const RequestPermissionRequest = jsonRpcRequest(
  "session/request_permission",
  AcpSchema.RequestPermissionRequest,
);
const RequestPermissionResponse = jsonRpcResponse(AcpSchema.RequestPermissionResponse);
const ExtRequest = jsonRpcRequest("x/test", Schema.Struct({ hello: Schema.String }));
const ExtResponse = jsonRpcResponse(Schema.Struct({ ok: Schema.Boolean }));
const decodeSessionCancelNotification = Schema.decodeEffect(
  Schema.fromJsonString(SessionCancelNotification),
);
const decodeExtRequest = Schema.decodeEffect(Schema.fromJsonString(ExtRequest));
const decodeRequestPermissionResponse = Schema.decodeEffect(
  Schema.fromJsonString(RequestPermissionResponse),
);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
const encoder = new TextEncoder();
const mockPeerPath = Effect.map(Effect.service(Path.Path), (path) =>
  path.join(import.meta.dirname, "../test/fixtures/acp-mock-peer.ts"),
);
const mockPeerArgs = (path: string) => [path];

const makeHandle = (env?: Record<string, string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const path = yield* Path.Path;
    const command = ChildProcess.make(process.execPath, mockPeerArgs(yield* mockPeerPath), {
      cwd: path.join(import.meta.dirname, ".."),
      ...(env ? { env: { ...process.env, ...env } } : {}),
    });
    return yield* spawner.spawn(command);
  });

it.layer(NodeServices.layer)("effect-acp protocol", (it) => {
  it.effect(
    "emits exact JSON-RPC notifications and decodes inbound session/update and elicitation completion",
    () =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
          stdio,
          serverRequestMethods: new Set(),
        });

        const notifications =
          yield* Deferred.make<ReadonlyArray<AcpProtocol.AcpIncomingNotification>>();
        yield* transport.incoming.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.flatMap((notificationChunk) => Deferred.succeed(notifications, notificationChunk)),
          Effect.forkScoped,
        );

        yield* transport.notify("session/cancel", { sessionId: "session-1" });
        const outbound = yield* Queue.take(output);
        assert.deepEqual(yield* decodeSessionCancelNotification(outbound), {
          jsonrpc: "2.0",
          method: "session/cancel",
          params: {
            sessionId: "session-1",
          },
        });

        yield* Queue.offer(
          input,
          yield* encodeJsonl(SessionUpdateNotification, {
            jsonrpc: "2.0",
            method: "session/update",
            params: {
              sessionId: "session-1",
              update: {
                sessionUpdate: "plan",
                entries: [
                  {
                    content: "Inspect repository",
                    priority: "high",
                    status: "in_progress",
                  },
                ],
              },
            },
          }),
        );

        yield* Queue.offer(
          input,
          yield* encodeJsonl(ElicitationCompleteNotification, {
            jsonrpc: "2.0",
            method: "session/elicitation/complete",
            params: {
              elicitationId: "elicitation-1",
            },
          }),
        );

        const [update, completion] = yield* Deferred.await(notifications);
        assert.equal(update?._tag, "SessionUpdate");
        assert.equal(completion?._tag, "ElicitationComplete");
      }),
  );

  it.effect("keeps invalid core notification values only in the schema cause", () =>
    Effect.gen(function* () {
      const secret = "acp-core-notification-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const termination = yield* Deferred.make<AcpError.AcpError>();
      yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encoder.encode(
          `${encodeUnknownJsonString({
            jsonrpc: "2.0",
            method: "session/update",
            params: {
              sessionId: { secret },
              update: {
                sessionUpdate: "plan",
                entries: [],
              },
            },
          })}\n`,
        ),
      );

      const error = yield* Deferred.await(termination);
      assert.instanceOf(error, AcpError.AcpProtocolParseError);
      const parseError = error as AcpError.AcpProtocolParseError;
      const { cause, ...directDiagnostics } = parseError;
      assert.equal(parseError.operation, "decode-notification-payload");
      assert.equal(parseError.method, "session/update");
      assert.isAbove(parseError.issueCount ?? 0, 0);
      assert.include(parseError.issueKinds ?? [], "Pointer");
      assert.isAbove(parseError.maximumPathDepth ?? 0, 0);
      assert.isTrue(Schema.isSchemaError(cause));
      assert.notInclude(parseError.message, secret);
      assert.notInclude(encodeUnknownJsonString(directDiagnostics), secret);
    }),
  );

  it.effect("logs outgoing notifications when logOutgoing is enabled", () =>
    Effect.gen(function* () {
      const { stdio } = yield* makeInMemoryStdio();
      const events: Array<AcpProtocol.AcpProtocolLogEvent> = [];
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        logOutgoing: true,
        logger: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      });

      yield* transport.notify("session/cancel", { sessionId: "session-1" });

      assert.deepEqual(events, [
        {
          direction: "outgoing",
          stage: "decoded",
          payload: {
            _tag: "Request",
            id: "",
            tag: "session/cancel",
            payload: {
              sessionId: "session-1",
            },
            headers: [],
          },
        },
        {
          direction: "outgoing",
          stage: "raw",
          payload:
            '{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"session-1"},"id":"","headers":[]}\n',
        },
      ]);
    }),
  );

  it.effect("logs decode failures without copying the cause or wire payload", () =>
    Effect.gen(function* () {
      const secret = "acp-wire-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const events: Array<AcpProtocol.AcpProtocolLogEvent> = [];
      const termination = yield* Deferred.make<AcpError.AcpError>();
      yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        logIncoming: true,
        logger: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`{"secret":"${secret}"\n`));
      yield* Deferred.await(termination);

      const event = events.find(({ stage }) => stage === "decode_failed");
      assert.deepEqual(event, {
        direction: "incoming",
        stage: "decode_failed",
        payload: {
          operation: "decode-wire-message",
        },
      });
      assert.notInclude(encodeUnknownJsonString(event), secret);
    }),
  );

  it.effect("fails notification encoding through the declared ACP error channel", () =>
    Effect.gen(function* () {
      const { stdio } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });

      const bigintError = yield* transport.notify("x/test", 1n).pipe(Effect.flip);
      assert.instanceOf(bigintError, AcpError.AcpProtocolParseError);
      assert.equal(bigintError.operation, "encode-message");
      assert.equal(bigintError.method, "x/test");
      assert.instanceOf(bigintError.cause, TypeError);
      assert.equal(
        bigintError.message,
        "ACP protocol operation 'encode-message' failed for method 'x/test'.",
      );

      const circular: Record<string, unknown> = {};
      circular.self = circular;
      const circularError = yield* transport.notify("x/test", circular).pipe(Effect.flip);
      assert.instanceOf(circularError, AcpError.AcpProtocolParseError);
      assert.equal(circularError.operation, "encode-message");
      assert.equal(circularError.method, "x/test");
      assert.instanceOf(circularError.cause, TypeError);

      const requestError = yield* transport.request("x/request", 1n).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected request encoding to fail"),
        }),
      );
      assert.instanceOf(requestError, AcpError.AcpProtocolParseError);
      assert.deepInclude(requestError, {
        operation: "encode-message",
        method: "x/request",
        requestId: "1",
      });
    }),
  );

  it.effect("supports generic extension requests over the patched transport", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });

      const response = yield* transport
        .request("x/test", { hello: "world" })
        .pipe(Effect.forkScoped);
      const outbound = yield* Queue.take(output);
      assert.deepEqual(yield* decodeExtRequest(outbound), {
        jsonrpc: "2.0",
        id: 1,
        method: "x/test",
        params: {
          hello: "world",
        },
        headers: [],
      });

      yield* Queue.offer(
        input,
        yield* encodeJsonl(ExtResponse, {
          jsonrpc: "2.0",
          id: 1,
          result: {
            ok: true,
          },
        }),
      );

      const resolved = yield* Fiber.join(response);
      assert.deepEqual(resolved, { ok: true });
    }),
  );

  it.effect("correlates extension response errors with the originating request", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });

      const response = yield* transport
        .request("x/private", { hello: "world" })
        .pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(
        input,
        encoder.encode(
          `${encodeUnknownJsonString({
            jsonrpc: "2.0",
            id: 1,
            error: {
              _tag: "Cause",
              code: -32602,
              message: "Invalid params",
              data: [
                {
                  _tag: "Fail",
                  error: {
                    code: -32602,
                    message: "Invalid params",
                    data: { field: "hello" },
                  },
                },
              ],
            },
          })}\n`,
        ),
      );

      const error = yield* Fiber.join(response).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected extension request to fail"),
        }),
      );
      assert.instanceOf(error, AcpError.AcpRequestError);
      assert.deepInclude(error, {
        code: -32602,
        errorMessage: "Invalid params",
        method: "x/private",
        requestId: "1",
        operation: "receive-response",
      });
    }),
  );

  it.effect("preserves zero-valued ids for inbound core client requests", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(["session/request_permission"]),
      });
      const inboundRequest = yield* Deferred.make<unknown>();

      yield* transport.serverProtocol
        .run((_clientId, message) => Deferred.succeed(inboundRequest, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);

      yield* Queue.offer(
        input,
        yield* encodeJsonl(RequestPermissionRequest, {
          jsonrpc: "2.0",
          id: 0,
          method: "session/request_permission",
          params: {
            sessionId: "session-1",
            toolCall: {
              toolCallId: "tool-1",
              title: "Allow mock action",
            },
            options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }],
          },
          headers: [],
        }),
      );

      const message = yield* Deferred.await(inboundRequest);
      assert.deepEqual(message, {
        _tag: "Request",
        id: "0",
        tag: "session/request_permission",
        payload: {
          sessionId: "session-1",
          toolCall: {
            toolCallId: "tool-1",
            title: "Allow mock action",
          },
          options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }],
        },
        headers: [],
      });

      yield* transport.serverProtocol.send(0, {
        _tag: "Exit",
        requestId: "0",
        exit: {
          _tag: "Success",
          value: {
            outcome: {
              outcome: "selected",
              optionId: "allow",
            },
          },
        },
      });

      const outbound = yield* Queue.take(output);
      assert.deepEqual(yield* decodeRequestPermissionResponse(outbound), {
        jsonrpc: "2.0",
        id: 0,
        result: {
          outcome: {
            outcome: "selected",
            optionId: "allow",
          },
        },
      });
    }),
  );

  it.effect("cleans up interrupted extension requests before a late response arrives", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const lateResponse = yield* Deferred.make<unknown>();

      yield* transport.clientProtocol
        .run(0, (message) => Deferred.succeed(lateResponse, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);

      const response = yield* transport
        .request("x/test", { hello: "world" })
        .pipe(Effect.forkScoped);
      const outbound = yield* Queue.take(output);
      assert.deepEqual(yield* decodeExtRequest(outbound), {
        jsonrpc: "2.0",
        id: 1,
        method: "x/test",
        params: {
          hello: "world",
        },
        headers: [],
      });

      yield* Fiber.interrupt(response);
      yield* Queue.offer(
        input,
        yield* encodeJsonl(ExtResponse, {
          jsonrpc: "2.0",
          id: 1,
          result: {
            ok: true,
          },
        }),
      );

      const message = yield* Deferred.await(lateResponse);
      assert.deepEqual(message, {
        _tag: "Exit",
        requestId: "1",
        exit: {
          _tag: "Success",
          value: {
            ok: true,
          },
        },
      });
    }),
  );

  it.effect("propagates the real child exit code when the input stream ends", () =>
    Effect.gen(function* () {
      const handle = yield* makeHandle({ ACP_MOCK_EXIT_IMMEDIATELY_CODE: "7" });
      const firstMessage = yield* Deferred.make<unknown>();
      const termination = yield* Deferred.make<AcpError.AcpError>();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio: makeChildStdio(handle),
        terminationError: makeTerminationError(handle),
        serverRequestMethods: new Set(),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* transport.clientProtocol
        .run(0, (message) => Deferred.succeed(firstMessage, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);

      const message = yield* Deferred.await(firstMessage);
      const exitError = yield* Deferred.await(termination);
      assert.instanceOf(exitError, AcpError.AcpProcessExitedError);
      assert.equal((exitError as AcpError.AcpProcessExitedError).code, 7);
      assert.equal((message as { readonly _tag?: string })._tag, "ClientProtocolError");
      const defect = (message as { readonly error: { readonly reason: unknown } }).error.reason as {
        readonly _tag: string;
        readonly message: string;
        readonly cause: unknown;
      };
      assert.equal(defect._tag, "RpcClientDefect");
      assert.equal(defect.message, "ACP protocol terminated.");
      assert.instanceOf(defect.cause, AcpError.AcpProcessExitedError);
      assert.equal((defect.cause as AcpError.AcpProcessExitedError).code, 7);
    }),
  );

  it.effect("classifies an input stream ending without inventing a cause", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const termination = yield* Deferred.make<AcpError.AcpError>();
      yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.end(input);

      const error = yield* Deferred.await(termination);
      assert.instanceOf(error, AcpError.AcpInputStreamEndedError);
      assert.equal(error.message, "ACP input stream ended.");
      assert.equal("cause" in error, false);
    }),
  );

  it.effect("does not emit a second process-exit error after a decode failure", () =>
    Effect.gen(function* () {
      const handle = yield* makeHandle({
        ACP_MOCK_MALFORMED_OUTPUT: "1",
        ACP_MOCK_MALFORMED_OUTPUT_EXIT_CODE: "23",
      });
      const terminationCalls = yield* Ref.make(0);
      const firstMessage = yield* Deferred.make<unknown>();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio: makeChildStdio(handle),
        terminationError: makeTerminationError(handle),
        serverRequestMethods: new Set(),
        onTermination: () => Ref.update(terminationCalls, (count) => count + 1),
      });

      yield* transport.clientProtocol
        .run(0, (message) => Deferred.succeed(firstMessage, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);

      const message = yield* Deferred.await(firstMessage);
      assert.equal(yield* Ref.get(terminationCalls), 1);
      assert.equal((message as { readonly _tag?: string })._tag, "ClientProtocolError");
      const defect = (message as { readonly error: { readonly reason: unknown } }).error.reason as {
        readonly _tag: string;
        readonly message: string;
        readonly cause: unknown;
      };
      assert.equal(defect._tag, "RpcClientDefect");
      assert.equal(defect.message, "ACP protocol terminated.");
      assert.instanceOf(defect.cause, AcpError.AcpProtocolParseError);
    }),
  );

  it.effect("keeps client send failure messages independent from the cause", () =>
    Effect.gen(function* () {
      const { stdio } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });

      const failure = yield* transport.clientProtocol
        .send(0, {
          _tag: "Request",
          id: "request-1",
          tag: "x/test",
          payload: 1n,
          headers: [],
        })
        .pipe(Effect.flip);
      const defect = failure.reason as {
        readonly _tag: string;
        readonly message: string;
        readonly cause: unknown;
      };

      assert.equal(defect._tag, "RpcClientDefect");
      assert.equal(defect.message, "Failed to send ACP protocol message.");
      assert.instanceOf(defect.cause, AcpError.AcpProtocolParseError);
    }),
  );

  it.effect("acknowledges the exact request only after it enters the outgoing queue", () =>
    Effect.gen(function* () {
      const { stdio, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();
      const request = yield* transport
        .withOutgoingAck("x/test", outgoingAck, transport.request("x/test", { hello: "world" }))
        .pipe(Effect.forkScoped);

      const encoded = yield* Queue.take(output);
      const evidence = yield* Deferred.await(outgoingAck);
      assert.equal(
        typeof encoded === "string" ? encoded.includes('"method":"x/test"') : false,
        true,
      );
      assert.deepStrictEqual(evidence, {
        method: "x/test",
        requestId: "1",
      });
      yield* Fiber.interrupt(request);
    }),
  );

  it.effect("fails the request-specific ack when the outgoing queue is already closed", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();
      const lateResponse = yield* Deferred.make<unknown>();
      yield* transport.clientProtocol
        .run(0, (message) => Deferred.succeed(lateResponse, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);

      yield* transport.serverProtocol.end(0);
      const requestExit = yield* Effect.exit(
        transport.withOutgoingAck(
          "x/test",
          outgoingAck,
          transport.request("x/test", { hello: "closed" }),
        ),
      );
      const ackExit = yield* Effect.exit(Deferred.await(outgoingAck));

      assert.equal(Exit.isFailure(requestExit), true);
      assert.equal(Exit.isFailure(ackExit), true);
      if (Exit.isFailure(requestExit) && Exit.isFailure(ackExit)) {
        assert.deepStrictEqual(ackExit.cause, requestExit.cause);
        const failure = requestExit.cause.reasons.find(Cause.isFailReason);
        assert.isDefined(failure);
        if (failure !== undefined) {
          assert.instanceOf(failure.error, AcpError.AcpTransportError);
          assert.deepInclude(failure.error, {
            operation: "call-rpc",
            method: "x/test",
            detail: "ACP outgoing queue closed before accepting the message.",
          });
        }
      }

      yield* Queue.offer(
        input,
        yield* encodeJsonl(ExtResponse, {
          jsonrpc: "2.0",
          id: 1,
          result: { ok: true },
        }),
      );
      assert.deepEqual(yield* Deferred.await(lateResponse), {
        _tag: "Exit",
        requestId: "1",
        exit: { _tag: "Success", value: { ok: true } },
      });
    }),
  );

  it.effect("fails the request-specific ack when shutdown wins after encoding", () =>
    Effect.gen(function* () {
      const { stdio, output } = yield* makeInMemoryStdio();
      let transport!: AcpProtocol.AcpPatchedProtocol;
      transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        logOutgoing: true,
        logger: (event) =>
          event.direction === "outgoing" && event.stage === "raw"
            ? transport.serverProtocol.end(0)
            : Effect.void,
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();

      const requestExit = yield* Effect.exit(
        transport.withOutgoingAck(
          "x/test",
          outgoingAck,
          transport.request("x/test", { hello: "race" }),
        ),
      );
      const ackExit = yield* Effect.exit(Deferred.await(outgoingAck));

      assert.equal(Exit.isFailure(requestExit), true);
      assert.equal(Exit.isFailure(ackExit), true);
      if (Exit.isFailure(requestExit) && Exit.isFailure(ackExit)) {
        assert.deepStrictEqual(ackExit.cause, requestExit.cause);
      }
      assert.equal(Option.isNone(yield* Queue.poll(output)), true);
    }),
  );

  it.effect("fails pre-ack when the writer tears down immediately before offer", () =>
    Effect.gen(function* () {
      const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
      const failWriter = yield* Deferred.make<void>();
      const terminated = yield* Deferred.make<AcpError.AcpError>();
      const writerError = PlatformError.systemError({
        _tag: "Unknown",
        module: "Stdio",
        method: "write",
        cause: new Error("writer stopped before offer"),
      });
      const stdio = Stdio.make({
        args: Effect.succeed([]),
        stdin: Stream.fromQueue(input),
        stdout: () =>
          Sink.fromEffect(
            Deferred.await(failWriter).pipe(Effect.andThen(Effect.fail(writerError))),
          ),
        stderr: () => Sink.drain,
      });
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        logOutgoing: true,
        logger: (event) =>
          event.direction === "outgoing" && event.stage === "raw"
            ? Deferred.succeed(failWriter, undefined).pipe(
                Effect.andThen(Deferred.await(terminated)),
                Effect.asVoid,
              )
            : Effect.void,
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();

      const requestExit = yield* Effect.exit(
        transport.withOutgoingAck(
          "x/test",
          outgoingAck,
          transport.request("x/test", { hello: "writer" }),
        ),
      );
      const ackExit = yield* Effect.exit(Deferred.await(outgoingAck));
      assert.equal(Exit.isFailure(requestExit), true);
      assert.equal(Exit.isFailure(ackExit), true);
      if (Exit.isFailure(requestExit) && Exit.isFailure(ackExit)) {
        assert.deepStrictEqual(ackExit.cause, requestExit.cause);
      }
      assert.strictEqual((yield* Deferred.await(terminated)).cause, writerError);
    }),
  );

  it.effect("fails pre-ack when the protocol scope closes immediately before offer", () =>
    Effect.gen(function* () {
      const { stdio, output } = yield* makeInMemoryStdio();
      const protocolScope = yield* Scope.make("sequential");
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      }).pipe(Scope.provide(protocolScope));
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();

      yield* Scope.close(protocolScope, Exit.void);
      const requestExit = yield* Effect.exit(
        transport.withOutgoingAck(
          "x/test",
          outgoingAck,
          transport.request("x/test", { hello: "scope" }),
        ),
      );
      const ackExit = yield* Effect.exit(Deferred.await(outgoingAck));
      assert.equal(Exit.isFailure(requestExit), true);
      assert.equal(Exit.isFailure(ackExit), true);
      if (Exit.isFailure(requestExit) && Exit.isFailure(ackExit)) {
        assert.deepStrictEqual(ackExit.cause, requestExit.cause);
      }
      assert.equal(Option.isNone(yield* Queue.poll(output)), true);
    }),
  );

  it.effect("keeps a successful ack when the queue ends after offer", () =>
    Effect.gen(function* () {
      const { stdio, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();
      const request = yield* transport
        .withOutgoingAck("x/test", outgoingAck, transport.request("x/test", { hello: "world" }))
        .pipe(Effect.forkScoped);

      yield* Queue.take(output);
      const evidence = yield* Deferred.await(outgoingAck);
      yield* transport.serverProtocol.end(0);
      assert.deepStrictEqual(evidence, { method: "x/test", requestId: "1" });
      assert.equal(Exit.isSuccess(yield* Effect.exit(Deferred.await(outgoingAck))), true);
      yield* Fiber.interrupt(request);
    }),
  );

  it.effect("keeps a successful ack when the writer fails after offer", () =>
    Effect.gen(function* () {
      const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
      const wrote = yield* Deferred.make<void>();
      const terminated = yield* Deferred.make<AcpError.AcpError>();
      const writerError = PlatformError.systemError({
        _tag: "Unknown",
        module: "Stdio",
        method: "write",
        cause: new Error("writer failed after offer"),
      });
      const stdio = Stdio.make({
        args: Effect.succeed([]),
        stdin: Stream.fromQueue(input),
        stdout: () =>
          Sink.forEach(() =>
            Deferred.succeed(wrote, undefined).pipe(
              Effect.andThen(Effect.fail(writerError)),
              Effect.asVoid,
            ),
          ),
        stderr: () => Sink.drain,
      });
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();
      const request = yield* transport
        .withOutgoingAck("x/test", outgoingAck, transport.request("x/test", { hello: "world" }))
        .pipe(Effect.forkScoped);

      yield* Deferred.await(wrote);
      assert.deepStrictEqual(yield* Deferred.await(outgoingAck), {
        method: "x/test",
        requestId: "1",
      });
      const error = yield* Deferred.await(terminated);
      assert.instanceOf(error, AcpError.AcpTransportError);
      assert.strictEqual(error.cause, writerError);
      const requestExit = yield* Fiber.await(request);
      assert.equal(Exit.isFailure(requestExit), true);
      assert.equal(Exit.isSuccess(yield* Effect.exit(Deferred.await(outgoingAck))), true);
    }),
  );

  it.effect("keeps parallel request acknowledgements bound to their request ids", () =>
    Effect.gen(function* () {
      const { stdio, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const ackOne = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();
      const ackTwo = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();
      const requestOne = yield* transport
        .withOutgoingAck("x/one", ackOne, transport.request("x/one", { order: 1 }))
        .pipe(Effect.forkScoped);
      const requestTwo = yield* transport
        .withOutgoingAck("x/two", ackTwo, transport.request("x/two", { order: 2 }))
        .pipe(Effect.forkScoped);

      const encoded = [yield* Queue.take(output), yield* Queue.take(output)].join("");
      const evidence = [yield* Deferred.await(ackOne), yield* Deferred.await(ackTwo)];
      assert.include(encoded, '"method":"x/one"');
      assert.include(encoded, '"method":"x/two"');
      assert.deepEqual(
        evidence
          .map(({ method, requestId }) => ({ method, requestId }))
          .sort((a, b) => a.method.localeCompare(b.method)),
        [
          { method: "x/one", requestId: "1" },
          { method: "x/two", requestId: "2" },
        ],
      );
      yield* Fiber.interrupt(requestOne);
      yield* Fiber.interrupt(requestTwo);
    }),
  );

  it.effect("fails the request-specific ack with the same pre-enqueue encoding cause", () =>
    Effect.gen(function* () {
      const { stdio, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();

      const requestExit = yield* Effect.exit(
        transport.withOutgoingAck(
          "x/test",
          outgoingAck,
          transport.request("x/test", { value: 1n }),
        ),
      );
      const ackExit = yield* Effect.exit(Deferred.await(outgoingAck));

      assert.equal(Exit.isFailure(requestExit), true);
      assert.equal(Exit.isFailure(ackExit), true);
      if (Exit.isFailure(requestExit) && Exit.isFailure(ackExit)) {
        assert.deepStrictEqual(ackExit.cause, requestExit.cause);
      }
      assert.equal(Option.isNone(yield* Queue.poll(output)), true);
    }),
  );

  it.effect("terminates a request fiber that exits before its outgoing ack", () =>
    Effect.gen(function* () {
      const { stdio } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        serverRequestMethods: new Set(),
      });
      const outgoingAck = yield* Deferred.make<
        AcpProtocol.AcpOutgoingRequestEvidence,
        AcpError.AcpError
      >();

      const requestExit = yield* Effect.exit(
        transport.withOutgoingAck("x/test", outgoingAck, Effect.succeed("impossible-success")),
      );
      const ackExit = yield* Effect.exit(Deferred.await(outgoingAck));

      assert.equal(Exit.isSuccess(requestExit), true);
      assert.equal(Exit.isFailure(ackExit), true);
      if (Exit.isFailure(ackExit)) {
        assert.equal(Cause.hasDies(ackExit.cause), true);
      }
    }),
  );

  it.effect("fails pending extension requests with the propagated exit code", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* AcpProtocol.makeAcpPatchedProtocol({
        stdio,
        terminationError: Effect.succeed(new AcpError.AcpProcessExitedError({ code: 0 })),
        serverRequestMethods: new Set(),
      });

      const response = yield* transport
        .request("x/test", { hello: "world" })
        .pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.end(input);

      const error = yield* Fiber.join(response).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected request to fail after process exit"),
        }),
      );
      assert.instanceOf(error, AcpError.AcpProcessExitedError);
      assert.equal(error.code, 0);
    }),
  );
});
