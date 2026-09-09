// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpProtocol from "effect-acp/protocol";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");
const mockAgentCommand = "node";
const mockAgentArgs = [mockAgentPath];
const mockRuntimeOptions = {
  spawn: { command: mockAgentCommand, args: mockAgentArgs },
  cwd: process.cwd(),
  clientInfo: { name: "t3-test", version: "0.0.0" },
  authMethodId: "test",
} satisfies AcpSessionRuntime.AcpSessionRuntimeOptions;

function countMockAgentPrompts(requestLogPath: string): number {
  if (!NodeFS.existsSync(requestLogPath)) {
    return 0;
  }
  return NodeFS.readFileSync(requestLogPath, "utf8")
    .split("\n")
    .filter((line) => line.includes('"method":"session/prompt"')).length;
}

const withMockRequestLog = <A, E, R>(
  use: (requestLogPath: string) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-acp-outgoing-ack-"))),
    (directory) => use(NodePath.join(directory, "requests.jsonl")),
    (directory) => Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
  );

function isPromptProtocolEvent(event: EffectAcpProtocol.AcpProtocolLogEvent): boolean {
  if (event.direction !== "outgoing") {
    return false;
  }
  if (event.stage === "raw") {
    return typeof event.payload === "string" && event.payload.includes('"method":"session/prompt"');
  }
  if (event.stage !== "decoded" || typeof event.payload !== "object" || event.payload === null) {
    return false;
  }
  return "tag" in event.payload && event.payload.tag === "session/prompt";
}

function expectSameCauseReasons(
  actual: Cause.Cause<unknown>,
  expected: Cause.Cause<unknown>,
): void {
  expect(actual.reasons).toHaveLength(expected.reasons.length);
  for (const [index, actualReason] of actual.reasons.entries()) {
    const expectedReason = expected.reasons[index]!;
    expect(actualReason._tag).toBe(expectedReason._tag);
    const actualAnnotations = new Map(actualReason.annotations);
    const expectedAnnotations = new Map(expectedReason.annotations);
    actualAnnotations.delete(Cause.StackTrace.key);
    expectedAnnotations.delete(Cause.StackTrace.key);
    expect(actualAnnotations).toEqual(expectedAnnotations);
    if (Cause.isFailReason(actualReason) && Cause.isFailReason(expectedReason)) {
      expect(actualReason.error).toBe(expectedReason.error);
    } else if (Cause.isDieReason(actualReason) && Cause.isDieReason(expectedReason)) {
      expect(actualReason.defect).toBe(expectedReason.defect);
    } else if (Cause.isInterruptReason(actualReason) && Cause.isInterruptReason(expectedReason)) {
      expect(actualReason.fiberId).toBe(expectedReason.fiberId);
    }
  }
}

describe("AcpSessionRuntime", () => {
  for (const setupMethod of ["session/new", "session/resume"] as const) {
    it.effect(`buffers root metadata while ${setupMethod} startup is still pending`, () =>
      Effect.gen(function* () {
        const setupReplied = yield* Deferred.make<void>();
        const allowStartup = yield* Deferred.make<void>();
        const events: Array<AcpSessionRuntime.AcpSessionRuntimeEvent> = [];
        const runtime = yield* AcpSessionRuntime.make({
          ...mockRuntimeOptions,
          ...(setupMethod === "session/resume"
            ? { resumeSessionId: "mock-session-1", resumeMethod: "resume" as const }
            : {}),
          requestLogger: (event) =>
            event.method === setupMethod && event.status === "succeeded"
              ? Deferred.succeed(setupReplied, undefined).pipe(
                  Effect.andThen(Deferred.await(allowStartup)),
                )
              : Effect.void,
        });
        yield* runtime.getEvents().pipe(
          Stream.runForEach((event) => {
            if (event._tag === "EventStreamBarrier") {
              return Deferred.succeed(event.acknowledge, undefined);
            }
            events.push(event);
            return Effect.void;
          }),
          Effect.forkChild,
        );
        const startup = yield* runtime.start().pipe(Effect.forkChild);
        yield* Deferred.await(setupReplied);
        yield* runtime.request("_test/startup-metadata", {});
        yield* Deferred.succeed(allowStartup, undefined);
        yield* Fiber.join(startup);
        yield* runtime.drainEvents;

        expect(events.map((event) => event._tag)).toEqual([
          "AvailableCommandsUpdated",
          "ModeChanged",
          "ConfigOptionsUpdated",
        ]);
        expect(events[0]).toMatchObject({
          availableCommands: [{ name: "plan", description: "Native command" }],
        });
        expect(yield* runtime.getModeState).toMatchObject({ currentModeId: "code" });
        expect(events[2]).toMatchObject({
          configOptions: yield* runtime.getConfigOptions,
        });
        expect(
          (yield* runtime.getConfigOptions).find((option) => option.category === "model"),
        ).toMatchObject({ currentValue: "gpt-5.4" });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }

  it.effect("publishes model changes returned by a config request and live notifications", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.make(mockRuntimeOptions);
      yield* runtime.start();
      const updates = yield* Stream.toPull(
        runtime.getEvents().pipe(Stream.filter((event) => event._tag === "ConfigOptionsUpdated")),
      );
      const selected = yield* runtime.setConfigOption("model", "composer-2");
      expect((yield* updates)[0]?.configOptions).toEqual(selected.configOptions);
      yield* runtime.request("_test/startup-metadata", {});
      expect((yield* updates)[0]?.configOptions).toEqual(yield* runtime.getConfigOptions);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("awaits native resume instead of using the load replay idle fallback", () =>
    Effect.gen(function* () {
      const resumeStarted = yield* Deferred.make<void>();
      const requestMethods: Array<string> = [];
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: {
          ...mockRuntimeOptions.spawn,
          env: { T3_ACP_WAIT_FOR_RESUME_RELEASE: "1" },
        },
        resumeSessionId: "mock-session-1",
        resumeMethod: "resume",
        sessionLoadReplayIdleGap: "1 second",
        requestLogger: (event) =>
          Effect.sync(() => {
            if (event.status === "started") requestMethods.push(event.method);
          }),
      });
      yield* runtime.handleSessionUpdate((notification) =>
        notification.update.sessionUpdate === "user_message_chunk"
          ? Deferred.succeed(resumeStarted, undefined).pipe(Effect.asVoid)
          : Effect.void,
      );
      const startup = yield* runtime.start().pipe(Effect.forkChild);
      yield* Deferred.await(resumeStarted);
      yield* TestClock.adjust("3 seconds");
      expect(startup.pollUnsafe()).toBeUndefined();
      yield* runtime.request("_test/release-resume", {});
      const started = yield* Fiber.join(startup);

      expect(started.sessionSetupResult._meta).toEqual({ nativeResume: true });
      expect(requestMethods).toContain("session/resume");
      expect(requestMethods).not.toContain("session/load");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("waits for native cancellation and drains final updates before another prompt", () =>
    Effect.gen(function* () {
      const toolStarted = yield* Deferred.make<void>();
      const cancelReceived = yield* Deferred.make<void>();
      const events: Array<AcpSessionRuntime.AcpSessionRuntimeEvent> = [];
      let promptRequests = 0;
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: {
          ...mockRuntimeOptions.spawn,
          env: { T3_ACP_COMPLETE_FIRST_PROMPT_ON_CANCEL: "1" },
        },
        cancelBehavior: "wait-for-prompt",
        requestLogger: (event) =>
          Effect.sync(() => {
            if (event.method === "session/prompt" && event.status === "started")
              promptRequests += 1;
          }),
      });
      yield* runtime.getEvents().pipe(
        Stream.runForEach((event) => {
          if (event._tag === "EventStreamBarrier") {
            return Deferred.succeed(event.acknowledge, undefined);
          }
          events.push(event);
          if (event._tag === "ToolCallUpdated" && event.toolCall.status === "inProgress") {
            return Deferred.succeed(toolStarted, undefined);
          }
          if (event._tag === "ThoughtDelta" && event.text === "native-cancel-received") {
            return Deferred.succeed(cancelReceived, undefined);
          }
          return Effect.void;
        }),
        Effect.forkChild,
      );
      yield* runtime.start();
      const prompt = yield* runtime
        .prompt({
          prompt: [{ type: "text", text: "first" }],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(toolStarted);
      const cancellation = yield* runtime.cancel.pipe(Effect.forkChild);
      yield* Deferred.await(cancelReceived);
      const replacement = yield* runtime
        .prompt({
          prompt: [{ type: "text", text: "second" }],
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(prompt.pollUnsafe()).toBeUndefined();
      expect(cancellation.pollUnsafe()).toBeUndefined();
      expect(promptRequests).toBe(1);
      yield* runtime.request("_test/finish-cancel", {});
      yield* Fiber.join(cancellation);

      expect(yield* Fiber.join(prompt)).toEqual({
        stopReason: "cancelled",
        _meta: { nativeCancel: true },
      });
      expect(
        events.some(
          (event) =>
            event._tag === "ToolCallUpdated" &&
            event.toolCall.status === "failed" &&
            event.toolCall.detail === "Cancelled.",
        ),
      ).toBe(true);
      const cancelledDelta = events.find(
        (event) => event._tag === "ContentDelta" && event.text === "Request cancelled.",
      );
      expect(cancelledDelta?._tag).toBe("ContentDelta");
      if (cancelledDelta?._tag === "ContentDelta") {
        expect(
          events.filter(
            (event) =>
              event._tag === "AssistantItemCompleted" && event.itemId === cancelledDelta.itemId,
          ),
        ).toHaveLength(1);
      }
      expect(yield* Fiber.join(replacement)).toMatchObject({ stopReason: "end_turn" });
      expect(promptRequests).toBe(2);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("retires a process when native cancellation times out", () =>
    Effect.gen(function* () {
      const toolStarted = yield* Deferred.make<void>();
      const cancelReceived = yield* Deferred.make<void>();
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: {
          ...mockRuntimeOptions.spawn,
          env: { T3_ACP_COMPLETE_FIRST_PROMPT_ON_CANCEL: "1" },
        },
        cancelBehavior: "wait-for-prompt",
        cancelTimeout: "1 second",
      });
      yield* runtime.getEvents().pipe(
        Stream.runForEach((event) => {
          if (event._tag === "EventStreamBarrier") {
            return Deferred.succeed(event.acknowledge, undefined);
          }
          if (event._tag === "ToolCallUpdated") {
            return Deferred.succeed(toolStarted, undefined);
          }
          if (event._tag === "ThoughtDelta") {
            return Deferred.succeed(cancelReceived, undefined);
          }
          return Effect.void;
        }),
        Effect.forkChild,
      );
      yield* runtime.start();
      const prompt = yield* runtime
        .prompt({
          prompt: [{ type: "text", text: "first" }],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(toolStarted);
      const cancellation = yield* runtime.cancel.pipe(Effect.forkChild);
      yield* Deferred.await(cancelReceived);
      yield* TestClock.adjust("2 seconds");

      const error = yield* Fiber.join(cancellation).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "AcpTransportError",
        method: "session/cancel",
      });
      expect(Exit.isFailure(yield* Fiber.await(prompt))).toBe(true);
      expect(
        yield* runtime
          .prompt({
            prompt: [{ type: "text", text: "must not run" }],
          })
          .pipe(Effect.flip),
      ).toBe(error);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reports an idle child exit and rejects later prompts", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.make(mockRuntimeOptions);
      yield* runtime.start();
      yield* runtime.notify("_test/exit", {});
      const events = yield* runtime.getEvents().pipe(Stream.take(1), Stream.runCollect);
      const event = events[0];
      expect(event).toMatchObject({ _tag: "ConnectionTerminated", error: { code: 19 } });
      if (event?._tag !== "ConnectionTerminated") return;
      expect(
        yield* runtime
          .prompt({
            prompt: [{ type: "text", text: "must not run" }],
          })
          .pipe(Effect.flip),
      ).toBe(event.error);
      expect(yield* runtime.start().pipe(Effect.flip)).toBe(event.error);
      expect(yield* runtime.initialize().pipe(Effect.flip)).toBe(event.error);
      expect(
        yield* runtime.request("_test/environment", {}).pipe(
          Effect.match({
            onFailure: (error) => error,
            onSuccess: () => undefined,
          }),
        ),
      ).toBe(event.error);
      expect(yield* runtime.notify("_test/exit", {}).pipe(Effect.flip)).toBe(event.error);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("retires a native runtime when its prompt caller is interrupted", () =>
    Effect.gen(function* () {
      const dispatched = yield* Deferred.make<void>();
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: {
          ...mockRuntimeOptions.spawn,
          env: { T3_ACP_COMPLETE_FIRST_PROMPT_ON_CANCEL: "1" },
        },
        cancelBehavior: "wait-for-prompt",
      });
      yield* runtime.start();
      const prompt = yield* runtime
        .prompt(
          {
            prompt: [{ type: "text", text: "first" }],
          },
          { dispatched },
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(dispatched);
      yield* Fiber.interrupt(prompt);
      const events = yield* runtime.getEvents().pipe(
        Stream.filter((event) => event._tag === "ConnectionTerminated"),
        Stream.take(1),
        Stream.runCollect,
      );
      expect(events[0]).toMatchObject({
        error: { _tag: "AcpTransportError", method: "session/prompt" },
      });
      expect(
        yield* runtime
          .prompt({
            prompt: [{ type: "text", text: "must not run" }],
          })
          .pipe(Effect.flip),
      ).toBe(events[0]?.error);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails a pending request when the stderr handler rejects the runtime", () =>
    Effect.gen(function* () {
      const failure = new EffectAcpErrors.AcpTransportError({
        detail: "Sign in before continuing.",
        cause: undefined,
      });
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: { ...mockRuntimeOptions.spawn, env: { T3_ACP_FLOOD_STDERR: "1" } },
        onStderr: () => Effect.fail(failure),
      });
      expect(yield* runtime.start().pipe(Effect.flip)).toBe(failure);
      const events = yield* runtime.getEvents().pipe(
        Stream.filter((event) => event._tag === "ConnectionTerminated"),
        Stream.take(1),
        Stream.runCollect,
      );
      expect(events[0]?.error).toBe(failure);
      expect(yield* runtime.initialize().pipe(Effect.flip)).toBe(failure);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("drains large stderr output and keeps auth-sized logging chunks", () =>
    Effect.gen(function* () {
      const lengths: Array<number> = [];
      for (const logStderr of [false, true]) {
        yield* Effect.gen(function* () {
          const runtime = yield* AcpSessionRuntime.make({
            ...mockRuntimeOptions,
            spawn: { ...mockRuntimeOptions.spawn, env: { T3_ACP_FLOOD_STDERR: "1" } },
            ...(logStderr
              ? {
                  onStderr: (text: string) =>
                    Effect.sync(() => {
                      lengths.push(text.length);
                    }),
                }
              : {}),
          });
          expect(yield* runtime.initialize()).toMatchObject({ protocolVersion: 1 });
        }).pipe(Effect.scoped);
      }
      expect(lengths.length).toBeGreaterThan(0);
      expect(Math.max(...lengths)).toBeGreaterThanOrEqual(16_384);
      expect(Math.max(...lengths)).toBeLessThanOrEqual(32_768);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("releases a queued event drain when its runtime scope closes", () =>
    Effect.gen(function* () {
      const scope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
        Scope.close(scope, Exit.void),
      );
      const barrierReceived = yield* Deferred.make<void>();
      const runtime = yield* AcpSessionRuntime.make(mockRuntimeOptions).pipe(
        Effect.provideService(Scope.Scope, scope),
      );
      yield* runtime.start();
      yield* runtime.getEvents().pipe(
        Stream.runForEach((event) =>
          event._tag === "EventStreamBarrier"
            ? Deferred.succeed(barrierReceived, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
        ),
        Effect.forkIn(scope),
      );
      const drain = yield* runtime.drainEvents.pipe(Effect.forkChild);
      yield* Deferred.await(barrierReceived);
      yield* Scope.close(scope, Exit.void);
      yield* Fiber.join(drain);
      yield* runtime.drainEvents;
      expect(yield* runtime.initialize().pipe(Effect.flip)).toMatchObject({
        _tag: "AcpTransportError",
        detail: "The ACP session runtime is closed.",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("bounds native cancellation when its event consumer is absent", () =>
    Effect.gen(function* () {
      const toolStarted = yield* Deferred.make<void>();
      const cancelReceived = yield* Deferred.make<void>();
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: {
          ...mockRuntimeOptions.spawn,
          env: { T3_ACP_COMPLETE_FIRST_PROMPT_ON_CANCEL: "1" },
        },
        cancelBehavior: "wait-for-prompt",
        cancelTimeout: "1 second",
      });
      yield* runtime.handleSessionUpdate((notification) => {
        if (notification.update.sessionUpdate === "tool_call") {
          return Deferred.succeed(toolStarted, undefined).pipe(Effect.asVoid);
        }
        if (notification.update.sessionUpdate === "agent_thought_chunk") {
          return Deferred.succeed(cancelReceived, undefined).pipe(Effect.asVoid);
        }
        return Effect.void;
      });
      yield* runtime.start();
      const prompt = yield* runtime
        .prompt({
          prompt: [{ type: "text", text: "first" }],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(toolStarted);
      const cancellation = yield* runtime.cancel.pipe(Effect.forkChild);
      yield* Deferred.await(cancelReceived);
      yield* runtime.request("_test/finish-cancel", {});
      expect(yield* Fiber.join(prompt)).toMatchObject({ stopReason: "cancelled" });
      yield* TestClock.adjust("2 seconds");
      expect(yield* Fiber.join(cancellation).pipe(Effect.flip)).toMatchObject({
        _tag: "AcpTransportError",
        method: "session/cancel",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("does not restore ambient variables to a sanitized child environment", () =>
    Effect.gen(function* () {
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          const previous = process.env.T3_ACP_RUNTIME_AMBIENT;
          process.env.T3_ACP_RUNTIME_AMBIENT = "sentinel";
          return previous;
        }),
        (previous) =>
          Effect.sync(() => {
            if (previous === undefined) delete process.env.T3_ACP_RUNTIME_AMBIENT;
            else process.env.T3_ACP_RUNTIME_AMBIENT = previous;
          }),
      );
      const runtime = yield* AcpSessionRuntime.make({
        ...mockRuntimeOptions,
        spawn: {
          command: process.execPath,
          args: mockAgentArgs,
          extendEnv: false,
          env: { T3_ACP_RUNTIME_EXPLICIT: "kept" },
        },
      });
      yield* runtime.initialize();
      expect(yield* runtime.request("_test/environment", {})).toEqual({
        inherited: false,
        explicit: true,
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("merges custom initialize client capabilities into the ACP handshake", () => {
    const requestEvents: Array<AcpSessionRuntime.AcpSessionRequestLogEvent> = [];
    return Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const initializeStarted = requestEvents.find(
        (event) => event.method === "initialize" && event.status === "started",
      );
      expect(initializeStarted?.payload).toMatchObject({
        protocolVersion: 1,
        clientCapabilities: {
          fs: { readTextFile: false, writeTextFile: false },
          terminal: false,
          _meta: { parameterizedModelPicker: true },
        },
      });
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientCapabilities: {
            _meta: {
              parameterizedModelPicker: true,
            },
          },
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
          requestLogger: (event) =>
            Effect.sync(() => {
              requestEvents.push(event);
            }),
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("starts a session, prompts, and emits normalized events against the mock agent", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      const started = yield* runtime.start();

      expect(started.initializeResult).toMatchObject({ protocolVersion: 1 });
      expect(started.sessionId).toBe("mock-session-1");

      const promptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });
      expect(promptResult).toMatchObject({ stopReason: "end_turn" });

      const notes = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 4)));
      expect(notes).toHaveLength(4);
      expect(notes.map((note) => note._tag)).toEqual([
        "PlanUpdated",
        "AssistantItemStarted",
        "ContentDelta",
        "AssistantItemCompleted",
      ]);
      const planUpdate = notes.find((note) => note._tag === "PlanUpdated");
      expect(planUpdate?._tag).toBe("PlanUpdated");
      if (planUpdate?._tag === "PlanUpdated") {
        expect(planUpdate.payload.plan).toHaveLength(2);
      }
      const assistantStart = notes[1];
      const assistantDelta = notes[2];
      if (
        assistantStart?._tag === "AssistantItemStarted" &&
        assistantDelta?._tag === "ContentDelta"
      ) {
        expect(assistantDelta.itemId).toBe(assistantStart.itemId);
      }
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("attests native prompt invocation only after the real ACP request starts", () => {
    const order: Array<string> = [];
    return Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const result = yield* runtime.prompt(
        {
          prompt: [{ type: "text", text: "native invocation handshake" }],
        },
        {
          nativeInvocationStarted: () =>
            Effect.sync(() => {
              order.push("native-invocation-started");
            }),
        },
      );

      expect(result).toMatchObject({ stopReason: "end_turn" });
      expect(order).toEqual(["request-started", "native-invocation-started"]);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
          requestLogger: (event) =>
            Effect.sync(() => {
              if (event.method === "session/prompt" && event.status === "started") {
                order.push("request-started");
              }
            }),
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("fails pre-enqueue request, encoding, and protocol logger exits without an ack", () =>
    withMockRequestLog((requestLogPath) =>
      Effect.gen(function* () {
        const scenarios = [
          {
            name: "request-logger-defect",
            requestLogger: (event: AcpSessionRuntime.AcpSessionRequestLogEvent) =>
              event.method === "session/prompt" && event.status === "started"
                ? Effect.die(new Error("request-logger-defect"))
                : Effect.void,
            protocolLogger: (_event: EffectAcpProtocol.AcpProtocolLogEvent) => Effect.void,
          },
          {
            name: "encoding-defect",
            requestLogger: (_event: AcpSessionRuntime.AcpSessionRequestLogEvent) => Effect.void,
            protocolLogger: (event: EffectAcpProtocol.AcpProtocolLogEvent) => {
              if (
                event.stage === "decoded" &&
                isPromptProtocolEvent(event) &&
                typeof event.payload === "object" &&
                event.payload !== null
              ) {
                return Effect.sync(() => {
                  Object.defineProperty(event.payload, "payload", {
                    configurable: true,
                    get: () => {
                      throw new Error("protocol-encoding-defect");
                    },
                  });
                });
              }
              return Effect.void;
            },
          },
          {
            name: "protocol-logger-defect-before-offer",
            requestLogger: (_event: AcpSessionRuntime.AcpSessionRequestLogEvent) => Effect.void,
            protocolLogger: (event: EffectAcpProtocol.AcpProtocolLogEvent) =>
              event.stage === "raw" && isPromptProtocolEvent(event)
                ? Effect.die(new Error("protocol-logger-defect-before-offer"))
                : Effect.void,
          },
          {
            name: "protocol-logger-interrupt-before-offer",
            requestLogger: (_event: AcpSessionRuntime.AcpSessionRequestLogEvent) => Effect.void,
            protocolLogger: (event: EffectAcpProtocol.AcpProtocolLogEvent) =>
              event.stage === "raw" && isPromptProtocolEvent(event)
                ? Effect.interrupt
                : Effect.void,
          },
        ] as const;

        for (const scenario of scenarios) {
          NodeFS.rmSync(requestLogPath, { force: true });
          let outgoingEnqueues = 0;
          let promptDecoded = 0;
          let promptRaw = 0;
          const exit = yield* Effect.exit(
            Effect.gen(function* () {
              const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
              yield* runtime.start();
              return yield* runtime.prompt(
                { prompt: [{ type: "text", text: scenario.name }] },
                {
                  nativeInvocationStarted: () =>
                    Effect.sync(() => {
                      outgoingEnqueues += 1;
                    }),
                },
              );
            }).pipe(
              Effect.provide(
                AcpSessionRuntime.layer({
                  spawn: {
                    command: mockAgentCommand,
                    args: mockAgentArgs,
                    env: { T3_ACP_REQUEST_LOG_PATH: requestLogPath },
                  },
                  cwd: process.cwd(),
                  clientInfo: { name: "t3-test", version: "0.0.0" },
                  authMethodId: "test",
                  requestLogger: scenario.requestLogger,
                  protocolLogging: {
                    logOutgoing: true,
                    logger: (event) => {
                      if (isPromptProtocolEvent(event)) {
                        if (event.stage === "decoded") promptDecoded += 1;
                        if (event.stage === "raw") promptRaw += 1;
                      }
                      return scenario.protocolLogger(event);
                    },
                  },
                }),
              ),
              Effect.scoped,
              Effect.provide(NodeServices.layer),
            ),
          );

          expect(exit._tag, scenario.name).toBe("Failure");
          expect(outgoingEnqueues, scenario.name).toBe(0);
          expect(countMockAgentPrompts(requestLogPath), scenario.name).toBe(0);
          if (scenario.name === "request-logger-defect") {
            expect(promptDecoded).toBe(0);
            expect(promptRaw).toBe(0);
          } else if (scenario.name === "encoding-defect") {
            expect(promptDecoded).toBe(1);
            expect(promptRaw).toBe(0);
          } else {
            expect(promptDecoded).toBe(1);
            expect(promptRaw).toBe(1);
          }
        }
      }),
    ),
  );

  it.effect("preserves a real caller interrupt while the protocol logger is pre-enqueue", () =>
    withMockRequestLog((requestLogPath) =>
      Effect.gen(function* () {
        const loggerReached = yield* Deferred.make<void>();
        const holdLogger = yield* Deferred.make<void>();
        let outgoingEnqueues = 0;
        const promptExit = yield* Effect.gen(function* () {
          const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
          yield* runtime.start();
          const prompt = yield* runtime
            .prompt(
              { prompt: [{ type: "text", text: "interrupt before offer" }] },
              {
                nativeInvocationStarted: () =>
                  Effect.sync(() => {
                    outgoingEnqueues += 1;
                  }),
              },
            )
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(loggerReached);
          yield* Fiber.interrupt(prompt);
          return yield* Fiber.await(prompt);
        }).pipe(
          Effect.provide(
            AcpSessionRuntime.layer({
              spawn: {
                command: mockAgentCommand,
                args: mockAgentArgs,
                env: { T3_ACP_REQUEST_LOG_PATH: requestLogPath },
              },
              cwd: process.cwd(),
              clientInfo: { name: "t3-test", version: "0.0.0" },
              authMethodId: "test",
              protocolLogging: {
                logOutgoing: true,
                logger: (event) =>
                  event.stage === "raw" && isPromptProtocolEvent(event)
                    ? Deferred.succeed(loggerReached, undefined).pipe(
                        Effect.andThen(Deferred.await(holdLogger)),
                      )
                    : Effect.void,
              },
            }),
          ),
          Effect.scoped,
          Effect.provide(NodeServices.layer),
        );

        expect(Exit.hasInterrupts(promptExit)).toBe(true);
        expect(outgoingEnqueues).toBe(0);
        expect(countMockAgentPrompts(requestLogPath)).toBe(0);
      }),
    ),
  );

  it.effect("preserves a combined pre-ack Cause through AcpSessionRuntime.prompt", () =>
    withMockRequestLog((requestLogPath) =>
      Effect.gen(function* () {
        const promptFailure = new EffectAcpErrors.AcpTransportError({
          operation: "call-rpc",
          detail: "combined runtime prompt sentinel",
          cause: new Error("combined runtime prompt failure origin"),
        });
        const promptDefect = new Error("combined runtime prompt defect");
        const promptCause = Cause.fromReasons<EffectAcpErrors.AcpError>([
          Cause.makeFailReason(promptFailure),
          Cause.makeDieReason(promptDefect),
          Cause.makeInterruptReason(47_002),
        ]);
        const requestFailureCauses: Array<Cause.Cause<EffectAcpErrors.AcpError>> = [];
        let nativeInvocationStarted = 0;
        const promptExit = yield* Effect.gen(function* () {
          const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
          yield* runtime.start();
          return yield* Effect.exit(
            runtime.prompt(
              { prompt: [{ type: "text", text: "combined pre-ack Cause" }] },
              {
                nativeInvocationStarted: () =>
                  Effect.sync(() => {
                    nativeInvocationStarted += 1;
                  }),
              },
            ),
          );
        }).pipe(
          Effect.provide(
            AcpSessionRuntime.layer({
              spawn: {
                command: mockAgentCommand,
                args: mockAgentArgs,
                env: { T3_ACP_REQUEST_LOG_PATH: requestLogPath },
              },
              cwd: process.cwd(),
              clientInfo: { name: "t3-test", version: "0.0.0" },
              authMethodId: "test",
              onRequestFailure: ({ method, cause }) =>
                method === "session/prompt"
                  ? Effect.sync(() => {
                      requestFailureCauses.push(cause);
                    })
                  : Effect.void,
              protocolLogging: {
                logOutgoing: true,
                logger: (event) =>
                  event.stage === "raw" && isPromptProtocolEvent(event)
                    ? Effect.failCause(promptCause as Cause.Cause<never>)
                    : Effect.void,
              },
            }),
          ),
          Effect.scoped,
          Effect.provide(NodeServices.layer),
        );

        expect(Exit.isFailure(promptExit)).toBe(true);
        if (Exit.isFailure(promptExit)) {
          expectSameCauseReasons(promptExit.cause, promptCause);
        }
        expect(requestFailureCauses).toHaveLength(1);
        expectSameCauseReasons(requestFailureCauses[0]!, promptCause);
        expect(nativeInvocationStarted).toBe(0);
        expect(countMockAgentPrompts(requestLogPath)).toBe(0);
      }),
    ),
  );

  it.effect(
    "preserves the ordered Cause matrix across every ACP setup operation",
    () =>
      Effect.gen(function* () {
        class SetupSemanticAnnotation extends Context.Service<
          SetupSemanticAnnotation,
          { readonly label: string }
        >()("t3/provider/acp/AcpJsonRpcConnection.test/SetupSemanticAnnotation") {}
        const first = new EffectAcpErrors.AcpTransportError({
          operation: "call-rpc",
          detail: "setup first failure",
          cause: new Error("setup first failure origin"),
        });
        const second = new EffectAcpErrors.AcpTransportError({
          operation: "call-rpc",
          detail: "setup second failure",
          cause: new Error("setup second failure origin"),
        });
        const defect = new Error("setup defect");
        const semantic = Context.make(SetupSemanticAnnotation, { label: "setup-semantic" });
        const stackTrace = Context.makeUnsafe(
          new Map<string, unknown>([
            [
              Cause.StackTrace.key,
              { name: "setup-test", stack: () => undefined, parent: undefined },
            ],
          ]),
        );
        const cases = [
          Cause.fromReasons<EffectAcpErrors.AcpError>([Cause.makeFailReason(first)]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([Cause.makeDieReason(defect)]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([Cause.makeInterruptReason(47_020)]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([
            Cause.makeFailReason(first),
            Cause.makeDieReason(defect),
            Cause.makeInterruptReason(47_021),
          ]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([
            Cause.makeFailReason(first),
            Cause.makeFailReason(second),
          ]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([
            Cause.makeFailReason(first).annotate(semantic),
          ]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([
            Cause.makeFailReason(first).annotate(semantic),
            Cause.makeDieReason(defect).annotate(semantic),
            Cause.makeInterruptReason(47_022).annotate(semantic),
          ]),
          Cause.fromReasons<EffectAcpErrors.AcpError>([
            Cause.makeFailReason(first).annotate(stackTrace),
            Cause.makeDieReason(defect).annotate(stackTrace),
            Cause.makeInterruptReason(47_023).annotate(stackTrace),
          ]),
        ] as const;

        for (const [caseIndex, sourceCause] of cases.entries()) {
          let armedMethod: string | undefined;
          const observedFailures: Array<{
            readonly method: string;
            readonly cause: Cause.Cause<EffectAcpErrors.AcpError>;
          }> = [];
          const runtimeScope = yield* Scope.make("sequential");
          const context = yield* Layer.buildWithScope(
            AcpSessionRuntime.layer({
              spawn: { command: mockAgentCommand, args: mockAgentArgs },
              cwd: process.cwd(),
              clientCapabilities: { _meta: { parameterizedModelPicker: true } },
              clientInfo: { name: "t3-test", version: "0.0.0" },
              authMethodId: "test",
              onRequestFailure: (failure) =>
                Effect.sync(() => {
                  observedFailures.push(failure);
                }),
              protocolLogging: {
                logOutgoing: true,
                logger: (event) => {
                  if (
                    armedMethod !== undefined &&
                    event.stage === "raw" &&
                    event.direction === "outgoing" &&
                    typeof event.payload === "string" &&
                    event.payload.includes(`"method":"${armedMethod}"`)
                  ) {
                    armedMethod = undefined;
                    return Effect.failCause(sourceCause as Cause.Cause<never>);
                  }
                  return Effect.void;
                },
              },
            }).pipe(Layer.provide(NodeServices.layer)),
            runtimeScope,
          );
          const runtime = Context.get(context, AcpSessionRuntime.AcpSessionRuntime);
          const expectFailureThenHealthy = Effect.fn("expectSetupFailureThenHealthy")(function* <A>(
            method: string,
            operation: Effect.Effect<A, EffectAcpErrors.AcpError>,
          ) {
            const observedBefore = observedFailures.length;
            armedMethod = method;
            const exit = yield* Effect.exit(operation);
            expect(Exit.isFailure(exit), `${caseIndex}:${method}`).toBe(true);
            if (Exit.isFailure(exit)) {
              expectSameCauseReasons(exit.cause, sourceCause);
            }
            expect(observedFailures).toHaveLength(observedBefore + 1);
            expect(observedFailures.at(-1)?.method).toBe(method);
            expectSameCauseReasons(observedFailures.at(-1)!.cause, sourceCause);
            return yield* operation;
          });

          yield* expectFailureThenHealthy("session/new", runtime.start());
          yield* expectFailureThenHealthy("session/set_config_option", runtime.setModel("gpt-5.4"));
          yield* expectFailureThenHealthy(
            "session/set_config_option",
            runtime.setConfigOption("reasoning", "high"),
          );
          yield* expectFailureThenHealthy(
            "session/set_config_option",
            runtime.setMode("architect"),
          );
          yield* expectFailureThenHealthy(
            "session/set_model",
            runtime.setSessionModel("grok-mock-alt"),
          );
          yield* Scope.close(runtimeScope, Exit.void);
        }
      }),
    60_000,
  );

  it.effect("closes a runtime scope before ack without leaving the waiting prompt fiber open", () =>
    withMockRequestLog((requestLogPath) =>
      Effect.gen(function* () {
        const loggerReached = yield* Deferred.make<void>();
        const holdLogger = yield* Deferred.make<void>();
        const runtimeScope = yield* Scope.make("sequential");
        const runtimeContext = yield* Layer.buildWithScope(
          AcpSessionRuntime.layer({
            spawn: {
              command: mockAgentCommand,
              args: mockAgentArgs,
              env: { T3_ACP_REQUEST_LOG_PATH: requestLogPath },
            },
            cwd: process.cwd(),
            clientInfo: { name: "t3-test", version: "0.0.0" },
            authMethodId: "test",
            protocolLogging: {
              logOutgoing: true,
              logger: (event) =>
                event.stage === "raw" && isPromptProtocolEvent(event)
                  ? Deferred.succeed(loggerReached, undefined).pipe(
                      Effect.andThen(Deferred.await(holdLogger)),
                    )
                  : Effect.void,
            },
          }).pipe(Layer.provide(NodeServices.layer)),
          runtimeScope,
        );
        const runtime = Context.get(runtimeContext, AcpSessionRuntime.AcpSessionRuntime);
        yield* runtime.start();
        let outgoingEnqueues = 0;
        const prompt = yield* runtime
          .prompt(
            { prompt: [{ type: "text", text: "scope closes before ack" }] },
            {
              nativeInvocationStarted: () =>
                Effect.sync(() => {
                  outgoingEnqueues += 1;
                }),
            },
          )
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(loggerReached);
        yield* Scope.close(runtimeScope, Exit.void);
        const promptExit = yield* Fiber.await(prompt);

        expect(Exit.hasInterrupts(promptExit)).toBe(true);
        expect(outgoingEnqueues).toBe(0);
        expect(countMockAgentPrompts(requestLogPath)).toBe(0);
      }),
    ),
  );

  it.effect(
    "distinguishes post-enqueue interrupt, response loss, and normal prompt completion",
    () =>
      withMockRequestLog((requestLogPath) =>
        Effect.gen(function* () {
          const run = (input: {
            readonly name: string;
            readonly env?: NodeJS.ProcessEnv;
            readonly afterAck?: Effect.Effect<void>;
          }) => {
            let outgoingEnqueues = 0;
            return Effect.gen(function* () {
              const exit = yield* Effect.exit(
                Effect.gen(function* () {
                  const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
                  yield* runtime.start();
                  return yield* runtime.prompt(
                    { prompt: [{ type: "text", text: input.name }] },
                    {
                      nativeInvocationStarted: () =>
                        Effect.sync(() => {
                          outgoingEnqueues += 1;
                        }).pipe(Effect.andThen(input.afterAck ?? Effect.void)),
                    },
                  );
                }).pipe(
                  Effect.provide(
                    AcpSessionRuntime.layer({
                      spawn: {
                        command: mockAgentCommand,
                        args: mockAgentArgs,
                        env: {
                          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
                          ...input.env,
                        },
                      },
                      cwd: process.cwd(),
                      clientInfo: { name: "t3-test", version: "0.0.0" },
                      authMethodId: "test",
                    }),
                  ),
                  Effect.scoped,
                  Effect.provide(NodeServices.layer),
                ),
              );
              return { exit, outgoingEnqueues } as const;
            });
          };

          NodeFS.rmSync(requestLogPath, { force: true });
          const interrupted = yield* run({
            name: "interrupt after offer",
            afterAck: Effect.interrupt,
          });
          expect(interrupted.outgoingEnqueues).toBe(1);
          expect(interrupted.exit).toEqual(Exit.succeed({ stopReason: "cancelled" }));

          NodeFS.rmSync(requestLogPath, { force: true });
          const responseLost = yield* run({
            name: "response lost after offer",
            env: { T3_ACP_EXIT_AFTER_ACCEPTING_PROMPT: "1" },
          });
          expect(responseLost.outgoingEnqueues).toBe(1);
          expect(responseLost.exit._tag).toBe("Failure");
          expect(countMockAgentPrompts(requestLogPath)).toBe(1);

          NodeFS.rmSync(requestLogPath, { force: true });
          const completed = yield* run({ name: "normal prompt" });
          expect(completed.outgoingEnqueues).toBe(1);
          expect(completed.exit).toEqual(Exit.succeed({ stopReason: "end_turn" }));
          expect(countMockAgentPrompts(requestLogPath)).toBe(1);
        }),
      ),
  );

  it.effect("keeps assistant item IDs unique when a provider session restarts", () => {
    const collectFirstAssistantItemId = Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      const started = yield* runtime.start();
      expect(started.sessionId).toBe("mock-session-1");

      yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });

      const events = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 4)));
      const assistantStart = events.find((event) => event._tag === "AssistantItemStarted");
      expect(assistantStart?._tag).toBe("AssistantItemStarted");
      return assistantStart?._tag === "AssistantItemStarted" ? assistantStart.itemId : "";
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
    );

    return Effect.gen(function* () {
      const beforeRestart = yield* collectFirstAssistantItemId;
      const afterRestart = yield* collectFirstAssistantItemId;

      expect(afterRestart).not.toBe(beforeRestart);
    }).pipe(Effect.provide(NodeServices.layer));
  });

  it.effect("drops session updates emitted for a child ACP session", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const promptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });
      expect(promptResult).toMatchObject({ stopReason: "end_turn" });

      const notes = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 4)));
      expect(notes.map((note) => note._tag)).toEqual([
        "AssistantItemStarted",
        "ContentDelta",
        "ContentDelta",
        "AssistantItemCompleted",
      ]);
      expect(
        notes
          .filter((note) => note._tag === "ContentDelta")
          .map((note) => note.text)
          .join(""),
      ).toBe("root before child root after child");
      expect(notes.some((note) => note._tag === "ToolCallUpdated")).toBe(false);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_EMIT_FOREIGN_SESSION_UPDATES: "1",
            },
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("supports successive standard ACP prompts", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const firstPromptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "first" }],
      });
      const secondPromptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "second" }],
      });

      expect(firstPromptResult).toMatchObject({ stopReason: "end_turn" });
      expect(secondPromptResult).toMatchObject({ stopReason: "end_turn" });
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("releases a fully silent prompt when session/cancel is requested", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const promptFiber = yield* runtime
        .prompt({
          prompt: [{ type: "text", text: "hang forever" }],
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* TestClock.adjust("500 millis");
      yield* runtime.cancel;

      const firstPromptResult = yield* Fiber.join(promptFiber);
      expect(firstPromptResult).toMatchObject({ stopReason: "cancelled" });

      const secondPromptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "second" }],
      });
      expect(secondPromptResult).toMatchObject({ stopReason: "end_turn" });
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
            },
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("segments assistant text around ACP tool calls", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const promptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });
      expect(promptResult).toMatchObject({ stopReason: "end_turn" });

      const notes = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 7)));
      expect(notes.map((note) => note._tag)).toEqual([
        "AssistantItemStarted",
        "ContentDelta",
        "AssistantItemCompleted",
        "ToolCallUpdated",
        "ToolCallUpdated",
        "AssistantItemStarted",
        "ContentDelta",
      ]);

      const firstStarted = notes[0];
      const firstDelta = notes[1];
      const firstCompleted = notes[2];
      const secondStarted = notes[5];
      const secondDelta = notes[6];
      expect(firstStarted?._tag).toBe("AssistantItemStarted");
      expect(firstCompleted?._tag).toBe("AssistantItemCompleted");
      expect(secondStarted?._tag).toBe("AssistantItemStarted");
      if (
        firstStarted?._tag === "AssistantItemStarted" &&
        firstDelta?._tag === "ContentDelta" &&
        firstCompleted?._tag === "AssistantItemCompleted" &&
        secondStarted?._tag === "AssistantItemStarted" &&
        secondDelta?._tag === "ContentDelta"
      ) {
        expect(firstDelta.itemId).toBe(firstStarted.itemId);
        expect(firstCompleted.itemId).toBe(firstStarted.itemId);
        expect(secondStarted.itemId).not.toBe(firstStarted.itemId);
        expect(secondDelta.itemId).toBe(secondStarted.itemId);
      }
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_EMIT_INTERLEAVED_ASSISTANT_TOOL_CALLS: "1",
            },
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("emits status-only tool updates through completion", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const promptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });
      expect(promptResult).toMatchObject({ stopReason: "end_turn" });

      const notes = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 3)));
      expect(notes.map((note) => note._tag)).toEqual([
        "ToolCallUpdated",
        "ToolCallUpdated",
        "ToolCallUpdated",
      ]);
      const toolCalls = notes.flatMap((note) =>
        note._tag === "ToolCallUpdated" ? [note.toolCall] : [],
      );
      expect(toolCalls.map((toolCall) => toolCall.status)).toEqual([
        "pending",
        "inProgress",
        "completed",
      ]);
      for (const toolCall of toolCalls) {
        expect(toolCall.title).toBe("Read file");
      }
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_EMIT_GENERIC_TOOL_PLACEHOLDERS: "1",
            },
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          authMethodId: "test",
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("logs ACP requests from the shared runtime", () => {
    const requestEvents: Array<AcpSessionRuntime.AcpSessionRequestLogEvent> = [];
    return Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      yield* runtime.setModel("composer-2");
      yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });

      expect(
        requestEvents.some(
          (event) => event.method === "session/set_config_option" && event.status === "started",
        ),
      ).toBe(true);
      expect(
        requestEvents.some(
          (event) => event.method === "session/set_config_option" && event.status === "succeeded",
        ),
      ).toBe(true);
      expect(
        requestEvents.some(
          (event) => event.method === "session/prompt" && event.status === "started",
        ),
      ).toBe(true);
      expect(
        requestEvents.some(
          (event) => event.method === "session/prompt" && event.status === "succeeded",
        ),
      ).toBe(true);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          requestLogger: (event) =>
            Effect.sync(() => {
              requestEvents.push(event);
            }),
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("skips no-op session config writes when the requested value is already active", () => {
    const requestEvents: Array<AcpSessionRuntime.AcpSessionRequestLogEvent> = [];
    return Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      yield* runtime.setConfigOption("model", "default");
      yield* runtime.setMode("ask");

      expect(
        requestEvents.some(
          (event) => event.method === "session/set_config_option" && event.status === "started",
        ),
      ).toBe(false);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          requestLogger: (event) =>
            Effect.sync(() => {
              requestEvents.push(event);
            }),
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("emits low-level ACP protocol logs for raw and decoded messages", () => {
    const protocolEvents: Array<EffectAcpProtocol.AcpProtocolLogEvent> = [];
    return Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });

      expect(
        protocolEvents.some((event) => event.direction === "outgoing" && event.stage === "raw"),
      ).toBe(true);
      expect(
        protocolEvents.some((event) => event.direction === "outgoing" && event.stage === "decoded"),
      ).toBe(true);
      expect(
        protocolEvents.some((event) => event.direction === "incoming" && event.stage === "raw"),
      ).toBe(true);
      expect(
        protocolEvents.some((event) => event.direction === "incoming" && event.stage === "decoded"),
      ).toBe(true);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
          protocolLogging: {
            logIncoming: true,
            logOutgoing: true,
            logger: (event) =>
              Effect.sync(() => {
                protocolEvents.push(event);
              }),
          },
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("fails session startup when session/load returns an error", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      const error = yield* runtime.start().pipe(Effect.flip);

      expect(error._tag).toBe("AcpRequestError");
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_FAIL_LOAD_SESSION: "1",
            },
          },
          cwd: process.cwd(),
          resumeSessionId: "stale-session-id",
          clientInfo: { name: "t3-test", version: "0.0.0" },
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("ignores session/update replay notifications during session/load", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });
      const notes = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 4)));
      expect(notes.map((note) => note._tag)).toEqual([
        "PlanUpdated",
        "AssistantItemStarted",
        "ContentDelta",
        "AssistantItemCompleted",
      ]);
      expect(notes.some((note) => note._tag === "ToolCallUpdated")).toBe(false);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_EMIT_LOAD_REPLAY: "1",
            },
          },
          cwd: process.cwd(),
          resumeSessionId: "mock-session-1",
          clientInfo: { name: "t3-test", version: "0.0.0" },
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("completes session/load after replay becomes idle while its RPC stays pending", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      const started = yield* runtime.start().pipe(Effect.timeout("2 seconds"));

      expect(started.sessionId).toBe("mock-session-1");
      expect(started.sessionSetupResult._meta).toMatchObject({
        t3SessionLoadReady: "replay_idle",
      });

      const unexpectedReplayEvent = yield* Stream.runHead(runtime.getEvents()).pipe(
        Effect.timeoutOption("100 millis"),
      );
      expect(Option.isNone(unexpectedReplayEvent)).toBe(true);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_HANG_LOAD_SESSION_AFTER_REPLAY: "1",
              T3_ACP_LOAD_SESSION_DELAY_MS: "10000",
            },
          },
          cwd: process.cwd(),
          resumeSessionId: "mock-session-1",
          sessionLoadReplayIdleGap: "50 millis",
          sessionLoadTimeout: "1 second",
          clientInfo: { name: "t3-test", version: "0.0.0" },
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      TestClock.withLive,
    ),
  );

  it.effect("rejects invalid config option values before sending session/set_config_option", () => {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "acp-runtime-"));
    const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
    return Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const error = yield* runtime.setModel("composer-2[fast=false]").pipe(Effect.flip);
      expect(error._tag).toBe("AcpRequestError");
      if (error._tag === "AcpRequestError") {
        expect(error.code).toBe(-32602);
        expect(error.message).toContain(
          'Invalid value "composer-2[fast=false]" for session config option "model"',
        );
        expect(error.message).toContain("composer-2[fast=true]");
      }

      const recordedRequests = NodeFS.readFileSync(requestLogPath, "utf8")
        .trim()
        .split("\n")
        .filter((line) => line.length > 0)
        .map((line) => JSON.parse(line) as { method?: string; params?: { value?: unknown } });
      expect(
        recordedRequests.some(
          (message) =>
            message.method === "session/set_config_option" &&
            message.params?.value === "composer-2[fast=false]",
        ),
      ).toBe(false);
    }).pipe(
      Effect.provide(
        AcpSessionRuntime.layer({
          authMethodId: "test",
          spawn: {
            command: mockAgentCommand,
            args: mockAgentArgs,
            env: {
              T3_ACP_REQUEST_LOG_PATH: requestLogPath,
            },
          },
          cwd: process.cwd(),
          clientInfo: { name: "t3-test", version: "0.0.0" },
        }),
      ),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
    );
  });
});
