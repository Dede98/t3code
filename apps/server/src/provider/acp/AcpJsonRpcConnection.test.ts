// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
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
import type * as EffectAcpProtocol from "effect-acp/protocol";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");
const mockAgentCommand = "node";
const mockAgentArgs = [mockAgentPath];

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

describe("AcpSessionRuntime", () => {
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

  it.effect("suppresses generic placeholder tool updates until completion", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();

      const promptResult = yield* runtime.prompt({
        prompt: [{ type: "text", text: "hi" }],
      });
      expect(promptResult).toMatchObject({ stopReason: "end_turn" });

      const notes = Array.from(yield* Stream.runCollect(Stream.take(runtime.getEvents(), 1)));
      expect(notes.map((note) => note._tag)).toEqual(["ToolCallUpdated"]);
      const toolCall = notes[0];
      expect(toolCall?._tag).toBe("ToolCallUpdated");
      if (toolCall?._tag === "ToolCallUpdated") {
        expect(toolCall.toolCall.status).toBe("completed");
        expect(toolCall.toolCall.title).toBe("Read file");
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
