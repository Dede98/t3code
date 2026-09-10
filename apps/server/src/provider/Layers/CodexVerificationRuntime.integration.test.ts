// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ThreadId,
  type AgentControlVerificationCheck,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";
import { VerificationCheckError } from "../../agentControl/verificationTurn/checkEvidence.ts";
import { AgentControlVerificationExecution } from "../../agentControl/verificationTurn/executionContext.ts";
import {
  CODEX_VERIFICATION_TOOL,
  createCodexVerificationTool,
} from "../CodexVerificationChecks.ts";
import wireFixture from "../testFixtures/codexMultiAgentWire.json" with { type: "json" };
import { makeCodexSessionRuntime } from "./CodexSessionRuntime.ts";

const threadId = ThreadId.make("controlled-verification");
const cwd = NodeFS.realpathSync(NodeOS.tmpdir());
const check: AgentControlVerificationCheck = {
  id: "scoped-tests",
  command: "vp",
  args: ["test", "run", "packages/shared/src/semver.test.ts", "--reporter=json"],
  cwd: ".",
  required: true,
  timeoutMs: 60_000,
  allowTemporaryFiles: false,
  resultFormat: "vitest-json",
};
const runCheck: NonNullable<typeof AgentControlVerificationExecution.Service>["runCheck"] = (
  checkId,
  providerTurnId,
  execute,
) =>
  Effect.gen(function* () {
    expect(checkId).toBe(check.id);
    expect(providerTurnId).toBe(nativeTurnId);
    return yield* execute.pipe(Effect.mapError((cause) => new VerificationCheckError({ cause })));
  });
const authorization = { threadId, cwd, checks: [check], runCheck };
const nativeTurnId = "verification-native-turn";
const toolCall = {
  id: 51,
  method: "item/tool/call",
  params: {
    threadId: wireFixture.rootThreadId,
    turnId: nativeTurnId,
    callId: "check-call",
    tool: CODEX_VERIFICATION_TOOL.name,
    arguments: { check: "scoped-tests" },
  },
};
const peerPath = NodePath.join(
  import.meta.dirname,
  `../testFixtures/codexCollabMockPeer.${HostProcessPlatform.defaultValue() === "win32" ? "cmd" : "sh"}`,
);

const exercise = (input: {
  request?: { id: number; method: string; params: unknown };
  sessionAuthorization?: typeof authorization | null;
  turnAuthorization?: typeof authorization | null;
  runtimeMode?: "full-access" | "approval-required";
  secondTurn?: boolean;
}) =>
  Effect.gen(function* () {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-check-runtime-"));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
    );
    const scriptPath = NodePath.join(dir, "script.json");
    NodeFS.writeFileSync(
      scriptPath,
      JSON.stringify({
        rootThreadId: wireFixture.rootThreadId,
        turnIds: [nativeTurnId, nativeTurnId],
        notifications: [],
        holdTurnOpen: true,
        completeTurnOnServerResponse: true,
        recordAllRequests: true,
        serverRequests: [input.request ?? toolCall],
      }),
    );
    const runtime = yield* makeCodexSessionRuntime({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
      binaryPath: peerPath,
      cwd,
      runtimeMode: input.runtimeMode ?? "approval-required",
      environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
    }).pipe(
      Effect.provideService(
        AgentControlVerificationExecution,
        input.sessionAuthorization === undefined ? authorization : input.sessionAuthorization,
      ),
    );
    yield* runtime.start();
    let approvalRequests = 0;
    const runTurn = (context: typeof authorization | null) =>
      Effect.gen(function* () {
        const completed = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.kind === "request") approvalRequests += 1;
            return event.method === "turn/completed"
              ? Deferred.succeed(completed, undefined)
              : Effect.void;
          }),
          Effect.forkScoped,
        );
        yield* runtime
          .sendTurn({ input: "Verify using the fixed checks" })
          .pipe(Effect.provideService(AgentControlVerificationExecution, context));
        yield* Deferred.await(completed);
      }).pipe(Effect.scoped);
    yield* runTurn(input.turnAuthorization === undefined ? authorization : input.turnAuthorization);
    if (input.secondTurn) yield* runTurn(null);
    const requests = NodeFS.readFileSync(`${scriptPath}.requests`, "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { method: string; params: Record<string, unknown> });
    const responses = NodeFS.readFileSync(`${scriptPath}.responses`, "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { result: unknown });
    yield* runtime.close;
    return { requests, responses, approvalRequests };
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

describe("Codex controlled Verification checks", () => {
  it.effect(
    "registers the tool on the wire and executes a fixed check without widening shell policy",
    () =>
      Effect.gen(function* () {
        const result = yield* exercise({});
        expect(result.requests.find((r) => r.method === "thread/start")?.params).toMatchObject({
          dynamicTools: [createCodexVerificationTool([check])],
          approvalPolicy: "untrusted",
          sandbox: "read-only",
        });
        expect(result.requests.find((r) => r.method === "turn/start")?.params).toMatchObject({
          approvalPolicy: "untrusted",
          sandboxPolicy: { type: "readOnly" },
        });
        expect(result.requests.filter((r) => r.method === "command/exec")).toEqual([
          {
            method: "command/exec",
            params: {
              command: [check.command, ...check.args],
              cwd,
              sandboxPolicy: { type: "readOnly", networkAccess: false },
              timeoutMs: 60_000,
              outputBytesCap: 32_768,
            },
          },
        ]);
        expect(result.responses[0]?.result).toMatchObject({ success: true });
        expect(result.approvalRequests).toBe(0);
      }),
  );

  it.effect.each([
    { threadId: "unrelated-native-thread" },
    { turnId: "stale-native-turn" },
    { tool: "exec" },
    { namespace: "other" },
    { arguments: { check: "scoped-tests", command: ["python3", "-c", "print(1)"] } },
    { arguments: { check: "scoped-tests", cwd: "/" } },
    { arguments: { check: "scoped-tests; touch changed" } },
  ])("rejects calls outside the exact tool contract: %j", (params) =>
    Effect.gen(function* () {
      const result = yield* exercise({
        request: { ...toolCall, params: { ...toolCall.params, ...params } },
      });
      expect(result.requests.filter((r) => r.method === "command/exec")).toEqual([]);
      expect(result.responses[0]?.result).toMatchObject({ success: false });
    }),
  );

  it.effect.each([
    { sessionAuthorization: null },
    { turnAuthorization: null },
    { sessionAuthorization: { ...authorization, cwd: "/" } },
    { turnAuthorization: { ...authorization, threadId: ThreadId.make("other") } },
    { runtimeMode: "full-access" as const },
  ])("requires admitted session and turn context: %j", (input) =>
    Effect.gen(function* () {
      const result = yield* exercise(input);
      expect(result.requests.filter((r) => r.method === "command/exec")).toEqual([]);
      expect(result.responses[0]?.result).toMatchObject({ success: false });
    }),
  );

  it.effect("cannot execute a registered check when its evidence authority rejects the claim", () =>
    Effect.gen(function* () {
      const result = yield* exercise({
        turnAuthorization: {
          ...authorization,
          runCheck: () => new VerificationCheckError({ cause: "stale verification fence" }),
        },
      });
      expect(result.requests.filter((request) => request.method === "command/exec")).toEqual([]);
      expect(result.responses[0]?.result).toMatchObject({ success: false });
    }),
  );

  it.effect("does not retain authorization for the next turn", () =>
    Effect.gen(function* () {
      const result = yield* exercise({ secondTurn: true });
      expect(result.requests.filter((r) => r.method === "command/exec")).toHaveLength(1);
      expect(result.responses.map((r) => r.result)).toMatchObject([
        { success: true },
        { success: false },
      ]);
    }),
  );

  it.effect.each(["item/commandExecution/requestApproval", "item/fileChange/requestApproval"])(
    "declines unexpected %s without asking a client",
    (method) =>
      Effect.gen(function* () {
        const result = yield* exercise({
          request: {
            id: 51,
            method,
            params: {
              threadId: wireFixture.rootThreadId,
              turnId: nativeTurnId,
              itemId: "unsafe-command",
              startedAtMs: 1,
              command: "python3 -c 'print(open(\"/etc/hosts\").read())'",
              cwd,
            },
          },
        });
        expect(result.responses[0]?.result).toEqual({ decision: "decline" });
        expect(result.approvalRequests).toBe(0);
        expect(result.requests.filter((r) => r.method === "command/exec")).toEqual([]);
      }),
  );
});
