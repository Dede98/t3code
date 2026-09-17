// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { query } from "@anthropic-ai/claude-agent-sdk";
import { ClaudeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "@effect/vitest";

import { makeClaudeEnvironment } from "./ClaudeHome.ts";
import { materializeClaudeSharedHome } from "./ClaudeSharedHome.ts";

const run = NodeUtil.promisify(NodeChildProcess.execFile);
const binary = process.env.T3_TEST_CLAUDE_BINARY;
const decodeSettings = Schema.decodeUnknownSync(ClaudeSettings);
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      model: Schema.optionalKey(Schema.String),
      stream: Schema.optionalKey(Schema.Boolean),
      system: Schema.optionalKey(Schema.Unknown),
      messages: Schema.optionalKey(Schema.Unknown),
    }),
  ),
);

// No subscription or API service is used. A real CLI reads/writes the filesystem
// and sends requests to this loopback fixture. Run when changing the native
// memory contract: T3_TEST_CLAUDE_BINARY=/path/to/claude vp test run <this file>.
it.live.skipIf(!binary)(
  "shares Claude memory, skills and instructions across accounts and SDK/CLI sessions without sharing transcripts",
  () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(async () =>
        NodeFSP.realpath(
          await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-memory-")),
        ),
      );
      const environments = new Map<string, NodeJS.ProcessEnv>();
      for (const account of ["account-a", "account-b"]) {
        for (const shared of [true, false]) {
          environments.set(
            `${account}:${shared}`,
            yield* makeClaudeEnvironment(
              decodeSettings({
                configDirPath: NodePath.join(root, account),
                sharedHomePath: shared ? NodePath.join(root, "shared") : "",
              }),
              {
                PATH: process.env.PATH,
                HOME: NodePath.join(root, "home"),
                CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
                DISABLE_AUTOUPDATER: "1",
                DISABLE_TELEMETRY: "1",
                ANTHROPIC_API_KEY: "fixture-not-a-real-key",
              },
            ),
          );
        }
      }
      const instructionMarker = "T3_SHARED_GLOBAL_INSTRUCTIONS_CANARY";
      const ruleMarker = "T3_SHARED_GLOBAL_RULE_CANARY";
      const skillMarker = "T3_SHARED_SKILL_BODY_CANARY";
      yield* Effect.promise(async () => {
        const shared = NodePath.join(root, "shared");
        await NodeFSP.mkdir(NodePath.join(shared, "rules"), { recursive: true });
        await NodeFSP.mkdir(NodePath.join(shared, "skills", "shared-example"), { recursive: true });
        await NodeFSP.writeFile(NodePath.join(shared, "CLAUDE.md"), instructionMarker);
        await NodeFSP.writeFile(NodePath.join(shared, "rules", "style.md"), ruleMarker);
        await NodeFSP.writeFile(
          NodePath.join(shared, "skills", "shared-example", "SKILL.md"),
          `---\ndescription: Shared skill fixture\ndisable-model-invocation: true\n---\n${skillMarker}\n`,
        );
      });
      for (const account of ["account-a", "account-b"]) {
        yield* materializeClaudeSharedHome(
          decodeSettings({
            configDirPath: NodePath.join(root, account),
            sharedHomePath: NodePath.join(root, "shared"),
          }),
        );
      }
      return yield* Effect.promise(async () => {
        const repository = NodePath.join(root, "repo");
        const worktree = NodePath.join(root, "worktree");
        const otherRepository = NodePath.join(root, "other-repo");
        const shared = NodePath.join(root, "shared");
        const projectMemory = NodePath.join(
          shared,
          "projects",
          repository.replace(/[^a-zA-Z0-9]/g, "-"),
          "memory",
          "MEMORY.md",
        );
        const marker = "T3_SHARED_PROJECT_MEMORY_CANARY";
        const agentMarker = "T3_SHARED_USER_AGENT_MEMORY_CANARY";
        let writeMemory = false;
        const requests: string[] = [];
        const server = NodeHttp.createServer(async (request, response) => {
          try {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const input = decodeRequest(Buffer.concat(chunks).toString("utf8"));
            requests.push(JSON.stringify({ system: input.system, messages: input.messages }));
            if (request.url?.includes("count_tokens")) {
              response.setHeader("Content-Type", "application/json");
              response.end(JSON.stringify({ input_tokens: 100 }));
              return;
            }
            const writing = writeMemory;
            writeMemory = false;
            const block = writing
              ? {
                  type: "tool_use",
                  id: "tool_memory",
                  name: "Write",
                  input: { file_path: projectMemory, content: `${marker}\n` },
                }
              : { type: "text", text: "done" };
            const message = {
              id: "msg_memory",
              type: "message",
              role: "assistant",
              model: input.model,
              content: [block],
              stop_reason: writing ? "tool_use" : "end_turn",
              stop_sequence: null,
              usage: { input_tokens: 100, output_tokens: 10 },
            };
            if (!input.stream) {
              response.setHeader("Content-Type", "application/json");
              response.end(JSON.stringify(message));
              return;
            }
            response.setHeader("Content-Type", "text/event-stream");
            const event = (type: string, value: unknown) =>
              response.write(`event: ${type}\ndata: ${JSON.stringify(value)}\n\n`);
            event("message_start", {
              type: "message_start",
              message: { ...message, content: [], stop_reason: null },
            });
            event("content_block_start", {
              type: "content_block_start",
              index: 0,
              content_block: writing ? { ...block, input: {} } : { type: "text", text: "" },
            });
            event("content_block_delta", {
              type: "content_block_delta",
              index: 0,
              delta: writing
                ? { type: "input_json_delta", partial_json: JSON.stringify(block.input) }
                : { type: "text_delta", text: "done" },
            });
            event("content_block_stop", { type: "content_block_stop", index: 0 });
            event("message_delta", {
              type: "message_delta",
              delta: { stop_reason: message.stop_reason, stop_sequence: null },
              usage: { output_tokens: 10 },
            });
            event("message_stop", { type: "message_stop" });
            response.end();
          } catch (error) {
            response.statusCode = 500;
            response.end(String(error));
          }
        });

        try {
          await new Promise<void>((resolve, reject) => {
            server.once("error", reject);
            server.listen(0, "127.0.0.1", resolve);
          });
          const address = server.address();
          if (address === null || typeof address === "string")
            throw new Error("Missing test server port");
          await NodeFSP.mkdir(repository);
          await NodeFSP.mkdir(otherRepository);
          await run("git", ["init", "-q", repository]);
          await run("git", [
            "-C",
            repository,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "fixture",
          ]);
          await run("git", ["-C", repository, "worktree", "add", "-q", "--detach", worktree]);

          const session = async (
            account: string,
            cwd: string,
            options: { cli?: boolean; shared?: boolean; agent?: string; prompt?: string } = {},
          ) => {
            const configDirPath = NodePath.join(root, account);
            await NodeFSP.mkdir(configDirPath, { recursive: true });
            const env = {
              ...environments.get(`${account}:${options.shared !== false}`),
              ANTHROPIC_BASE_URL: `http://127.0.0.1:${address.port}`,
            };
            const start = requests.length;
            const settings = { disableAllHooks: true, autoMemoryEnabled: true };
            if (options.cli) {
              await run(
                binary!,
                [
                  "-p",
                  options.prompt ?? "Use memory for this task.",
                  "--model",
                  "claude-sonnet-4-6",
                  "--tools",
                  "Write",
                  "--settings",
                  JSON.stringify(settings),
                ],
                { cwd, env, timeout: 20_000 },
              );
            } else {
              const sessionQuery = query({
                prompt: options.prompt ?? "Use memory for this task.",
                options: {
                  cwd,
                  env,
                  pathToClaudeCodeExecutable: binary!,
                  model: "claude-sonnet-4-6",
                  systemPrompt: { type: "preset", preset: "claude_code" },
                  settingSources: ["user", "project", "local"],
                  tools: ["Write"],
                  settings,
                  permissionMode: "bypassPermissions",
                  allowDangerouslySkipPermissions: true,
                  ...(options.agent ? { agent: options.agent } : {}),
                },
              });
              try {
                for await (const message of sessionQuery) {
                  if (message.type === "result") expect(message.subtype).toBe("success");
                }
              } finally {
                sessionQuery.close();
              }
            }
            expect(requests.length).toBeGreaterThan(start);
            return requests.slice(start).join("\n");
          };

          writeMemory = true;
          const firstRequest = await session("account-a", repository);
          expect(firstRequest).toContain(instructionMarker);
          expect(firstRequest).toContain(ruleMarker);
          expect(await NodeFSP.readFile(projectMemory, "utf8")).toContain(marker);
          const secondRequest = await session("account-b", repository);
          expect(secondRequest).toContain(marker);
          expect(secondRequest).toContain(instructionMarker);
          expect(secondRequest).toContain(ruleMarker);
          const cliRequest = await session("account-b", repository, { cli: true });
          expect(cliRequest).toContain(marker);
          expect(cliRequest).toContain(instructionMarker);
          expect(cliRequest).toContain(ruleMarker);
          expect(await session("account-a", repository, { prompt: "/shared-example" })).toContain(
            skillMarker,
          );
          expect(
            await session("account-b", repository, { cli: true, prompt: "/shared-example" }),
          ).toContain(skillMarker);
          expect(await session("account-b", worktree)).toContain(marker);
          expect(await session("account-b", otherRepository)).not.toContain(marker);
          expect(await session("account-b", repository, { shared: false })).not.toContain(marker);

          for (const account of ["account-a", "account-b"]) {
            const agents = NodePath.join(root, account, "agents");
            await NodeFSP.mkdir(agents, { recursive: true });
            await NodeFSP.writeFile(
              NodePath.join(agents, "reviewer.md"),
              "---\nname: reviewer\ndescription: Test reviewer\nmemory: user\n---\nUse your persistent memory.\n",
            );
            expect(
              (await NodeFSP.readdir(NodePath.join(root, account, "projects"))).length,
            ).toBeGreaterThan(0);
          }
          const agentMemory = NodePath.join(shared, "agent-memory", "reviewer");
          await NodeFSP.mkdir(agentMemory, { recursive: true });
          await NodeFSP.writeFile(NodePath.join(agentMemory, "MEMORY.md"), agentMarker);
          expect(await session("account-a", repository, { agent: "reviewer" })).toContain(
            agentMarker,
          );
          expect(await session("account-b", repository, { agent: "reviewer" })).toContain(
            agentMarker,
          );
          // Sharing the memory root must not put account transcripts in it.
          expect(await NodeFSP.readdir(NodePath.dirname(NodePath.dirname(projectMemory)))).toEqual([
            "memory",
          ]);
        } finally {
          server.closeAllConnections();
          await new Promise<void>((resolve, reject) =>
            server.close((error) => (error ? reject(error) : resolve())),
          );
          await NodeFSP.rm(root, { recursive: true, force: true });
        }
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);
