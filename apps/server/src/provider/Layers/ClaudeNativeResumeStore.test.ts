import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ClaudeSessionStore } from "../Services/ClaudeSessionStore.ts";
import {
  type ClaudeNativeResumeStoreOptions,
  importClaudeNativeSessionToStore,
  makeClaudeNativeResumeStore,
} from "./ClaudeNativeResumeStore.ts";
import { ClaudeSessionStoreLive } from "./ClaudeSessionStore.ts";

const SESSION_ID = "00000000-0000-4000-8000-000000000099";
const PROJECT_KEY = "tmp-project";
const ASSISTANT_UUID = "assistant-checkpoint";
const encodeUnknownJsonString = Schema.encodeSync(Schema.UnknownFromJsonString);
const decodeUnknownJsonString = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);

const testLayer = it.layer(
  Layer.mergeAll(
    NodeServices.layer,
    ClaudeSessionStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
  ),
);

testLayer("ClaudeNativeResumeStore", (it) => {
  it.effect("imports newer source entries without touching account settings", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000096";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-manual-sync-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        const subagentsDirectory = path.join(projectDirectory, sessionId, "subagents");
        const settingsPath = path.join(sourceConfigDirPath, "settings.json");
        yield* fileSystem.makeDirectory(subagentsDirectory, { recursive: true });
        yield* fileSystem.writeFileString(settingsPath, '{"account":"source"}');
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "manual-user" }),
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            "",
          ].join("\n"),
        );
        yield* fileSystem.writeFileString(
          path.join(subagentsDirectory, "agent-manual.jsonl"),
          `${encodeUnknownJsonString({
            type: "assistant",
            uuid: "manual-subagent",
          })}\n`,
        );
        yield* fileSystem.writeFileString(
          path.join(subagentsDirectory, "agent-manual.meta.json"),
          '{"toolUseId":"manual-tool"}',
        );

        const first = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: ASSISTANT_UUID,
          },
          { fileSystem, path },
        );
        assert.equal(first.state, "imported");
        assert.deepEqual(first.snapshot.subkeys, [
          {
            subpath: "subagents/agent-manual",
            entries: [
              { type: "assistant", uuid: "manual-subagent" },
              { type: "agent_metadata", toolUseId: "manual-tool" },
            ],
          },
        ]);
        assert.equal(yield* fileSystem.readFileString(settingsPath), '{"account":"source"}');

        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "manual-user" }),
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            encodeUnknownJsonString({ type: "assistant", uuid: "assistant-newest" }),
            "",
          ].join("\n"),
        );

        const second = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: ASSISTANT_UUID,
          },
          { fileSystem, path },
        );
        assert.equal(second.state, "imported");
        assert.deepEqual(second.snapshot.entries, [
          { type: "user", uuid: "manual-user" },
          { type: "assistant", uuid: ASSISTANT_UUID },
          { type: "assistant", uuid: "assistant-newest" },
        ]);

        const third = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: ASSISTANT_UUID,
          },
          { fileSystem, path },
        );
        assert.equal(third.state, "already-synced");
      }),
    ),
  );

  it.effect("preserves a shared transcript that is newer than the native source", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000095";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-store-newer-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "store-newer-user" }),
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            "",
          ].join("\n"),
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              { type: "user", uuid: "store-newer-user" },
              { type: "assistant", uuid: ASSISTANT_UUID },
              { type: "assistant", uuid: "store-only-newest" },
            ],
            subkeys: [],
          }),
        );

        const result = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: "store-only-newest",
          },
          { fileSystem, path },
        );

        assert.equal(result.state, "already-synced");
        assert.equal(result.snapshot.entries.at(-1)?.uuid, "store-only-newest");
        assert.equal(
          (yield* Effect.promise(() =>
            sharedStore.loadSession({ projectKey: PROJECT_KEY, sessionId }),
          ))?.entries.at(-1)?.uuid,
          "store-only-newest",
        );
      }),
    ),
  );

  it.effect("ignores subagent metadata placement and JSON object key order", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000091";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-metadata-order-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        const subagentsDirectory = path.join(projectDirectory, sessionId, "subagents");
        yield* fileSystem.makeDirectory(subagentsDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          `${encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID })}\n`,
        );
        yield* fileSystem.writeFileString(
          path.join(subagentsDirectory, "agent-order.jsonl"),
          `${encodeUnknownJsonString({ type: "assistant", uuid: "subagent-order" })}\n`,
        );
        yield* fileSystem.writeFileString(
          path.join(subagentsDirectory, "agent-order.meta.json"),
          '{"toolUseId":"tool-order","agentType":"Explore"}',
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [{ uuid: ASSISTANT_UUID, type: "assistant" }],
            subkeys: [
              {
                subpath: "subagents/agent-order",
                entries: [
                  {
                    type: "agent_metadata",
                    agentType: "Explore",
                    toolUseId: "tool-order",
                  },
                  { uuid: "subagent-order", type: "assistant" },
                ],
              },
            ],
          }),
        );

        const result = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: ASSISTANT_UUID,
          },
          { fileSystem, path },
        );

        assert.equal(result.state, "already-synced");
      }),
    ),
  );

  it.effect("ignores superseded main transcript state entries", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000090";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-state-history-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        const sourceEntries = [
          { type: "user", uuid: "state-user" },
          { type: "assistant", uuid: ASSISTANT_UUID },
          { type: "queue-operation", operation: "enqueue", content: "follow-up" },
          { type: "last-prompt", prompt: "latest" },
          { type: "mode", mode: "default" },
          { type: "assistant", uuid: "state-final" },
        ];
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          `${sourceEntries.map((entry) => encodeUnknownJsonString(entry)).join("\n")}\n`,
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              { type: "user", uuid: "state-user" },
              { type: "assistant", uuid: ASSISTANT_UUID },
              { type: "last-prompt", prompt: "superseded" },
              { type: "mode", mode: "plan" },
              { type: "queue-operation", operation: "enqueue", content: "follow-up" },
              { type: "last-prompt", prompt: "latest" },
              { type: "mode", mode: "default" },
              { type: "assistant", uuid: "state-final" },
            ],
            subkeys: [],
          }),
        );

        const result = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: ASSISTANT_UUID,
          },
          { fileSystem, path },
        );

        assert.equal(result.state, "already-synced");
        assert.equal(result.snapshot.entries.length, 8);
      }),
    ),
  );

  it.effect("accepts changed effective state when ordered source history is newer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000089";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-newer-state-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        const sourceEntries = [
          { type: "user", uuid: "newer-state-user" },
          { type: "assistant", uuid: ASSISTANT_UUID },
          { type: "last-prompt", prompt: "new" },
          { type: "mode", mode: "plan" },
          { type: "user", uuid: "newer-state-user-two" },
          { type: "assistant", uuid: "newer-state-assistant" },
        ];
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          `${sourceEntries.map((entry) => encodeUnknownJsonString(entry)).join("\n")}\n`,
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              { type: "user", uuid: "newer-state-user" },
              { type: "assistant", uuid: ASSISTANT_UUID },
              { type: "last-prompt", prompt: "old" },
              { type: "mode", mode: "default" },
            ],
            subkeys: [],
          }),
        );

        const result = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: "newer-state-assistant",
          },
          { fileSystem, path },
        );

        assert.equal(result.state, "imported");
        assert.deepEqual(result.snapshot.entries, sourceEntries);
      }),
    ),
  );

  it.effect("rejects conflicting effective state for the same ordered history", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000088";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-conflicting-state-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "conflicting-state-user" }),
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            encodeUnknownJsonString({ type: "last-prompt", prompt: "source" }),
            "",
          ].join("\n"),
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              { type: "user", uuid: "conflicting-state-user" },
              { type: "assistant", uuid: ASSISTANT_UUID },
              { type: "last-prompt", prompt: "shared" },
            ],
            subkeys: [],
          }),
        );

        const error = yield* Effect.flip(
          importClaudeNativeSessionToStore(
            sharedStore,
            { sessionId, sourceConfigDirPath, projectKey: PROJECT_KEY },
            { fileSystem, path },
          ),
        );

        assert.equal(error.operation, "nativeResume:importDiverged");
        assert.include(error.detail, 'Main effective "last-prompt" state differs.');
      }),
    ),
  );

  it.effect("still rejects reordered or changed queue operations", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000087";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-queue-diverged-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "queue-user" }),
            encodeUnknownJsonString({ type: "queue-operation", operation: "enqueue" }),
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            "",
          ].join("\n"),
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              { type: "user", uuid: "queue-user" },
              { type: "queue-operation", operation: "dequeue" },
              { type: "assistant", uuid: ASSISTANT_UUID },
            ],
            subkeys: [],
          }),
        );

        const error = yield* Effect.flip(
          importClaudeNativeSessionToStore(
            sharedStore,
            { sessionId, sourceConfigDirPath, projectKey: PROJECT_KEY },
            { fileSystem, path },
          ),
        );

        assert.equal(error.operation, "nativeResume:importDiverged");
        assert.include(error.detail, "Main ordered transcript entry 2 differs");
      }),
    ),
  );

  it.effect("accepts normalized assistant accounting for the same subagent message UUID", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000086";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-normalized-accounting-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        const subagentsDirectory = path.join(projectDirectory, sessionId, "subagents");
        const content = [{ type: "tool_use", id: "tool-call", name: "Read", input: {} }];
        yield* fileSystem.makeDirectory(subagentsDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            encodeUnknownJsonString({ type: "assistant", uuid: "native-newest" }),
            "",
          ].join("\n"),
        );
        yield* fileSystem.writeFileString(
          path.join(subagentsDirectory, "agent-accounting.jsonl"),
          `${encodeUnknownJsonString({
            type: "assistant",
            uuid: "stable-subagent-message",
            message: { role: "assistant", content, stop_reason: null, usage: { output_tokens: 1 } },
          })}\n`,
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [{ type: "assistant", uuid: ASSISTANT_UUID }],
            subkeys: [
              {
                subpath: "subagents/agent-accounting",
                entries: [
                  {
                    type: "assistant",
                    uuid: "stable-subagent-message",
                    message: {
                      role: "assistant",
                      content,
                      stop_reason: "tool_use",
                      usage: { output_tokens: 2, iterations: 1 },
                    },
                  },
                ],
              },
            ],
          }),
        );

        const result = yield* importClaudeNativeSessionToStore(
          sharedStore,
          {
            sessionId,
            sourceConfigDirPath,
            projectKey: PROJECT_KEY,
            expectedAssistantUuid: "native-newest",
          },
          { fileSystem, path },
        );

        assert.equal(result.state, "imported");
        assert.equal(result.snapshot.entries.at(-1)?.uuid, "native-newest");
      }),
    ),
  );

  it.effect("rejects changed assistant content even when the UUID is unchanged", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000085";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-changed-message-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          `${encodeUnknownJsonString({
            type: "assistant",
            uuid: ASSISTANT_UUID,
            message: { role: "assistant", content: [{ type: "text", text: "source" }] },
          })}\n`,
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              {
                type: "assistant",
                uuid: ASSISTANT_UUID,
                message: { role: "assistant", content: [{ type: "text", text: "shared" }] },
              },
            ],
            subkeys: [],
          }),
        );

        const error = yield* Effect.flip(
          importClaudeNativeSessionToStore(
            sharedStore,
            { sessionId, sourceConfigDirPath, projectKey: PROJECT_KEY },
            { fileSystem, path },
          ),
        );

        assert.equal(error.operation, "nativeResume:importDiverged");
        assert.include(error.detail, "Main ordered transcript entry 1 differs");
      }),
    ),
  );

  it.effect("refuses to overwrite divergent source and shared transcripts", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000094";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-diverged-",
        });
        const projectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "diverged-user" }),
            encodeUnknownJsonString({ type: "assistant", uuid: "source-assistant" }),
            "",
          ].join("\n"),
        );
        yield* Effect.promise(() =>
          sharedStore.replaceSession({
            projectKey: PROJECT_KEY,
            sessionId,
            entries: [
              { type: "user", uuid: "diverged-user" },
              { type: "assistant", uuid: "store-assistant" },
            ],
            subkeys: [],
          }),
        );

        const error = yield* Effect.flip(
          importClaudeNativeSessionToStore(
            sharedStore,
            {
              sessionId,
              sourceConfigDirPath,
              projectKey: PROJECT_KEY,
            },
            { fileSystem, path },
          ),
        );

        assert.equal(error.operation, "nativeResume:importDiverged");
        assert.equal(
          (yield* Effect.promise(() =>
            sharedStore.loadSession({ projectKey: PROJECT_KEY, sessionId }),
          ))?.entries.at(-1)?.uuid,
          "store-assistant",
        );
      }),
    ),
  );

  it.effect(
    "carries the newest native source entry through the shared store into a target config",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const sessionId = "00000000-0000-4000-8000-000000000093";
          const newestAssistantUuid = "integration-assistant-n-plus-one";
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const sharedStore = yield* ClaudeSessionStore;
          const sourceConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
            prefix: "t3-claude-integration-source-",
          });
          const targetConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
            prefix: "t3-claude-integration-target-",
          });
          const sourceProjectDirectory = path.join(sourceConfigDirPath, "projects", PROJECT_KEY);
          yield* fileSystem.makeDirectory(sourceProjectDirectory, { recursive: true });
          yield* fileSystem.writeFileString(
            path.join(sourceProjectDirectory, `${sessionId}.jsonl`),
            [
              encodeUnknownJsonString({ type: "user", uuid: "integration-user-n" }),
              encodeUnknownJsonString({ type: "assistant", uuid: "integration-assistant-n" }),
              encodeUnknownJsonString({ type: "assistant", uuid: newestAssistantUuid }),
              "",
            ].join("\n"),
          );

          const imported = yield* importClaudeNativeSessionToStore(
            sharedStore,
            {
              sessionId,
              sourceConfigDirPath,
              projectKey: PROJECT_KEY,
              expectedAssistantUuid: newestAssistantUuid,
            },
            { fileSystem, path },
          );
          assert.equal(imported.state, "imported");

          const resumeStore = makeClaudeNativeResumeStore(
            sharedStore,
            {
              sessionId,
              targetConfigDirPath,
              expectedAssistantUuid: newestAssistantUuid,
              readinessTimeoutMs: 0,
            },
            { fileSystem, path },
          );
          const key = { projectKey: PROJECT_KEY, sessionId };
          assert.equal(yield* Effect.promise(() => resumeStore.load(key)), null);

          const targetEntries = (yield* fileSystem.readFileString(
            path.join(targetConfigDirPath, "projects", PROJECT_KEY, `${sessionId}.jsonl`),
          ))
            .trim()
            .split("\n")
            .map((line) => decodeUnknownJsonString(line));
          assert.deepEqual(targetEntries.at(-1), {
            type: "assistant",
            uuid: newestAssistantUuid,
          });

          const loadedEntries = yield* Effect.promise(() => resumeStore.load(key));
          assert.equal(loadedEntries?.at(-1)?.uuid, newestAssistantUuid);
        }),
      ),
  );

  it.effect(
    "materializes shared transcripts into the real target config and preserves settings",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const sharedStore = yield* ClaudeSessionStore;
          const targetConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
            prefix: "t3-claude-native-resume-",
          });
          const settingsPath = path.join(targetConfigDirPath, "settings.json");
          const skillPath = path.join(targetConfigDirPath, "skills", "account-skill", "SKILL.md");
          yield* fileSystem.makeDirectory(path.dirname(skillPath), { recursive: true });
          yield* fileSystem.writeFileString(settingsPath, '{"account":"target"}');
          yield* fileSystem.writeFileString(skillPath, "target-only skill");

          yield* Effect.promise(() =>
            sharedStore.replaceSession({
              projectKey: PROJECT_KEY,
              sessionId: SESSION_ID,
              entries: [
                { type: "user", uuid: "user-one", message: "hello" },
                { type: "assistant", uuid: ASSISTANT_UUID, message: "hi" },
              ],
              subkeys: [
                {
                  subpath: "subagents/agent-a",
                  entries: [
                    { type: "assistant", uuid: "subagent-one", message: "done" },
                    { type: "agent_metadata", toolUseId: "tool-a", parentAgentId: null },
                  ],
                },
              ],
            }),
          );

          const resumeStore = makeClaudeNativeResumeStore(
            sharedStore,
            {
              sessionId: SESSION_ID,
              targetConfigDirPath,
              expectedAssistantUuid: ASSISTANT_UUID,
              readinessTimeoutMs: 0,
            },
            { fileSystem, path },
          );
          const key = { projectKey: PROJECT_KEY, sessionId: SESSION_ID };
          assert.equal(yield* Effect.promise(() => resumeStore.load(key)), null);

          const projectDirectory = path.join(targetConfigDirPath, "projects", PROJECT_KEY);
          const mainText = yield* fileSystem.readFileString(
            path.join(projectDirectory, `${SESSION_ID}.jsonl`),
          );
          const subagentText = yield* fileSystem.readFileString(
            path.join(projectDirectory, SESSION_ID, "subagents", "agent-a.jsonl"),
          );
          const metadataText = yield* fileSystem.readFileString(
            path.join(projectDirectory, SESSION_ID, "subagents", "agent-a.meta.json"),
          );
          assert.match(mainText, new RegExp(ASSISTANT_UUID));
          assert.match(subagentText, /subagent-one/);
          assert.deepEqual(decodeUnknownJsonString(metadataText), {
            toolUseId: "tool-a",
            parentAgentId: null,
          });
          assert.equal(yield* fileSystem.readFileString(settingsPath), '{"account":"target"}');
          assert.equal(yield* fileSystem.readFileString(skillPath), "target-only skill");
          assert.deepEqual(yield* Effect.promise(() => resumeStore.load(key)), [
            { type: "user", uuid: "user-one", message: "hello" },
            { type: "assistant", uuid: ASSISTANT_UUID, message: "hi" },
          ]);
        }),
      ),
  );

  it.effect("imports a legacy native transcript before returning the SDK null fallback", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000098";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const targetConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-legacy-resume-",
        });
        const projectDirectory = path.join(targetConfigDirPath, "projects", PROJECT_KEY);
        yield* fileSystem.makeDirectory(projectDirectory, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(projectDirectory, `${sessionId}.jsonl`),
          [
            encodeUnknownJsonString({ type: "user", uuid: "legacy-user" }),
            encodeUnknownJsonString({ type: "assistant", uuid: ASSISTANT_UUID }),
            "",
          ].join("\n"),
        );

        const resumeStore = makeClaudeNativeResumeStore(
          sharedStore,
          {
            sessionId,
            targetConfigDirPath,
            expectedAssistantUuid: ASSISTANT_UUID,
            readinessTimeoutMs: 0,
          },
          { fileSystem, path },
        );
        const key = { projectKey: PROJECT_KEY, sessionId };
        assert.equal(yield* Effect.promise(() => resumeStore.load(key)), null);
        assert.deepEqual((yield* Effect.promise(() => sharedStore.loadSession(key)))?.entries, [
          { type: "user", uuid: "legacy-user" },
          { type: "assistant", uuid: ASSISTANT_UUID },
        ]);
      }),
    ),
  );

  it.effect("fails safely when neither the shared store nor target config has the checkpoint", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sessionId = "00000000-0000-4000-8000-000000000097";
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedStore = yield* ClaudeSessionStore;
        const targetConfigDirPath = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-missing-resume-",
        });
        const options: ClaudeNativeResumeStoreOptions = {
          sessionId,
          targetConfigDirPath,
          expectedAssistantUuid: ASSISTANT_UUID,
          readinessTimeoutMs: 0,
        };
        const resumeStore = makeClaudeNativeResumeStore(sharedStore, options, {
          fileSystem,
          path,
        });
        const result = yield* Effect.result(
          Effect.tryPromise(() => resumeStore.load({ projectKey: PROJECT_KEY, sessionId })),
        );
        assert.equal(result._tag, "Failure");
      }),
    ),
  );
});
