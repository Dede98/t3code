import { forkSession } from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { makeClaudeNativeForkStore } from "./ClaudeNativeResumeStore.ts";

it.effect(
  "forks the selected account with the real SDK without changing either source or credentials",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-native-fork-" });
        const sessionId = "00000000-0000-4000-8000-000000000021";
        const userId = "00000000-0000-4000-8000-000000000022";
        const assistantId = "00000000-0000-4000-8000-000000000023";
        const sourceText = (account: string) =>
          [
            {
              type: "user",
              uuid: userId,
              parentUuid: null,
              sessionId,
              isSidechain: false,
              timestamp: "2026-09-18T00:00:00Z",
              message: { role: "user", content: `${account} full history `.repeat(100) },
            },
            {
              type: "assistant",
              uuid: assistantId,
              parentUuid: userId,
              sessionId,
              isSidechain: false,
              timestamp: "2026-09-18T00:00:01Z",
              message: {
                role: "assistant",
                content: [{ type: "text", text: `${account} answer` }],
              },
            },
          ]
            .map((entry) => JSON.stringify(entry))
            .join("\n") + "\n";
        for (const account of ["selected", "unrelated"]) {
          const home = path.join(root, account);
          yield* fileSystem.makeDirectory(path.join(home, "projects", "-workspace"), {
            recursive: true,
          });
          yield* fileSystem.writeFileString(
            path.join(home, "projects", "-workspace", `${sessionId}.jsonl`),
            sourceText(account),
          );
          yield* fileSystem.writeFileString(
            path.join(home, ".credentials.json"),
            `${account}-credentials`,
          );
        }
        const selected = path.join(root, "selected");
        const store = makeClaudeNativeForkStore(selected, sessionId, { fileSystem, path });
        const fork = yield* Effect.promise(() =>
          forkSession(sessionId, {
            dir: "/workspace",
            upToMessageId: assistantId,
            sessionStore: store,
          }),
        );
        const destination = path.join(
          selected,
          "projects",
          "-workspace",
          `${fork.sessionId}.jsonl`,
        );
        const forkText = yield* fileSystem.readFileString(destination);
        assert.include(forkText, "selected full history ".repeat(100));
        assert.notInclude(forkText, "unrelated");
        assert.notEqual(fork.sessionId, sessionId);
        const retry = yield* Effect.result(
          Effect.tryPromise(() =>
            store.append(
              {
                projectKey: "-workspace",
                sessionId: fork.sessionId,
              },
              [{ type: "user", uuid: "overwrite", message: "must not overwrite" }],
            ),
          ),
        );
        assert.equal(retry._tag, "Failure");
        assert.equal(yield* fileSystem.readFileString(destination), forkText);
        for (const account of ["selected", "unrelated"]) {
          const home = path.join(root, account);
          assert.equal(
            yield* fileSystem.readFileString(
              path.join(home, "projects", "-workspace", `${sessionId}.jsonl`),
            ),
            sourceText(account),
          );
          assert.equal(
            yield* fileSystem.readFileString(path.join(home, ".credentials.json")),
            `${account}-credentials`,
          );
        }
        assert.deepEqual(
          yield* fileSystem.readDirectory(path.join(root, "unrelated", "projects", "-workspace")),
          [`${sessionId}.jsonl`],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
