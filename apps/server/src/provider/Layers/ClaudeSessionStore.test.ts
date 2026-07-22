import type { SessionKey, SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ClaudeSessionStore } from "../Services/ClaudeSessionStore.ts";
import { ClaudeSessionStoreLive } from "./ClaudeSessionStore.ts";

const storeLayer = it.layer(
  ClaudeSessionStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

const mainKey = (projectKey: string, sessionId: string): SessionKey => ({
  projectKey,
  sessionId,
});

storeLayer("ClaudeSessionStore", (it) => {
  it.effect("round-trips large ordered batches of opaque transcript entries", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const key = mainKey("project-batch", "00000000-0000-4000-8000-000000000001");
      const entries = Array.from({ length: 1_001 }, (_, index) => ({
        type: index % 2 === 0 ? "assistant" : "user",
        uuid: `batch-entry-${index}`,
        timestamp: "2026-07-16T12:00:00.000Z",
        message: {
          index,
          nested: ["opaque", { retained: true }],
        },
      })) satisfies SessionStoreEntry[];

      yield* Effect.promise(() => store.append(key, entries));
      const loaded = yield* Effect.promise(() => store.load(key));

      assert.deepEqual(loaded, entries);
    }),
  );

  it.effect("upserts UUID entries in place while always appending entries without UUIDs", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const key = mainKey("project-dedupe", "00000000-0000-4000-8000-000000000002");
      const stable = { type: "assistant", uuid: "stable-uuid", payload: "first" };
      const marker = { type: "mode", mode: "plan" };

      yield* Effect.promise(() => store.append(key, [stable, marker]));
      yield* Effect.promise(() =>
        store.append(key, [
          { ...stable, payload: "latest" },
          marker,
          { type: "assistant", uuid: "second-uuid" },
        ]),
      );

      assert.deepEqual(yield* Effect.promise(() => store.load(key)), [
        { ...stable, payload: "latest" },
        marker,
        marker,
        { type: "assistant", uuid: "second-uuid" },
      ]);
    }),
  );

  it.effect("lists only main sessions and maintains strictly increasing mtimes", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const projectKey = "project-list";
      const firstKey = mainKey(projectKey, "00000000-0000-4000-8000-000000000003");
      const secondKey = mainKey(projectKey, "00000000-0000-4000-8000-000000000004");

      yield* Effect.promise(() => store.append(firstKey, []));
      const firstMtime = (yield* Effect.promise(() => store.listSessions!(projectKey))).find(
        (session) => session.sessionId === firstKey.sessionId,
      )?.mtime;
      yield* Effect.promise(() =>
        store.append({ ...firstKey, subpath: "subagents/agent-a" }, [
          { type: "assistant", uuid: "subagent-entry" },
        ]),
      );
      yield* Effect.promise(() => store.append(secondKey, [{ type: "user" }]));
      yield* Effect.promise(() => store.append(firstKey, [{ type: "assistant" }]));

      const sessions = yield* Effect.promise(() => store.listSessions!(projectKey));
      const updatedFirstMtime = sessions.find(
        (session) => session.sessionId === firstKey.sessionId,
      )?.mtime;
      assert.deepEqual(
        sessions.map((session) => session.sessionId),
        [firstKey.sessionId, secondKey.sessionId],
      );
      assert.equal(typeof firstMtime, "number");
      assert.equal(typeof updatedFirstMtime, "number");
      assert.ok(updatedFirstMtime! > firstMtime!);
    }),
  );

  it.effect("lists subkeys and deletes either one subkey or a whole session", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const key = mainKey("project-delete", "00000000-0000-4000-8000-000000000005");
      const firstSubkey = { ...key, subpath: "subagents/agent-b" };
      const secondSubkey = { ...key, subpath: "subagents/agent-a" };

      yield* Effect.promise(() => store.append(key, [{ type: "user" }]));
      yield* Effect.promise(() => store.append(firstSubkey, [{ type: "assistant" }]));
      yield* Effect.promise(() => store.append(secondSubkey, [{ type: "assistant" }]));

      assert.deepEqual(yield* Effect.promise(() => store.listSubkeys!(key)), [
        secondSubkey.subpath,
        firstSubkey.subpath,
      ]);

      yield* Effect.promise(() => store.delete!(firstSubkey));
      assert.equal(yield* Effect.promise(() => store.load(firstSubkey)), null);
      assert.deepEqual(yield* Effect.promise(() => store.listSubkeys!(key)), [
        secondSubkey.subpath,
      ]);

      yield* Effect.promise(() => store.delete!(key));
      assert.equal(yield* Effect.promise(() => store.load(key)), null);
      assert.equal(yield* Effect.promise(() => store.load(secondSubkey)), null);
      assert.deepEqual(yield* Effect.promise(() => store.listSubkeys!(key)), []);
    }),
  );

  it.effect("loads by canonical session id after a project key change", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const sessionId = "00000000-0000-4000-8000-000000000007";
      const originalKey = mainKey("project-before-move", sessionId);
      const movedKey = mainKey("project-after-move", sessionId);
      const originalEntry = { type: "user", uuid: "before-move" };

      yield* Effect.promise(() => store.append(originalKey, [originalEntry]));

      assert.deepEqual(yield* Effect.promise(() => store.load(movedKey)), [originalEntry]);
      assert.deepEqual(yield* Effect.promise(() => store.listSessions!("project-after-move")), []);

      yield* Effect.promise(() =>
        store.append({ ...movedKey, subpath: "subagents/after-move" }, [
          { type: "assistant", uuid: "after-move-subagent" },
        ]),
      );
      assert.deepEqual(
        (yield* Effect.promise(() => store.listSessions!("project-after-move"))).map(
          (session) => session.sessionId,
        ),
        [sessionId],
      );
      assert.deepEqual(yield* Effect.promise(() => store.listSessions!("project-before-move")), []);

      yield* Effect.promise(() =>
        store.append(movedKey, [{ type: "assistant", uuid: "after-move" }]),
      );
      assert.deepEqual(yield* Effect.promise(() => store.load(movedKey)), [
        originalEntry,
        { type: "assistant", uuid: "after-move" },
      ]);
    }),
  );

  it.effect("atomically replaces and snapshots a complete session bundle", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const sessionId = "00000000-0000-4000-8000-000000000008";
      const oldKey = mainKey("project-bundle-old", sessionId);
      yield* Effect.promise(() => store.append(oldKey, [{ type: "user", uuid: "old" }]));
      yield* Effect.promise(() =>
        store.append({ ...oldKey, subpath: "subagents/old" }, [
          { type: "assistant", uuid: "old-subagent" },
        ]),
      );

      yield* Effect.promise(() =>
        store.replaceSession({
          projectKey: "project-bundle-new",
          sessionId,
          entries: [
            { type: "user", uuid: "new-user", payload: { retained: true } },
            { type: "assistant", uuid: "new-assistant" },
          ],
          subkeys: [
            {
              subpath: "subagents/new",
              entries: [{ type: "assistant", uuid: "new-subagent" }],
            },
          ],
        }),
      );

      assert.deepEqual(
        yield* Effect.promise(() =>
          store.loadSession({ projectKey: "ignored-after-move", sessionId }),
        ),
        {
          projectKey: "project-bundle-new",
          sessionId,
          entries: [
            { type: "user", uuid: "new-user", payload: { retained: true } },
            { type: "assistant", uuid: "new-assistant" },
          ],
          subkeys: [
            {
              subpath: "subagents/new",
              entries: [{ type: "assistant", uuid: "new-subagent" }],
            },
          ],
        },
      );
      assert.equal(
        yield* Effect.promise(() => store.load({ ...oldKey, subpath: "subagents/old" })),
        null,
      );
    }),
  );

  it.effect("rejects empty subpaths and corrupt persisted entries", () =>
    Effect.gen(function* () {
      const store = yield* ClaudeSessionStore;
      const sql = yield* SqlClient.SqlClient;
      const key = mainKey("project-corrupt", "00000000-0000-4000-8000-000000000006");

      const invalidSubpathResult = yield* Effect.result(
        Effect.tryPromise(() => store.append({ ...key, subpath: "" }, [{ type: "user" }])),
      );
      assert.equal(invalidSubpathResult._tag, "Failure");

      yield* Effect.promise(() => store.append(key, [{ type: "user" }]));
      yield* sql`
        UPDATE claude_session_store_entries
        SET entry_json = ${"{"}
        WHERE session_id = ${key.sessionId}
      `;

      const corruptLoadResult = yield* Effect.result(Effect.tryPromise(() => store.load(key)));
      assert.equal(corruptLoadResult._tag, "Failure");
    }),
  );
});
