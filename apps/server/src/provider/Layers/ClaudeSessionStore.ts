import type { SessionKey, SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  type ClaudeSessionSnapshot,
  ClaudeSessionStore,
  ClaudeSessionStoreError,
  type ClaudeSessionStoreShape,
} from "../Services/ClaudeSessionStore.ts";

const MAIN_TRANSCRIPT_SUBPATH = "";
const MAX_ENTRIES_PER_INSERT = 500;

interface NormalizedSessionKey {
  readonly projectKey: string;
  readonly sessionId: string;
  readonly subpath: string;
}

interface StoredEntryRow {
  readonly entryJson: string;
}

interface StoredSessionRow {
  readonly sessionId: string;
  readonly mtime: number;
}

interface StoredSubkeyRow {
  readonly subpath: string;
}

interface StoreClockRow {
  readonly mtime: number;
}

interface StoredMainKeyRow {
  readonly projectKey: string;
}

interface StoredBundleEntryRow extends StoredEntryRow {
  readonly subpath: string;
}

const encodeEntryJson = Schema.encodeUnknownEffect(Schema.UnknownFromJsonString);
const decodeEntryJson = Schema.decodeUnknownEffect(Schema.UnknownFromJsonString);
const isClaudeSessionStoreError = Schema.is(ClaudeSessionStoreError);

function makeStoreError(operation: string, detail: string) {
  return (cause: unknown): ClaudeSessionStoreError =>
    new ClaudeSessionStoreError({
      operation,
      detail,
      cause,
    });
}

function normalizeKey(
  key: SessionKey,
): Effect.Effect<NormalizedSessionKey, ClaudeSessionStoreError> {
  if (key.subpath === "") {
    return Effect.fail(
      new ClaudeSessionStoreError({
        operation: "normalizeKey",
        detail: "An empty subpath is invalid; omit subpath for the main transcript.",
      }),
    );
  }

  return Effect.succeed({
    projectKey: key.projectKey,
    sessionId: key.sessionId,
    subpath: key.subpath ?? MAIN_TRANSCRIPT_SUBPATH,
  });
}

function chunksOf<A>(values: ReadonlyArray<A>, size: number): ReadonlyArray<ReadonlyArray<A>> {
  const chunks: Array<ReadonlyArray<A>> = [];
  for (let offset = 0; offset < values.length; offset += size) {
    chunks.push(values.slice(offset, offset + size));
  }
  return chunks;
}

function isSessionStoreEntry(value: unknown): value is SessionStoreEntry {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "type" in value &&
    typeof value.type === "string"
  );
}

const makeClaudeSessionStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const runtimeContext = yield* Effect.context<never>();
  const runPromise = Effect.runPromiseWith(runtimeContext);

  const appendEffect = Effect.fn("ClaudeSessionStore.append")(function* (
    key: SessionKey,
    entries: SessionStoreEntry[],
  ) {
    const normalizedKey = yield* normalizeKey(key);
    const encodedEntries = yield* Effect.forEach(entries, (entry) =>
      encodeEntryJson(entry).pipe(
        Effect.mapError(
          makeStoreError("append:encodeEntry", "A transcript entry was not JSON serializable."),
        ),
        Effect.map((entryJson) => ({
          session_id: normalizedKey.sessionId,
          subpath: normalizedKey.subpath,
          entry_uuid: entry.uuid ?? null,
          entry_json: entryJson,
        })),
      ),
    );

    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const clockRows = yield* sql<StoreClockRow>`
            UPDATE claude_session_store_clock
            SET last_mtime_ms = MAX(last_mtime_ms + 1, ${yield* Clock.currentTimeMillis})
            WHERE singleton = 1
            RETURNING last_mtime_ms AS "mtime"
          `;
          const mtime = clockRows[0]?.mtime;
          if (mtime === undefined) {
            return yield* new ClaudeSessionStoreError({
              operation: "append:advanceClock",
              detail: "The persistent modification-time clock was not initialized.",
            });
          }

          yield* sql`
            UPDATE claude_session_store_keys
            SET project_key = ${normalizedKey.projectKey}
            WHERE session_id = ${normalizedKey.sessionId}
          `;

          yield* sql`
            INSERT INTO claude_session_store_keys (
              project_key,
              session_id,
              subpath,
              mtime_ms
            )
            VALUES (
              ${normalizedKey.projectKey},
              ${normalizedKey.sessionId},
              ${normalizedKey.subpath},
              ${mtime}
            )
            ON CONFLICT (session_id, subpath)
            DO UPDATE SET
              project_key = excluded.project_key,
              mtime_ms = excluded.mtime_ms
          `;

          yield* Effect.forEach(
            chunksOf(encodedEntries, MAX_ENTRIES_PER_INSERT),
            (entryChunk) =>
              sql`
                INSERT INTO claude_session_store_entries ${sql.insert(entryChunk)}
                ON CONFLICT DO NOTHING
              `,
            { discard: true },
          );
        }),
      )
      .pipe(
        Effect.mapError(makeStoreError("append:persist", "Failed to persist a transcript batch.")),
      );
  });

  const loadEffect = Effect.fn("ClaudeSessionStore.load")(function* (key: SessionKey) {
    const normalizedKey = yield* normalizeKey(key);

    const result = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const keyRows = yield* sql<{ readonly present: number }>`
            SELECT 1 AS "present"
            FROM claude_session_store_keys
            WHERE session_id = ${normalizedKey.sessionId}
              AND subpath = ${normalizedKey.subpath}
          `;
          if (keyRows.length === 0) {
            return null;
          }

          const entryRows = yield* sql<StoredEntryRow>`
            SELECT entry_json AS "entryJson"
            FROM claude_session_store_entries
            WHERE session_id = ${normalizedKey.sessionId}
              AND subpath = ${normalizedKey.subpath}
            ORDER BY sequence ASC
          `;

          return yield* Effect.forEach(entryRows, (row) =>
            decodeEntryJson(row.entryJson).pipe(
              Effect.mapError(
                makeStoreError("load:decodeEntry", "A stored transcript entry is invalid JSON."),
              ),
              Effect.flatMap((entry) =>
                isSessionStoreEntry(entry)
                  ? Effect.succeed(entry)
                  : Effect.fail(
                      new ClaudeSessionStoreError({
                        operation: "load:validateEntry",
                        detail: "A stored transcript entry does not have a string type field.",
                      }),
                    ),
              ),
            ),
          );
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isClaudeSessionStoreError(cause)
            ? cause
            : makeStoreError("load:query", "Failed to load a transcript.")(cause),
        ),
      );

    return result;
  });

  const listSessionsEffect = Effect.fn("ClaudeSessionStore.listSessions")(function* (
    projectKey: string,
  ) {
    const rows = yield* sql<StoredSessionRow>`
      SELECT session_id AS "sessionId", mtime_ms AS "mtime"
      FROM claude_session_store_keys
      WHERE project_key = ${projectKey}
        AND subpath = ${MAIN_TRANSCRIPT_SUBPATH}
      ORDER BY mtime_ms DESC, session_id ASC
    `.pipe(
      Effect.mapError(
        makeStoreError("listSessions:query", "Failed to list stored Claude sessions."),
      ),
    );
    return Array.from(rows);
  });

  const listSubkeysEffect = Effect.fn("ClaudeSessionStore.listSubkeys")(function* (key: {
    projectKey: string;
    sessionId: string;
  }) {
    const rows = yield* sql<StoredSubkeyRow>`
      SELECT subpath
      FROM claude_session_store_keys
      WHERE session_id = ${key.sessionId}
        AND subpath <> ${MAIN_TRANSCRIPT_SUBPATH}
      ORDER BY subpath ASC
    `.pipe(
      Effect.mapError(
        makeStoreError("listSubkeys:query", "Failed to list stored Claude session subkeys."),
      ),
    );
    return rows.map((row) => row.subpath);
  });

  const deleteEffect = Effect.fn("ClaudeSessionStore.delete")(function* (key: SessionKey) {
    const normalizedKey = yield* normalizeKey(key);
    if (normalizedKey.subpath === MAIN_TRANSCRIPT_SUBPATH) {
      yield* sql`
        DELETE FROM claude_session_store_keys
        WHERE session_id = ${normalizedKey.sessionId}
      `.pipe(
        Effect.mapError(
          makeStoreError("delete:query", "Failed to delete a stored Claude session."),
        ),
      );
      return;
    }

    yield* sql`
      DELETE FROM claude_session_store_keys
      WHERE session_id = ${normalizedKey.sessionId}
        AND subpath = ${normalizedKey.subpath}
    `.pipe(
      Effect.mapError(
        makeStoreError("delete:query", "Failed to delete a stored Claude session subkey."),
      ),
    );
  });

  const loadSessionEffect = Effect.fn("ClaudeSessionStore.loadSession")(function* (key: {
    readonly projectKey: string;
    readonly sessionId: string;
  }) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const mainRows = yield* sql<StoredMainKeyRow>`
            SELECT project_key AS "projectKey"
            FROM claude_session_store_keys
            WHERE session_id = ${key.sessionId}
              AND subpath = ${MAIN_TRANSCRIPT_SUBPATH}
          `;
          const mainRow = mainRows[0];
          if (mainRow === undefined) {
            return null;
          }

          const keyRows = yield* sql<StoredSubkeyRow>`
            SELECT subpath
            FROM claude_session_store_keys
            WHERE session_id = ${key.sessionId}
            ORDER BY subpath ASC
          `;
          const entryRows = yield* sql<StoredBundleEntryRow>`
            SELECT subpath, entry_json AS "entryJson"
            FROM claude_session_store_entries
            WHERE session_id = ${key.sessionId}
            ORDER BY sequence ASC
          `;
          const entriesBySubpath = new Map<string, Array<SessionStoreEntry>>(
            keyRows.map((row) => [row.subpath, []]),
          );

          yield* Effect.forEach(
            entryRows,
            (row) =>
              decodeEntryJson(row.entryJson).pipe(
                Effect.mapError(
                  makeStoreError(
                    "loadSession:decodeEntry",
                    "A stored transcript entry is invalid JSON.",
                  ),
                ),
                Effect.flatMap((entry) =>
                  isSessionStoreEntry(entry)
                    ? Effect.sync(() => {
                        entriesBySubpath.get(row.subpath)?.push(entry);
                      })
                    : Effect.fail(
                        new ClaudeSessionStoreError({
                          operation: "loadSession:validateEntry",
                          detail: "A stored transcript entry does not have a string type field.",
                        }),
                      ),
                ),
              ),
            { discard: true },
          );

          return {
            projectKey: mainRow.projectKey,
            sessionId: key.sessionId,
            entries: entriesBySubpath.get(MAIN_TRANSCRIPT_SUBPATH) ?? [],
            subkeys: keyRows
              .filter((row) => row.subpath !== MAIN_TRANSCRIPT_SUBPATH)
              .map((row) => ({
                subpath: row.subpath,
                entries: entriesBySubpath.get(row.subpath) ?? [],
              })),
          } satisfies ClaudeSessionSnapshot;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isClaudeSessionStoreError(cause)
            ? cause
            : makeStoreError(
                "loadSession:query",
                "Failed to load the complete Claude session.",
              )(cause),
        ),
      );
  });

  const replaceSessionEffect = Effect.fn("ClaudeSessionStore.replaceSession")(function* (
    snapshot: ClaudeSessionSnapshot,
  ) {
    const normalizedMainKey = yield* normalizeKey({
      projectKey: snapshot.projectKey,
      sessionId: snapshot.sessionId,
    });
    const seenSubpaths = new Set<string>();
    const normalizedSubkeys = yield* Effect.forEach(snapshot.subkeys, (subkey) =>
      normalizeKey({
        projectKey: snapshot.projectKey,
        sessionId: snapshot.sessionId,
        subpath: subkey.subpath,
      }).pipe(
        Effect.flatMap((key) => {
          if (seenSubpaths.has(key.subpath)) {
            return Effect.fail(
              new ClaudeSessionStoreError({
                operation: "replaceSession:validateSubkeys",
                detail: `Duplicate subpath '${key.subpath}'.`,
              }),
            );
          }
          seenSubpaths.add(key.subpath);
          return Effect.succeed({ key, entries: subkey.entries });
        }),
      ),
    );
    const allKeys = [{ key: normalizedMainKey, entries: snapshot.entries }, ...normalizedSubkeys];
    const encodedByKey = yield* Effect.forEach(allKeys, ({ key, entries }) =>
      Effect.forEach(entries, (entry) =>
        encodeEntryJson(entry).pipe(
          Effect.mapError(
            makeStoreError(
              "replaceSession:encodeEntry",
              "A transcript entry was not JSON serializable.",
            ),
          ),
          Effect.map((entryJson) => ({
            session_id: key.sessionId,
            subpath: key.subpath,
            entry_uuid: entry.uuid ?? null,
            entry_json: entryJson,
          })),
        ),
      ).pipe(Effect.map((rows) => ({ key, rows }))),
    );

    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const clockRows = yield* sql<StoreClockRow>`
            UPDATE claude_session_store_clock
            SET last_mtime_ms = MAX(last_mtime_ms + 1, ${yield* Clock.currentTimeMillis})
            WHERE singleton = 1
            RETURNING last_mtime_ms AS "mtime"
          `;
          const mtime = clockRows[0]?.mtime;
          if (mtime === undefined) {
            return yield* new ClaudeSessionStoreError({
              operation: "replaceSession:advanceClock",
              detail: "The persistent modification-time clock was not initialized.",
            });
          }

          yield* sql`
            DELETE FROM claude_session_store_keys
            WHERE session_id = ${snapshot.sessionId}
          `;
          yield* sql`
            INSERT INTO claude_session_store_keys ${sql.insert(
              encodedByKey.map(({ key }) => ({
                project_key: snapshot.projectKey,
                session_id: snapshot.sessionId,
                subpath: key.subpath,
                mtime_ms: mtime,
              })),
            )}
          `;
          yield* Effect.forEach(
            encodedByKey,
            ({ rows }) =>
              Effect.forEach(
                chunksOf(rows, MAX_ENTRIES_PER_INSERT),
                (entryChunk) =>
                  sql`
                    INSERT INTO claude_session_store_entries ${sql.insert(entryChunk)}
                    ON CONFLICT DO NOTHING
                  `,
                { discard: true },
              ),
            { discard: true },
          );
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isClaudeSessionStoreError(cause)
            ? cause
            : makeStoreError(
                "replaceSession:persist",
                "Failed to atomically replace the Claude session.",
              )(cause),
        ),
      );
  });

  return {
    append: (key, entries) => runPromise(appendEffect(key, entries)),
    load: (key) => runPromise(loadEffect(key)),
    listSessions: (projectKey) => runPromise(listSessionsEffect(projectKey)),
    listSubkeys: (key) => runPromise(listSubkeysEffect(key)),
    delete: (key) => runPromise(deleteEffect(key)),
    loadSession: (key) => runPromise(loadSessionEffect(key)),
    replaceSession: (snapshot) => runPromise(replaceSessionEffect(snapshot)),
  } satisfies ClaudeSessionStoreShape;
});

export const ClaudeSessionStoreLive = Layer.effect(ClaudeSessionStore, makeClaudeSessionStore);
