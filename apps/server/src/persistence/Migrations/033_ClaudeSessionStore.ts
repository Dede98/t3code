import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS claude_session_store_clock (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      last_mtime_ms INTEGER NOT NULL CHECK (last_mtime_ms >= 0)
    )
  `;

  yield* sql`
    INSERT INTO claude_session_store_clock (singleton, last_mtime_ms)
    VALUES (1, 0)
    ON CONFLICT (singleton) DO NOTHING
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS claude_session_store_keys (
      project_key TEXT NOT NULL,
      session_id TEXT NOT NULL,
      subpath TEXT NOT NULL,
      mtime_ms INTEGER NOT NULL CHECK (mtime_ms >= 0),
      PRIMARY KEY (session_id, subpath)
    )
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS claude_session_store_entries (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      subpath TEXT NOT NULL,
      entry_uuid TEXT,
      entry_json TEXT NOT NULL,
      FOREIGN KEY (session_id, subpath)
        REFERENCES claude_session_store_keys (session_id, subpath)
        ON DELETE CASCADE
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_claude_session_store_keys_project_mtime
    ON claude_session_store_keys (project_key, subpath, mtime_ms DESC)
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_claude_session_store_entries_key_sequence
    ON claude_session_store_entries (session_id, subpath, sequence)
  `;

  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_claude_session_store_entries_uuid
    ON claude_session_store_entries (session_id, subpath, entry_uuid)
    WHERE entry_uuid IS NOT NULL
  `;
});
