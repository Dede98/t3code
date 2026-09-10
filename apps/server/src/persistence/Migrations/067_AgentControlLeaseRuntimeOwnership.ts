import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_lease_runtime_owners (
    holder_id TEXT PRIMARY KEY NOT NULL,
    owner_token TEXT NOT NULL,
    hostname TEXT NOT NULL,
    pid INTEGER NOT NULL CHECK (pid > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'closed'))
  )`;
});
