import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_verification_inspection_pages (
    provider_delivery_id TEXT NOT NULL REFERENCES agent_control_verification_check_manifests(provider_delivery_id),
    page_number INTEGER NOT NULL CHECK(page_number BETWEEN 1 AND 256),
    manifest_digest TEXT NOT NULL,
    content TEXT NOT NULL CHECK(length(CAST(content AS BLOB)) <= 25000),
    content_digest TEXT NOT NULL,
    PRIMARY KEY(provider_delivery_id, page_number)
  )`;
  for (const operation of ["UPDATE", "DELETE"]) {
    yield* sql.unsafe(`CREATE TRIGGER agent_control_verification_inspection_pages_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_verification_inspection_pages
      BEGIN SELECT RAISE(ABORT,'verification inspection pages are immutable'); END`).unprepared;
  }
});
