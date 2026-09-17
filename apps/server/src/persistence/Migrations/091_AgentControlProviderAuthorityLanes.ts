import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Shared resource admission owns capacity. The older per-stage authority keeps
 * its immutable evidence/fences, now independently for each admitted task. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const schema = yield* sql<{
    type: string;
    name: string;
    sql: string;
  }>`SELECT type,name,sql FROM main.sqlite_schema WHERE sql IS NOT NULL`;
  const table = schema.find(
    (entry) => entry.type === "table" && entry.name === "agent_control_provider_capacity_current",
  )!;
  const quote = (name: string) => '"' + name.replaceAll('"', '""') + '"';
  for (const entry of schema.filter((item) => item.type === "trigger" || item.type === "view"))
    yield* sql.unsafe(`DROP ${entry.type.toUpperCase()} main.${quote(entry.name)}`).unprepared;
  const ddl = table.sql
    .replace(
      "agent_control_provider_capacity_current",
      "agent_control_provider_capacity_current_091",
    )
    .replace(
      "provider_instance_id TEXT PRIMARY KEY",
      "capacity_lane TEXT NOT NULL,\n      provider_instance_id TEXT NOT NULL",
    )
    .replace(
      "FOREIGN KEY(active_admission_id)",
      "PRIMARY KEY(provider_instance_id,capacity_lane),\n      FOREIGN KEY(active_admission_id)",
    );
  yield* sql.unsafe(ddl).unprepared;
  yield* sql`INSERT INTO agent_control_provider_capacity_current_091
    SELECT COALESCE(old.active_admission_id,(SELECT history.admission_id FROM agent_control_provider_claim_history history
      WHERE history.provider_instance_id=old.provider_instance_id ORDER BY history.provider_fence_token DESC LIMIT 1),'legacy-unused'),old.*
    FROM agent_control_provider_capacity_current old`;
  yield* sql`DROP TABLE agent_control_provider_capacity_current`;
  yield* sql`ALTER TABLE agent_control_provider_capacity_current_091 RENAME TO agent_control_provider_capacity_current`;
  for (const entry of schema) {
    if (entry.type !== "trigger" && entry.type !== "view") continue;
    let source = entry.sql;
    if (entry.name === "agent_control_provider_admission_current_validate_update")
      source = source.replaceAll(
        "NEW.provider_fence_token=OLD.provider_fence_token+1",
        "NEW.provider_fence_token>OLD.provider_fence_token",
      );
    if (entry.name === "agent_control_provider_capacity_current_validate_update")
      source = source
        .replaceAll(
          "NEW.last_fence_token=OLD.last_fence_token+1",
          "NEW.last_fence_token>OLD.last_fence_token",
        )
        .replace(
          "NEW.provider_instance_id=OLD.provider_instance_id",
          "NEW.provider_instance_id=OLD.provider_instance_id AND NEW.capacity_lane=OLD.capacity_lane",
        );
    if (entry.name.startsWith("agent_control_provider_capacity_current_validate_"))
      source = source
        .replace(
          "AND (NEW.active_admission_id IS NULL OR EXISTS (",
          "AND (NEW.active_admission_id IS NULL OR (NEW.capacity_lane=NEW.active_admission_id AND EXISTS (",
        )
        .replace(
          ")\n      ) BEGIN SELECT RAISE(ABORT, 'provider capacity projection is inconsistent')",
          "))\n      ) BEGIN SELECT RAISE(ABORT, 'provider capacity projection is inconsistent')",
        );
    yield* sql.unsafe(source).unprepared;
  }
});

export const PROVIDER_AUTHORITY_LANE_DDL_FINGERPRINTS: Readonly<Record<string, string>> = {
  agent_control_provider_capacity_current:
    "e4fb7c1cd64d3aa410cadaa1a91e281f234bd011eb4f05396c3d4f9f458ff77c",
  agent_control_provider_capacity_current_validate_insert:
    "7fe331173f27aca868ccc0d7d8041987c8468149ace725771bd175fb2bfee01a",
  agent_control_provider_capacity_current_validate_update:
    "1624da3929dec8091fd06e92fb36a9c273ba142aef3d40a5398c799ef7a6a64b",
  agent_control_provider_admission_current_validate_update:
    "835f7113d8c88d148ab73f76af3bc3033cf60ef52130bc3ec809fb42341e69b1",
};
