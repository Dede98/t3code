import { AgentControlThreadBinding } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/**
 * Historical production storage order for Agent Control thread bindings.
 *
 * This is deliberately a schema encoder rather than a generic JSON
 * canonicalizer: persisted coordinator, orchestration, and projection
 * authority must use the same closed representation without alphabetically
 * reordering its keys.
 */
const encodeStorage = Schema.encodeUnknownSync(Schema.fromJsonString(AgentControlThreadBinding));

export const encodeAgentControlThreadBindingStorage = (
  binding: typeof AgentControlThreadBinding.Encoded,
): string => encodeStorage(binding);

const javascriptWhitespaceSql = [
  9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201,
  8202, 8232, 8233, 8239, 8287, 12288, 65279,
]
  .map((codePoint) => `char(${codePoint})`)
  .join(" || ");

const bindingTextSql = (column: string): string => `
  typeof(${column}) = 'text'
  AND length(${column}) > 0
  AND instr(${column}, char(0)) = 0
  AND t3_fatal_utf8(CAST(${column} AS BLOB)) = 1
`;

/** Closed, typed storage predicate shared by schema 060 and runtime replay authority. */
export const agentControlThreadBindingStorageSql = (column: string): string => `
  CASE
    WHEN typeof(${column}) != 'text'
      OR length(${column}) = 0
      OR instr(${column}, char(0)) != 0
      THEN 0
    WHEN t3_fatal_utf8(CAST(${column} AS BLOB)) != 1 THEN 0
    WHEN json_valid(${column}) != 1 OR json_type(${column}) != 'object' THEN 0
    ELSE
      (SELECT count(*) FROM json_each(${column})) = 5
      AND (SELECT count(DISTINCT key) FROM json_each(${column})) = 5
      AND NOT EXISTS (
        SELECT 1 FROM json_each(${column})
        WHERE key NOT IN ('taskId', 'stageRunId', 'attemptId', 'roleId', 'controlState')
      )
      AND json_type(${column}, '$.taskId') = 'text'
      AND json_type(${column}, '$.stageRunId') = 'text'
      AND json_type(${column}, '$.attemptId') = 'text'
      AND json_type(${column}, '$.roleId') = 'text'
      AND json_type(${column}, '$.controlState') = 'text'
      AND ${bindingTextSql(`json_extract(${column}, '$.taskId')`)}
      AND trim(
        json_extract(${column}, '$.taskId'), ${javascriptWhitespaceSql}
      ) = json_extract(${column}, '$.taskId')
      AND ${bindingTextSql(`json_extract(${column}, '$.stageRunId')`)}
      AND trim(
        json_extract(${column}, '$.stageRunId'), ${javascriptWhitespaceSql}
      ) = json_extract(${column}, '$.stageRunId')
      AND ${bindingTextSql(`json_extract(${column}, '$.attemptId')`)}
      AND trim(
        json_extract(${column}, '$.attemptId'), ${javascriptWhitespaceSql}
      ) = json_extract(${column}, '$.attemptId')
      AND ${bindingTextSql(`json_extract(${column}, '$.roleId')`)}
      AND trim(
        json_extract(${column}, '$.roleId'), ${javascriptWhitespaceSql}
      ) = json_extract(${column}, '$.roleId')
      AND CAST(json_extract(${column}, '$.controlState') AS BLOB) = CAST('controlled' AS BLOB)
  END
`;

/** Key-order- and whitespace-independent equality after both sides pass the closed predicate. */
export const agentControlThreadBindingEqualitySql = (left: string, right: string): string => `
  (${agentControlThreadBindingStorageSql(left)})
  AND (${agentControlThreadBindingStorageSql(right)})
  AND CAST(json_extract(${left}, '$.taskId') AS BLOB) =
    CAST(json_extract(${right}, '$.taskId') AS BLOB)
  AND CAST(json_extract(${left}, '$.stageRunId') AS BLOB) =
    CAST(json_extract(${right}, '$.stageRunId') AS BLOB)
  AND CAST(json_extract(${left}, '$.attemptId') AS BLOB) =
    CAST(json_extract(${right}, '$.attemptId') AS BLOB)
  AND CAST(json_extract(${left}, '$.roleId') AS BLOB) =
    CAST(json_extract(${right}, '$.roleId') AS BLOB)
  AND CAST(json_extract(${left}, '$.controlState') AS BLOB) =
    CAST(json_extract(${right}, '$.controlState') AS BLOB)
`;
