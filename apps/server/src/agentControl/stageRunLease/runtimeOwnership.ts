import { AgentControlStageRunLeaseHolderId } from "@t3tools/contracts";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const RuntimeOwner = Schema.Struct({
  holderId: AgentControlStageRunLeaseHolderId,
  ownerToken: Schema.NonEmptyString,
  hostname: Schema.NonEmptyString,
  pid: Schema.Int.check(Schema.isGreaterThan(0)),
  status: Schema.Literals(["active", "closed"]),
  hasLease: Schema.Literals([0, 1]),
});
const decodeOwners = Schema.decodeUnknownEffect(Schema.Array(RuntimeOwner));

/** Only ESRCH proves death. Permission errors and reused PIDs remain owned. */
const isKnownDeadProcess = (pid: number) => {
  try {
    process.kill(pid, 0);
    return false;
  } catch (cause) {
    return cause instanceof Error && "code" in cause && cause.code === "ESRCH";
  }
};

/**
 * A holder identifies a durable lease lineage. Its process incarnation may be
 * replaced only after a scoped release or proof that its same-host PID died.
 * Concurrent live engines and unregistered legacy holders remain foreign.
 */
export const acquireStageRunLeaseRuntimeOwnership = Effect.fn(
  "acquireStageRunLeaseRuntimeOwnership",
)(function* (input: {
  readonly holderId: AgentControlStageRunLeaseHolderId;
  readonly ownerToken: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const hostname = NodeOS.hostname();
  return yield* Effect.acquireRelease(
    sql.withTransaction(
      Effect.gen(function* () {
        // Obtain the SQLite writer lock before observing predecessor ownership.
        yield* sql`UPDATE agent_control_lease_runtime_owners SET status = status WHERE 0`;
        const owners = yield* decodeOwners(
          yield* sql`
          SELECT owner.holder_id AS "holderId", owner.owner_token AS "ownerToken",
            owner.hostname, owner.pid, owner.status,
            EXISTS (SELECT 1 FROM agent_control_stage_run_lease_states lease
              WHERE lease.holder_id = owner.holder_id) AS "hasLease"
          FROM agent_control_lease_runtime_owners owner
        `,
        );
        const recoverable = owners.filter(
          (owner) =>
            owner.hostname === hostname &&
            (owner.status === "closed" || isKnownDeadProcess(owner.pid)),
        );
        // Idle competitors have no durable lineage to recover. A released lease
        // still matters: its holder binds the next phase's worktree evidence.
        const persisted = recoverable.filter((owner) => owner.hasLease === 1);
        const candidates = persisted.length > 0 ? persisted : recoverable;
        // Multiple durable lineages cannot safely be guessed into one runtime.
        const previous = candidates.length === 1 ? candidates[0] : undefined;
        for (const owner of recoverable) {
          if (owner.hasLease === 0 && owner !== previous) {
            yield* sql`
              DELETE FROM agent_control_lease_runtime_owners
              WHERE holder_id = ${owner.holderId} AND owner_token = ${owner.ownerToken}
                AND status = ${owner.status} AND hostname = ${owner.hostname} AND pid = ${owner.pid}
            `;
          }
        }
        if (previous !== undefined) {
          const claimed = yield* sql`
            UPDATE agent_control_lease_runtime_owners
            SET owner_token = ${input.ownerToken}, hostname = ${hostname},
                pid = ${process.pid}, status = 'active'
            WHERE holder_id = ${previous.holderId} AND owner_token = ${previous.ownerToken}
              AND status = ${previous.status} AND hostname = ${previous.hostname}
              AND pid = ${previous.pid}
            RETURNING holder_id
          `;
          if (claimed.length === 1) return previous.holderId;
        }
        yield* sql`
          INSERT INTO agent_control_lease_runtime_owners
            (holder_id, owner_token, hostname, pid, status)
          VALUES (${input.holderId}, ${input.ownerToken}, ${hostname}, ${process.pid}, 'active')
        `;
        return input.holderId;
      }),
    ),
    (holderId) =>
      sql`
        UPDATE agent_control_lease_runtime_owners SET status = 'closed'
        WHERE holder_id = ${holderId} AND owner_token = ${input.ownerToken}
          AND status = 'active'
      `.pipe(Effect.orDie, Effect.asVoid),
  );
});
