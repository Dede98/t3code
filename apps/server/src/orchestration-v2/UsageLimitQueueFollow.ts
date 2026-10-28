import type { OrchestrationV2Run, OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import {
  latestExecutedRun,
  latestRootProviderFailure,
  runRanAfter,
} from "@t3tools/shared/orchestrationV2ThreadError";
import * as DateTime from "effect/DateTime";

/**
 * A queued run pinned to an account that stopped on its usage limit follows the thread once work
 * continued on another account, if it was queued before that move.
 */
export function movedPastUsageLimit(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "turnItems">,
  run: Pick<OrchestrationV2Run, "providerInstanceId" | "requestedAt">,
): boolean {
  if (run.providerInstanceId === projection.thread.providerInstanceId) return false;
  const limited = latestExecutedRun(
    projection.runs.filter((candidate) => candidate.providerInstanceId === run.providerInstanceId),
  );
  if (
    limited === null ||
    latestRootProviderFailure(limited, projection.turnItems)?.class !== "usage_limit"
  ) {
    return false;
  }
  // Queued runs that moved along are renumbered after runs that started meanwhile, so the lowest
  // ordinal that executed after the stop is the move itself.
  let move: OrchestrationV2Run | null = null;
  for (const candidate of projection.runs) {
    if (
      candidate.providerInstanceId === run.providerInstanceId ||
      candidate.status === "queued" ||
      (candidate.status === "cancelled" && candidate.startedAt === null) ||
      !runRanAfter(candidate, limited)
    ) {
      continue;
    }
    if (move === null || candidate.ordinal < move.ordinal) move = candidate;
  }
  return move !== null && !DateTime.isGreaterThan(run.requestedAt, move.requestedAt);
}
