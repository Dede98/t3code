import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlStageRunLeaseState,
  AgentControlStageRunLeaseView,
} from "./agentControlStageRunLease.ts";

const state = {
  schemaVersion: 1,
  leaseId: "stage-run-lease-test",
  projectId: "project-test",
  taskId: "task-test",
  stageRunId: "stage-run-test",
  attemptId: "attempt-test",
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  holderId: "holder-test",
  fenceToken: 1,
  status: "reserved",
  acquiredAt: "2026-07-24T10:00:00.000Z",
  renewedAt: "2026-07-24T10:00:00.000Z",
  expiresAt: "2026-07-24T10:01:00.000Z",
  releasedAt: null,
  revision: 1,
  sequence: 1,
} as const;
const decodeState = Schema.decodeUnknownEffect(AgentControlStageRunLeaseState);
const encodeView = Schema.encodeUnknownEffect(AgentControlStageRunLeaseView);

it.effect("decodes persistent lease state but keeps holder identity out of wire views", () =>
  Effect.gen(function* () {
    const decoded = yield* decodeState(state);
    assert.equal(decoded.holderId, "holder-test");
    const encodedView = yield* encodeView({
      leaseId: decoded.leaseId,
      projectId: decoded.projectId,
      taskId: decoded.taskId,
      stageRunId: decoded.stageRunId,
      attemptId: decoded.attemptId,
      fenceToken: decoded.fenceToken,
      status: decoded.status,
      ownership: "current-runtime",
      health: "healthy",
      acquiredAt: decoded.acquiredAt,
      renewedAt: decoded.renewedAt,
      expiresAt: decoded.expiresAt,
      releasedAt: decoded.releasedAt,
      revision: decoded.revision,
    });
    assert.notProperty(encodedView, "holderId");
  }),
);

it.effect("rejects non-positive fence tokens and inconsistent release shape", () =>
  Effect.gen(function* () {
    assert.equal(
      (yield* Effect.result(
        decodeState({
          ...state,
          fenceToken: 0,
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        decodeState({
          ...state,
          status: "released",
          releasedAt: null,
        }),
      ))._tag,
      "Failure",
    );
  }),
);
