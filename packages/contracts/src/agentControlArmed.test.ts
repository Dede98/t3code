import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlArmedDispatch,
  AgentControlArmedDispatchState,
  AgentControlArmedNoCandidateDecision,
} from "./agentControlArmed.ts";
import { AgentControlRunOnceActivation } from "./agentControlRunOnce.ts";

const decodeArmedDispatch = Schema.decodeUnknownEffect(AgentControlArmedDispatch);
const decodeArmedDispatchState = Schema.decodeUnknownEffect(AgentControlArmedDispatchState);
const decodeArmedNoCandidateDecision = Schema.decodeUnknownEffect(
  AgentControlArmedNoCandidateDecision,
);
const decodeRunOnceActivation = Schema.decodeUnknownEffect(AgentControlRunOnceActivation);

const epoch = {
  githubIntakeSequence: 7,
  githubEventId: "github-event",
  githubEventSequence: 7,
  githubEventStreamVersion: 3,
  sourceFingerprint: "a".repeat(64),
  reconcileRevision: 4,
  taskFrontierSequence: 11,
  taskFrontierRevision: 5,
  taskFrontierCount: 2,
  taskFrontierFingerprint: "b".repeat(64),
} as const;

it.effect("decodes closed Armed authority documents and rejects excess keys", () =>
  Effect.gen(function* () {
    const dispatch = {
      schemaVersion: 1,
      dispatchId: "armed-dispatch-id",
      claimId: "armed-claim-id",
      evidenceId: "armed-evidence-id",
      receiptId: "armed-receipt-id",
      markerId: "armed-marker-id",
      commandId: "armed-command-id",
      projectId: "armed-project",
      selectedTaskId: "armed-task",
      projectRevision: 2,
      projectEventSequence: 8,
      epoch,
      ownerId: "armed-owner",
      fenceToken: 1,
      claimedAt: "2026-09-02T08:00:00.000Z",
      expiresAt: "2026-09-02T08:01:00.000Z",
    } as const;
    const decoded = yield* decodeArmedDispatch(dispatch, {
      onExcessProperty: "error",
    });
    assert.equal(decoded.selectedTaskId, "armed-task");
    const excess = yield* Effect.result(
      decodeArmedDispatch(
        { ...dispatch, authority: "system" },
        {
          onExcessProperty: "error",
        },
      ),
    );
    assert.equal(excess._tag, "Failure");
  }),
);

it.effect("keeps dispatch activation triples and no-candidate epochs closed", () =>
  Effect.gen(function* () {
    const state = yield* decodeArmedDispatchState({
      schemaVersion: 1,
      dispatchId: "armed-dispatch-id",
      projectId: "armed-project",
      status: "claimed",
      ownerId: "armed-owner",
      fenceToken: 1,
      expiresAt: "2026-09-02T08:01:00.000Z",
      activationEventId: null,
      activationEventSequence: null,
      activationEventStreamVersion: null,
      updatedAt: "2026-09-02T08:00:00.000Z",
    });
    assert.equal(state.status, "claimed");
    for (const invalid of [
      {
        ...state,
        activationEventId: "unexpected-event",
        activationEventSequence: 1,
        activationEventStreamVersion: 1,
      },
      { ...state, status: "activated" },
      { ...state, status: "completed" },
    ]) {
      assert.equal((yield* Effect.result(decodeArmedDispatchState(invalid)))._tag, "Failure");
    }
    const noCandidate = yield* decodeArmedNoCandidateDecision({
      schemaVersion: 1,
      evidenceId: "no-candidate-evidence",
      receiptId: "no-candidate-receipt",
      markerId: "no-candidate-marker",
      projectId: "armed-project",
      projectRevision: 2,
      projectEventSequence: 8,
      epoch,
      decidedAt: "2026-09-02T08:00:00.000Z",
    });
    assert.equal(noCandidate.epoch.taskFrontierSequence, 11);
  }),
);

it.effect("binds Run-Once origin to its Armed authority identities", () =>
  Effect.gen(function* () {
    const activation = {
      schemaVersion: 1,
      runId: "run-once-origin",
      projectId: "armed-project",
      activationEventId: "activation-event",
      activationEventSequence: 10,
      activationEventStreamVersion: 3,
      activationCommandId: "activation-command",
      githubIntakeSequence: 7,
      githubEventId: "github-event",
      githubEventSequence: 7,
      githubEventStreamVersion: 3,
      reconcileRevision: 4,
      sourceFingerprint: "a".repeat(64),
      activatedAt: "2026-09-02T08:00:00.000Z",
    } as const;
    const observe = yield* decodeRunOnceActivation({
      ...activation,
      originMode: "observe",
      armedDispatchId: null,
      armedClaimId: null,
      armedMarkerId: null,
    });
    assert.equal(observe.originMode, "observe");
    const armed = yield* decodeRunOnceActivation({
      ...activation,
      originMode: "armed",
      armedDispatchId: "armed-dispatch",
      armedClaimId: "armed-claim",
      armedMarkerId: "armed-marker",
    });
    assert.equal(armed.originMode, "armed");
    for (const invalid of [
      { ...observe, armedDispatchId: "unexpected" },
      { ...armed, armedClaimId: null },
      { ...armed, armedMarkerId: null },
    ]) {
      assert.equal((yield* Effect.result(decodeRunOnceActivation(invalid)))._tag, "Failure");
    }
  }),
);
