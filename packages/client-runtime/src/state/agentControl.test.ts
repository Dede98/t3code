import { describe, expect, it } from "@effect/vitest";
import {
  AgentControlRunOnceSnapshot,
  AuthStandardClientScopes,
  AuthAdministrativeScopes,
  type AuthSessionState,
  ProviderInstanceId,
  ProviderDriverKind,
  AgentControlRunOnceStageView,
  type AgentControlPreflightRuntimeResult,
  type AgentControlPolicyStateResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/unstable/reactivity";

import {
  agentControlRunStatus,
  agentControlModeChangeBlocker,
  agentControlSnapshotReady,
  agentControlStageLabel,
  agentControlStageHeading,
  agentControlStartBlockers,
  agentControlStartInput,
  agentControlVerificationPassed,
} from "./agentControl.ts";

const decodeSnapshot = Schema.decodeUnknownSync(AgentControlRunOnceSnapshot);
const timestamp = "2026-09-10T10:00:00.000Z";
const stage = Schema.decodeUnknownSync(AgentControlRunOnceStageView)({
  schemaVersion: 1,
  projectId: "project",
  taskId: "task",
  stageRunId: "verification-2",
  attemptId: "attempt-2",
  roleId: "verification",
  stageKind: "verification",
  stageOrdinal: 5,
  attemptOrdinal: 1,
  status: "succeeded",
  taskRevision: 7,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "source",
  createdAt: timestamp,
  updatedAt: timestamp,
  revision: 3,
  sequence: 12,
  displayStage: "verification",
  threadId: "verification-thread-2",
  worktreePath: "/work/task",
  branch: "t3auto/task",
  errorCode: null,
  verification: {
    providerDeliveryId: "delivery-2",
    verdict: "passed",
    errorCode: null,
    evaluatedAt: timestamp,
    checks: [
      {
        id: "test",
        command: "node",
        args: ["--test"],
        cwd: ".",
        required: true,
        status: "passed",
        exitCode: 0,
        output: "1 test passed",
        completedAt: timestamp,
      },
    ],
  },
});

const snapshot = decodeSnapshot({
  projectId: "project",
  blockers: [],
  projectState: {
    schemaVersion: 1,
    projectId: "project",
    mode: "observe",
    pausedFromMode: null,
    revision: 4,
    sequence: 4,
    updatedAt: timestamp,
  },
  nextTaskId: "task",
  tasks: [
    {
      schemaVersion: 1,
      taskId: "task",
      source: {
        projectId: "project",
        repositoryNodeId: "repository",
        issueNodeId: "issue",
        issueNumber: 1,
        issueUrl: "https://example.test/issues/1",
      },
      status: "candidate",
      sourceGate: "eligible",
      stage: "intake",
      sourceUpdatedAt: timestamp,
      githubIntakeSequence: 1,
      title: "Small change",
      contentTrust: "untrusted-external",
      createdAt: timestamp,
      updatedAt: timestamp,
      revision: 1,
      sequence: 1,
    },
  ],
  runs: [
    {
      state: {
        schemaVersion: 1,
        runId: "run",
        projectId: "project",
        status: "completed",
        nextOrdinal: 10,
        lastStep: "completed",
        taskId: "task",
        stageRunId: "verification-2",
        leaseId: null,
        worktreeReservationId: null,
        controlledThreadReservationId: null,
        terminalTaskEventId: "terminal",
        activationProjectRevision: 3,
        resetProjectRevision: 4,
        updatedAt: timestamp,
      },
      errorCode: null,
      task: null,
      stages: [],
    },
  ],
});
const task = snapshot.tasks[0]!;
const run = {
  ...snapshot.runs[0]!,
  task: { ...task, status: "succeeded" as const },
  stages: [stage],
};
const preflight: AgentControlPreflightRuntimeResult = {
  ok: true,
  staticPreflight: { ok: true, roles: [] },
  roles: [],
};
const policy: AgentControlPolicyStateResult = {
  appPolicy: null,
  preflight: { ok: true, roles: [] },
  projectPolicy: {
    projectId: snapshot.projectId,
    revision: 1,
    updatedAt: timestamp,
    policy: {
      verificationChecks: [
        {
          id: "test",
          command: "node",
          args: ["--test"],
          cwd: ".",
          required: true,
          timeoutMs: 1000,
          allowTemporaryFiles: false,
          resultFormat: "exit-code",
        },
      ],
    },
  },
};
const adminSession: AuthSessionState = {
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["dpop-access-token"],
    sessionCookieName: "t3_session",
  },
  scopes: AuthAdministrativeScopes,
};
const start = {
  policy,
  snapshot,
  preflight,
  selectedTaskId: task.taskId,
  connected: true,
  pending: false,
  modeChangeBlocker: agentControlModeChangeBlocker(AsyncResult.success(adminSession)),
};

describe("Run Once client state", () => {
  it("blocks start and intake changes for a standard session but permits an admin session", () => {
    const standardSession = { ...adminSession, scopes: AuthStandardClientScopes };
    const blocked = agentControlModeChangeBlocker(AsyncResult.success(standardSession));
    expect(blocked).toContain("access:write");
    expect(blocked).toContain("start runs or change task intake");
    expect(agentControlStartBlockers({ ...start, modeChangeBlocker: blocked })).toEqual([blocked]);
    const allowed = agentControlModeChangeBlocker(AsyncResult.success(adminSession));
    expect(allowed).toBeNull();
    expect(agentControlStartBlockers({ ...start, modeChangeBlocker: allowed })).toEqual([]);
    // Returning to a standard environment must not inherit the other environment's admin grant.
    expect(agentControlModeChangeBlocker(AsyncResult.success(standardSession))).toBe(blocked);
  });

  it("keeps actions blocked while permissions are unknown, refreshing, failed, or unauthenticated", () => {
    const { scopes: _scopes, ...withoutScopes } = adminSession;
    const sessions = [
      AsyncResult.initial<AuthSessionState>(),
      AsyncResult.waiting(AsyncResult.success(adminSession)),
      AsyncResult.fail(new Error("Session unavailable")),
      AsyncResult.success({ ...adminSession, authenticated: false }),
      AsyncResult.success(withoutScopes),
    ];
    for (const session of sessions) {
      const blocker = agentControlModeChangeBlocker(session);
      expect(blocker).not.toBeNull();
      expect(agentControlStartBlockers({ ...start, modeChangeBlocker: blocker })).toEqual([
        blocker,
      ]);
    }
  });

  it("accepts a received snapshot while its live stream is waiting for the next event", () => {
    const streaming = AsyncResult.waiting(AsyncResult.success(snapshot));
    expect(streaming.waiting).toBe(true);
    expect(agentControlSnapshotReady(streaming, true)).toBe(true);
    expect(agentControlSnapshotReady(streaming, false)).toBe(false);
    expect(agentControlSnapshotReady(AsyncResult.initial(), true)).toBe(false);
  });
  it("requires an available policy with at least one required verification check", () => {
    expect(agentControlStartBlockers({ ...start, policy: null })).toContain(
      "Load the project's verification configuration before starting.",
    );
    for (const verificationChecks of [
      [],
      policy.projectPolicy!.policy.verificationChecks!.map((check) => ({
        ...check,
        required: false,
      })),
    ]) {
      expect(
        agentControlStartBlockers({
          ...start,
          policy: {
            ...policy,
            projectPolicy: { ...policy.projectPolicy!, policy: { verificationChecks } },
          },
        }),
      ).toContain(
        "Configure at least one required verification check for this project before starting.",
      );
    }
  });

  it("restores pre-stage failures from durable server diagnostics", () => {
    const blocked = { ...snapshot.runs[0]!, errorCode: "source-watermark-stale" };
    expect(agentControlRunStatus(blocked).label).toContain("Blocked · source-watermark-stale");
    expect(
      agentControlStartBlockers({
        ...start,
        snapshot: { ...snapshot, blockers: ["source-watermark-stale"] },
      }).join(" "),
    ).toContain("review the selected task's eligibility");
  });

  it("only admits the server-selected eligible task with a ready connection and preflight", () => {
    expect(agentControlStartBlockers(start)).toEqual([]);
    expect(agentControlStartBlockers({ ...start, connected: false })).toContain(
      "Reconnect to this environment before starting a task.",
    );
    expect(agentControlStartBlockers({ ...start, pending: true }).length).toBeGreaterThan(0);
    expect(agentControlStartBlockers({ ...start, preflight: null }).length).toBeGreaterThan(0);
    expect(agentControlStartBlockers({ ...start, snapshot: null }).length).toBeGreaterThan(0);
    expect(
      agentControlStartBlockers({ ...start, snapshot: { ...snapshot, nextTaskId: null } }),
    ).toContain(
      "Run Once currently accepts the next eligible task in issue-number order. Select that task.",
    );
    for (const status of [
      "succeeded",
      "failed",
      "running",
      "waiting",
      "needs-attention",
    ] as const) {
      expect(
        agentControlStartBlockers({
          ...start,
          snapshot: {
            ...snapshot,
            tasks: [{ ...task, status }],
          },
        }).some((message) => message.includes("cannot start")),
      ).toBe(true);
    }
  });

  it("blocks a new run until durable resource cleanup finishes", () => {
    expect(
      agentControlStartBlockers({
        ...start,
        snapshot: { ...snapshot, runs: [{ ...run, state: { ...run.state, status: "active" } }] },
      }),
    ).toContain("The previous run has not finished releasing its resources.");
  });

  it("retains command and task identity across repeated requests and snapshot reconstruction", () => {
    const restored = decodeSnapshot(JSON.parse(JSON.stringify(snapshot)));
    expect(agentControlStartInput(restored, task.taskId)).toEqual(
      agentControlStartInput(snapshot, task.taskId),
    );
    expect(agentControlStartInput(restored, task.taskId)).toMatchObject({
      projectId: "project",
      expectedRevision: 4,
      runOnceTaskId: "task",
      mode: "run-once",
    });
    expect(
      agentControlStartInput(
        { ...restored, projectState: { ...restored.projectState, revision: 5 } },
        task.taskId,
      ).commandId,
    ).not.toEqual(agentControlStartInput(snapshot, task.taskId).commandId);
  });

  it("explains provider and model blockers in the owning environment", () => {
    const blocked: AgentControlPreflightRuntimeResult = {
      ...preflight,
      ok: false,
      roles: [
        {
          role: "planner",
          accessMode: "restricted",
          strict: true,
          selectedCandidateIndex: null,
          errorCode: "role-runtime-unresolved",
          candidates: [
            {
              candidateIndex: 0,
              source: "role-route",
              providerInstanceId: ProviderInstanceId.make("codex"),
              model: "configured-model",
              driverKind: ProviderDriverKind.make("codex"),
              providerStatus: "error",
              authStatus: "unauthenticated",
              checkedAt: timestamp,
              runtimeReady: false,
              errorCode: "provider-unauthenticated",
            },
          ],
        },
      ],
    };
    expect(agentControlStartBlockers({ ...start, preflight: blocked }).join(" ")).toContain(
      "planner: codex / configured-model: provider-unauthenticated",
    );
  });

  it("reconstructs Repair from durable role data even though its implementation stage kind is unchanged", () => {
    const repair = {
      ...stage,
      displayStage: "repair" as const,
      stageKind: "implementation" as const,
      status: "running" as const,
      verification: null,
    };
    expect(agentControlStageLabel(repair)).toBe("Repair");
    expect(
      agentControlRunStatus({
        ...run,
        task: { ...task, status: "running" },
        state: { ...run.state, status: "active" },
        stages: [repair],
      }),
    ).toEqual({ label: "Running · Repair", tone: "running" });
  });

  it("numbers verification stages separately even when both have attempt ordinal one", () => {
    const first = { ...stage, stageOrdinal: 3 };
    const repair = { ...stage, stageOrdinal: 4, displayStage: "repair" as const };
    const stages = [stage, repair, first];
    expect(agentControlStageHeading(first, stages)).toBe("Verification 1");
    expect(agentControlStageHeading(stage, stages)).toBe("Verification 2 (after repair)");
  });

  it("keeps failed verification separate from the final successful verification after reload", () => {
    const failed = {
      ...stage,
      stageOrdinal: 3,
      status: "failed" as const,
      verification: {
        ...stage.verification!,
        providerDeliveryId: "delivery-1",
        verdict: "failed" as const,
        checks: [{ ...stage.verification!.checks[0]!, status: "failed" as const, exitCode: 1 }],
      },
    };
    const restored = decodeSnapshot(
      JSON.parse(
        JSON.stringify({
          ...snapshot,
          runs: [{ ...run, stages: [stage, failed] }],
        }),
      ),
    );
    expect(agentControlRunStatus(restored.runs[0]!)).toEqual({
      label: "Succeeded · verified",
      tone: "success",
    });
    expect(agentControlVerificationPassed(failed)).toBe(false);
    expect(agentControlVerificationPassed(stage)).toBe(true);
    expect(restored.runs[0]!.stages[1]!.verification!.checks[0]!.exitCode).toBe(1);
    const newerFailure = { ...failed, stageOrdinal: 6 };
    expect(agentControlRunStatus({ ...run, stages: [stage, newerFailure] }).tone).toBe("warning");
  });

  it.each(["missing", "unavailable", "stale", "running", "failed"] as const)(
    "never presents a required %s check as verified success",
    (status) => {
      const incomplete = {
        ...stage,
        verification: {
          ...stage.verification!,
          checks: [{ ...stage.verification!.checks[0]!, status }],
        },
      };
      expect(agentControlRunStatus({ ...run, stages: [incomplete] }).tone).toBe("warning");
    },
  );

  it("does not substitute an old successful attempt for missing final evidence", () => {
    for (const verification of [
      null,
      { ...stage.verification!, checks: [] },
      { ...stage.verification!, evaluatedAt: null },
    ]) {
      expect(
        agentControlRunStatus({
          ...run,
          stages: [stage, { ...stage, stageOrdinal: 6, verification }],
        }).tone,
      ).toBe("warning");
    }
  });
});
