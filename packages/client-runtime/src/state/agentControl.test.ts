import { describe, expect, it } from "@effect/vitest";
import {
  type AgentControlEpicPreview,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicHandoffPreview,
  type AgentControlEpicHandoff,
  ProjectId,
  AgentControlProjectRevisionConflictError,
  AgentControlCommandPreviouslyRejectedError,
  AgentControlRunOnceSnapshot,
  AgentControlTaskId,
  type AgentControlRunOnceView,
  AuthStandardClientScopes,
  AuthAdministrativeScopes,
  type AuthSessionState,
  ProviderInstanceId,
  ProviderDriverKind,
  AgentControlRunOnceStageView,
  AgentControlRunOnceId,
  type AgentControlPreflightRuntimeResult,
  type AgentControlPolicyStateResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/unstable/reactivity";

import {
  agentControlEpicQueueChangeBlockers,
  agentControlEpicQueueLeaveBlockers,
  agentControlEpicQueueApproveBlockers,
  agentControlEpicQueueChangeInput,
  agentControlEpicQueueMoveInput,
  agentControlEpicQueueView,
  agentControlEpicStartBlockers,
  agentControlEpicStartInput,
  agentControlEpicExecutionOptions,
  agentControlEpicMemberProgress,
  agentControlEpicControlInput,
  agentControlEpicControlAllowed,
  agentControlEpicStatus,
  agentControlEpicHandoffPermissionBlocker,
  agentControlEpicHandoffBlockers,
  agentControlEpicPublishHandoffInput,
  agentControlEndPausedInput,
  agentControlCommandErrorMessage,
  agentControlErrorMessage,
  agentControlSnapshotFresh,
  agentControlArmedStatus,
  agentControlArmBlockers,
  agentControlArmInput,
  agentControlDisarmInput,
  agentControlRunStatus,
  agentControlEndBlockedRunInput,
  agentControlCanEndBlockedRun,
  agentControlModeChangeBlocker,
  agentControlSnapshotReady,
  agentControlStageLabel,
  agentControlStageHeading,
  agentControlStartBlockers,
  agentControlStartInput,
  agentControlVerificationPassed,
  resourceAdmissionWaitMessage,
} from "./agentControl.ts";

const decodeSnapshot = Schema.decodeUnknownSync(AgentControlRunOnceSnapshot);
const timestamp = "2026-09-10T10:00:00.000Z";

describe("resource admission wait presentation", () => {
  it("names every wait cause as a server-host observation", () => {
    expect(
      resourceAdmissionWaitMessage({
        reason: "cpu-pressure",
        hostId: "builder-01",
        detail: "Observed CPU utilization is 91%; this is a scheduling threshold, not a quota.",
      }),
    ).toBe(
      "Waiting for CPU pressure to fall on builder-01. Observed CPU utilization is 91%; this is a scheduling threshold, not a quota.",
    );
    expect(resourceAdmissionWaitMessage({ reason: "telemetry-unavailable" })).toBe(
      "Waiting for host resource telemetry on the server host.",
    );
  });
});
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
  it("ends a blocked activation with fresh human mode authority, preserving command identity on replay", () => {
    const blocked = {
      ...snapshot,
      projectState: { ...snapshot.projectState, mode: "run-once" as const },
      runs: [
        {
          ...run,
          state: { ...run.state, status: "active" as const, lastStep: "lease-reserved" as const },
          errorCode: "downstream-rejected: default-remote-ref-unavailable",
        },
      ],
    };
    const input = { snapshot: blocked, connected: true, pending: false, modeChangeBlocker: null };
    const command = agentControlEndBlockedRunInput(input);
    expect(command).toMatchObject({
      projectId: snapshot.projectId,
      expectedRevision: 4,
      mode: "observe",
    });
    expect(agentControlEndBlockedRunInput(input)).toEqual(command);
    expect(
      agentControlEndBlockedRunInput({
        ...input,
        snapshot: { ...blocked, projectState: { ...blocked.projectState, revision: 6 } },
      })?.commandId,
    ).not.toBe(command?.commandId);
    for (const unavailable of [
      { ...input, connected: false },
      { ...input, pending: true },
      { ...input, snapshot: null },
      { ...input, snapshot },
      { ...input, snapshot: { ...blocked, runs: [{ ...blocked.runs[0]!, errorCode: null }] } },
      { ...input, snapshot: { ...blocked, runs: [{ ...blocked.runs[0]!, state: run.state }] } },
    ])
      expect(agentControlEndBlockedRunInput(unavailable)).toBeNull();
    for (const session of [
      AsyncResult.success({ ...adminSession, scopes: AuthStandardClientScopes }),
      AsyncResult.initial<AuthSessionState>(),
      AsyncResult.waiting(AsyncResult.success(adminSession)),
      AsyncResult.fail(new Error("Session unavailable")),
      AsyncResult.success({ ...adminSession, authenticated: false }),
    ]) {
      expect(
        agentControlEndBlockedRunInput({
          ...input,
          modeChangeBlocker: agentControlModeChangeBlocker(session),
        }),
      ).toBeNull();
    }
    expect(
      agentControlEndBlockedRunInput({
        ...input,
        modeChangeBlocker: agentControlModeChangeBlocker(AsyncResult.success(adminSession)),
      }),
    ).toEqual(command);
  });

  it("only offers ending for persisted pre-turn default-reference or reservation rejections", () => {
    const blocked = {
      ...run,
      state: { ...run.state, status: "active" as const, lastStep: "lease-reserved" as const },
    };
    const endInput = (value: AgentControlRunOnceView) =>
      agentControlEndBlockedRunInput({
        snapshot: {
          ...snapshot,
          projectState: { ...snapshot.projectState, mode: "run-once" },
          runs: [value],
        },
        connected: true,
        pending: false,
        modeChangeBlocker: null,
      });
    const supported = {
      ...blocked,
      errorCode: "downstream-rejected: default-remote-ref-unavailable",
    };
    expect(agentControlCanEndBlockedRun(supported)).toBe(true);
    expect(endInput(supported)?.mode).toBe("observe");
    const reservationConflict = {
      ...supported,
      errorCode: "downstream-rejected: reservation-conflict",
    };
    expect(agentControlCanEndBlockedRun(reservationConflict)).toBe(true);
    expect(endInput(reservationConflict)?.mode).toBe("observe");
    for (const lastStep of [
      "task-selected",
      "stage-prepared",
      "worktree-ready",
      "thread-activated",
    ] as const) {
      const unsupported = { ...supported, state: { ...supported.state, lastStep } };
      expect(agentControlCanEndBlockedRun(unsupported)).toBe(false);
      expect(endInput(unsupported)).toBeNull();
    }
    for (const errorCode of [
      null,
      "persistence",
      "downstream-rejected: internal-persistence-error",
      "downstream-rejected: stage-not-prepared",
      "downstream-rejected: lease-recovery-required",
      "downstream-rejected: controlled-thread-unavailable",
      "downstream-rejected: repository-unavailable",
      "downstream-rejected: repository-lock-unavailable",
      "downstream-rejected: repository-unavailable-extra",
    ]) {
      const unsupported = { ...blocked, errorCode };
      expect(agentControlCanEndBlockedRun(unsupported)).toBe(false);
      expect(endInput(unsupported)).toBeNull();
    }
  });

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

  it("shows the required check capability blocker for Run Once and Armed", () => {
    const unavailable: AgentControlPreflightRuntimeResult = {
      ...preflight,
      ok: false,
      roles: [
        {
          role: "verifier",
          accessMode: "restricted",
          strict: true,
          selectedCandidateIndex: null,
          errorCode: "role-runtime-unresolved",
          candidates: [
            {
              candidateIndex: 0,
              source: "role-route",
              providerInstanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5.4",
              driverKind: ProviderDriverKind.make("codex"),
              providerStatus: "ready",
              authStatus: "authenticated",
              checkedAt: timestamp,
              runtimeReady: false,
              errorCode: "verification-checks-unavailable",
              verificationCheckError: {
                checkId: "http-tests",
                message: "Assigned loopback networking is unavailable on this Linux environment.",
              },
            },
          ],
        },
      ],
    };
    for (const blockers of [
      agentControlStartBlockers({ ...start, preflight: unavailable }),
      agentControlArmBlockers({ ...start, preflight: unavailable }),
    ]) {
      expect(blockers.join(" ")).toContain(
        "Check http-tests: Assigned loopback networking is unavailable on this Linux environment.",
      );
    }
  });

  it("restores pre-stage failures from durable server diagnostics", () => {
    const blocked = {
      ...snapshot.runs[0]!,
      state: { ...snapshot.runs[0]!.state, status: "active" as const },
      errorCode: "source-watermark-stale",
    };
    expect(agentControlRunStatus(blocked).label).toContain("Blocked · source-watermark-stale");
    expect(
      agentControlStartBlockers({
        ...start,
        snapshot: { ...snapshot, blockers: ["source-watermark-stale"] },
      }).join(" "),
    ).toContain("review the selected task's eligibility");
  });

  it("explains reservation conflicts in active and historical run results", () => {
    for (const errorCode of ["reservation-conflict", "downstream-rejected: reservation-conflict"]) {
      const message = agentControlErrorMessage(errorCode);
      expect(message).toContain("Another attempt still owns this issue or its worktree target");
      expect(message).toContain("will not retry automatically");
      for (const status of ["active", "completed"] as const) {
        const run = {
          ...snapshot.runs[0]!,
          state: { ...snapshot.runs[0]!.state, status },
          errorCode,
        };
        expect(agentControlRunStatus(run).label).toContain(message);
        expect(agentControlRunStatus(run).tone).toBe("warning");
      }
    }
  });

  it("keeps an ended run's rejection visible without reporting success or blocking a fresh task", () => {
    const ended = {
      ...snapshot.runs[0]!,
      task,
      errorCode: "default-remote-ref-unavailable",
    };
    const status = agentControlRunStatus(ended);
    expect(status.label).toContain("Ended · blocked · default-remote-ref-unavailable");
    expect(status.tone).toBe("warning");
    const endedSnapshot = { ...snapshot, runs: [ended] };
    expect(agentControlStartBlockers({ ...start, snapshot: endedSnapshot })).toEqual([
      "This task belongs to an ended blocked run and cannot start again. Fix the reported cause, remove the old issue's ready label or pause it in GitHub, and wait for intake to update. Then select a new eligible task.",
    ]);
    const newTask = { ...task, taskId: AgentControlTaskId.make("new-task") };
    expect(
      agentControlStartBlockers({
        ...start,
        selectedTaskId: newTask.taskId,
        snapshot: {
          ...endedSnapshot,
          tasks: [{ ...task, sourceGate: "not-ready" }, newTask],
          nextTaskId: newTask.taskId,
        },
      }),
    ).toEqual([]);
    expect(agentControlRunStatus({ ...ended, task: null }).label).toContain("Ended · blocked");
    expect(agentControlRunStatus({ ...ended, task: { ...task, status: "succeeded" } }).tone).toBe(
      "warning",
    );
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
      "Task readiness changed or the next task already has execution history. Refresh intake; if the old issue was already attempted, remove its ready label or pause it in GitHub, then choose a new eligible task.",
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

  it("presents queued admission as waiting instead of provider-running", () => {
    const waiting = {
      ...stage,
      status: "waiting" as const,
      admissionWait: {
        reason: "local-capacity" as const,
        hostId: "builder-01",
        detail: "One managed check is already running.",
      },
      verification: null,
    };
    expect(
      agentControlRunStatus({
        ...run,
        task: { ...task, status: "running" },
        state: { ...run.state, status: "active" },
        stages: [waiting],
      }),
    ).toEqual({
      label:
        "Waiting for local check capacity on builder-01. One managed check is already running.",
      tone: "neutral",
    });
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

describe("Armed client state", () => {
  const off = { ...snapshot, armed: { enabled: false } };
  const on = {
    ...snapshot,
    armed: { enabled: true },
    projectState: { ...snapshot.projectState, mode: "armed" as const },
  };
  it("keeps automatic authority visible during a run-once execution and before its run is published", () => {
    const executing = {
      ...on,
      projectState: { ...on.projectState, mode: "run-once" as const },
      runs: [],
    };
    expect(agentControlArmedStatus(executing)).toMatchObject({ enabled: true, tone: "running" });
    expect(agentControlArmedStatus(on).label).toContain("waiting");
    expect(
      agentControlArmedStatus({
        ...executing,
        runs: [
          {
            ...run,
            task: { ...task, status: "running" },
            state: { ...run.state, status: "active" },
          },
        ],
      }).label,
    ).toContain("task running");
  });
  it("does not reactivate automatic mode from a historical armed run or an eligible follow-up", () => {
    expect(off.nextTaskId).not.toBeNull();
    expect(
      agentControlArmedStatus({ ...off, runs: [{ ...run, originMode: "armed" }] }),
    ).toMatchObject({ enabled: false, label: "Automatic mode off" });
    expect(agentControlDisarmInput({ ...start, snapshot: off })).toBeNull();
  });
  it("shows project and run blockers without hiding the stop action", () => {
    const blocked = { ...on, blockers: ["source-watermark-stale"] };
    expect(agentControlArmedStatus(blocked).label).toContain("blocked");
    expect(agentControlDisarmInput({ ...start, snapshot: blocked })?.mode).toBe("observe");
    expect(
      agentControlArmedStatus({
        ...on,
        runs: [
          { ...run, errorCode: "provider-unavailable", state: { ...run.state, status: "active" } },
        ],
      }).tone,
    ).toBe("warning");
  });
  it("can enable waiting without selecting tasks and delegates admission to the server", () => {
    expect(
      agentControlArmBlockers({
        ...start,
        snapshot: { ...off, tasks: [], runs: [], nextTaskId: null },
      }),
    ).toEqual([]);
    expect(agentControlArmInput(off)).toMatchObject({
      expectedRevision: 4,
      projectId: "project",
      mode: "armed",
    });
    expect(agentControlArmInput(off)).not.toHaveProperty("runOnceTaskId");
    expect(agentControlArmBlockers({ ...start, snapshot: on }).length).toBeGreaterThan(0);
  });
  it("fails closed for old servers, unknown or refreshing permissions and reconnect gaps", () => {
    expect(agentControlArmedStatus(snapshot).enabled).toBeNull();
    expect(agentControlArmBlockers(start).join(" ")).toContain("authority");
    for (const unavailable of [
      { connected: false },
      { pending: true },
      { snapshot: null },
      ...[
        AsyncResult.initial<AuthSessionState>(),
        AsyncResult.waiting(AsyncResult.success(adminSession)),
        AsyncResult.success({ ...adminSession, scopes: AuthStandardClientScopes }),
        AsyncResult.fail(new Error("reconnect")),
      ].map((session) => ({ modeChangeBlocker: agentControlModeChangeBlocker(session) })),
    ]) {
      expect(
        agentControlArmBlockers({ ...start, snapshot: off, ...unavailable }).length,
      ).toBeGreaterThan(0);
      expect(agentControlDisarmInput({ ...start, snapshot: on, ...unavailable })).toBeNull();
    }
    expect(agentControlArmBlockers({ ...start, snapshot: off })).toEqual([]);
    expect(agentControlDisarmInput({ ...start, snapshot: on })).not.toBeNull();
  });
  it("retains command identity at one revision and issues new identities after stop and reactivation", () => {
    const disarm = agentControlDisarmInput({ ...start, snapshot: on });
    expect(agentControlDisarmInput({ ...start, snapshot: on })).toEqual(disarm);
    const renewed = { ...off, projectState: { ...off.projectState, revision: 8 } };
    expect(agentControlArmInput(renewed).commandId).not.toBe(agentControlArmInput(off).commandId);
    expect(agentControlArmInput(renewed).expectedRevision).toBe(8);
    expect(
      agentControlDisarmInput({
        ...start,
        snapshot: { ...on, projectState: { ...on.projectState, revision: 9 } },
      })?.commandId,
    ).not.toBe(disarm?.commandId);
  });
});

it("does not treat a cached streaming value as fresh after reconnect", () => {
  expect(
    agentControlSnapshotFresh(AsyncResult.waiting(AsyncResult.success(snapshot)), snapshot, true),
  ).toBe(false);
  expect(agentControlSnapshotFresh(AsyncResult.success({ ...snapshot }), snapshot, false)).toBe(
    false,
  );
  expect(agentControlSnapshotFresh(AsyncResult.success({ ...snapshot }), snapshot, true)).toBe(
    true,
  );
  expect(agentControlSnapshotFresh(AsyncResult.initial(), snapshot, true)).toBe(false);
});
it("requires history-blocked candidates to leave intake before rearming", () => {
  expect(
    agentControlArmBlockers({
      ...start,
      snapshot: { ...snapshot, armed: { enabled: false }, nextTaskId: null },
    }).join(" "),
  ).toContain("execution history");
});

it("keeps an ended Armed run honest while its admitted pipeline continues", () => {
  expect(
    agentControlRunStatus({
      ...run,
      originMode: "armed",
      task,
      state: { ...run.state, terminalTaskEventId: null },
      stages: [{ ...stage, status: "running", verification: null }],
    }),
  ).toEqual({ label: "Automatic run ended · admitted work may continue", tone: "warning" });
});

it("explains why Run once is unavailable while automation is waiting", () => {
  expect(
    agentControlStartBlockers({
      ...start,
      snapshot: {
        ...snapshot,
        armed: { enabled: true },
        projectState: { ...snapshot.projectState, mode: "armed" },
      },
    }),
  ).toContain("Turn off automation before starting a single task.");
});

describe("autonomous command error presentation", () => {
  it("explains typed revision conflicts even though their Error message is empty", () => {
    const error = new AgentControlProjectRevisionConflictError({
      code: "revision-conflict",
      projectId: snapshot.projectId,
      expectedRevision: 4,
      actualRevision: 5,
    });
    expect(error.message).toBe("");
    expect(agentControlCommandErrorMessage(error)).toBe(
      "The project changed before this request was accepted. Review the refreshed state and try again.",
    );
    const replay = new AgentControlCommandPreviouslyRejectedError({
      code: "command-previously-rejected",
      commandId: agentControlArmInput(snapshot).commandId,
      originalErrorCode: "revision-conflict",
    });
    expect(agentControlCommandErrorMessage(replay)).toContain("previously rejected");
  });
  it("preserves useful error messages and never hides an unknown failure", () => {
    expect(agentControlCommandErrorMessage(new Error("Connection lost"))).toBe("Connection lost");
    for (const error of [
      null,
      undefined,
      new Error(""),
      new Error("  "),
      { code: "internal-persistence-error" },
    ]) {
      expect(agentControlCommandErrorMessage(error)).toContain("The request failed");
    }
    expect(agentControlCommandErrorMessage({ code: "internal-persistence-error" })).toContain(
      "internal-persistence-error",
    );
  });
});

describe("Armed start and paused states", () => {
  it("reports a preselection activation as starting and still prioritizes real blockers", () => {
    const starting = {
      ...run,
      task: null,
      state: {
        ...run.state,
        status: "active" as const,
        lastStep: "activation-admitted" as const,
        taskId: null,
      },
      stages: [],
    };
    const active = {
      ...snapshot,
      armed: { enabled: true },
      projectState: { ...snapshot.projectState, mode: "run-once" as const },
      runs: [starting],
    };
    expect(agentControlRunStatus(starting)).toEqual({ label: "Starting", tone: "running" });
    expect(agentControlArmedStatus(active)).toMatchObject({ enabled: true, tone: "running" });
    expect(agentControlArmedStatus({ ...active, blockers: ["source-watermark-stale"] }).tone).toBe(
      "warning",
    );
    expect(agentControlRunStatus({ ...starting, errorCode: "source-watermark-stale" }).tone).toBe(
      "warning",
    );
    expect(
      agentControlRunStatus({ ...starting, state: { ...starting.state, taskId: task.taskId } }),
    ).toEqual({ label: "Waiting for task data", tone: "warning" });
  });
  it.each(["observe", "armed", "run-once"] as const)(
    "leaves paused %s through Manual without reviving the old operation",
    (pausedFromMode) => {
      const paused = {
        ...snapshot,
        armed: { enabled: false },
        projectState: { ...snapshot.projectState, mode: "paused" as const, pausedFromMode },
        runs: [{ ...run, state: { ...run.state, status: "active" as const } }],
      };
      const input = { snapshot: paused, connected: true, pending: false, modeChangeBlocker: null };
      expect(agentControlArmedStatus(paused).label).toContain("project paused");
      const command = agentControlEndPausedInput(input);
      expect(command).toMatchObject({
        mode: "manual",
        projectId: snapshot.projectId,
        expectedRevision: snapshot.projectState.revision,
      });
      expect(command).not.toHaveProperty("runOnceTaskId");
      expect(agentControlEndPausedInput(input)).toEqual(command);
      for (const unavailable of [
        { connected: false },
        { pending: true },
        { snapshot: null },
        { snapshot },
        { modeChangeBlocker: "Checking permissions" },
      ]) {
        expect(agentControlEndPausedInput({ ...input, ...unavailable })).toBeNull();
      }
    },
  );
});

// Synthetic state tests exercise client authority and evidence rules; GitHub compatibility is
// verified against the native relationship reader separately.
const epicPreview: AgentControlEpicPreview = {
  canStart: true,
  projectId: snapshot.projectId,
  source: {
    format: "github-native-sub-issues-v1",
    repository: { repositoryNodeId: "repository", nameWithOwner: "test/project" },
    epic: {
      repositoryNodeId: "repository",
      nameWithOwner: "test/project",
      issueNodeId: "epic",
      number: 100,
      url: "https://github.com/test/project/issues/100",
      title: "Synthetic Epic",
      state: "open",
      subIssueCount: 1,
    },
    tasks: [
      {
        issue: {
          repositoryNodeId: "repository",
          nameWithOwner: "test/project",
          issueNodeId: "issue",
          number: 101,
          url: "https://github.com/test/project/issues/101",
          title: "Child task",
          state: "open",
          subIssueCount: 0,
        },
        position: 0,
        dependencies: [],
      },
    ],
    blockers: [],
    fingerprint: "scope-fingerprint",
    inspectedAt: timestamp,
  },
  blockers: [],
};
const epicRun: AgentControlEpicRuntimeView = {
  epicRunId: "epic-run",
  projectId: snapshot.projectId,
  revision: 7,
  status: "running",
  source: epicPreview.source,
  checks: policy.projectPolicy!.policy.verificationChecks!,
  members: [
    {
      issueNodeId: "issue",
      issueNumber: 101,
      taskId: task.taskId,
      childRunId: run.state.runId,
      status: "accepted",
      baseCommitSha: "base",
      reservationId: "reservation",
      taskFinalizationEvidenceId: "finalized",
      accepted: {
        commitSha: "common",
        treeSha: "tree",
        codeDigest: "digest",
        evidenceId: "accepted-evidence",
      },
    },
  ],
  activeTaskId: null,
  acceptedCommitSha: "common",
  blockers: [],
  blockerHistory: [],
  verificationAttempt: 1,
  finalVerification: null,
  finalVerificationHistory: [],
  createdAt: timestamp,
  updatedAt: timestamp,
};

describe("Epic execution client state", () => {
  it("allows reviewed integration retries without treating a failed provider task as retryable", () => {
    const dependencyPlan = agentControlEpicExecutionOptions(
      epicPreview,
      2,
      "Separate components",
    ).dependencyPlan!;
    const blocked = {
      ...epicRun,
      status: "blocked" as const,
      dependencyPlan,
      members: epicRun.members.map((member) => ({
        ...member,
        status: "failed" as const,
        captured: member.accepted!,
      })),
    };
    const readiness = { ...start, snapshot: { ...snapshot, epic: blocked } };
    expect(agentControlEpicControlAllowed(readiness, "resume")).toBe(true);
    expect(
      agentControlEpicControlAllowed(
        {
          ...readiness,
          snapshot: {
            ...snapshot,
            epic: {
              ...blocked,
              members: epicRun.members.map((member) => ({ ...member, status: "failed" as const })),
            },
          },
        },
        "resume",
      ),
    ).toBe(false);
  });

  it("requires a new command identity for a different approved parallel plan", () => {
    const serial = agentControlEpicStartInput(snapshot, epicPreview);
    const options = agentControlEpicExecutionOptions(epicPreview, 2, "Separate components");
    const parallel = agentControlEpicStartInput(snapshot, epicPreview, options);
    expect(agentControlEpicExecutionOptions(epicPreview, 1, "")).toEqual({});
    const reviewedSerial = agentControlEpicExecutionOptions(
      epicPreview,
      1,
      "Separate components",
      true,
    );
    expect(reviewedSerial.parallelism).toBe(1);
    expect(reviewedSerial.dependencyPlan).toEqual(options.dependencyPlan);
    expect(agentControlEpicStartInput(snapshot, epicPreview, reviewedSerial).commandId).not.toBe(
      serial.commandId,
    );

    expect(parallel.commandId).not.toBe(serial.commandId);
    expect(agentControlEpicStartInput(snapshot, epicPreview, options)).toEqual(parallel);
    expect(
      agentControlEpicStartInput(
        snapshot,
        epicPreview,
        agentControlEpicExecutionOptions(epicPreview, 3, "Separate components"),
      ).commandId,
    ).not.toBe(parallel.commandId);
    expect(options.dependencyPlan?.tasks).toEqual([{ issueNodeId: "issue", dependsOn: [] }]);
  });

  it("keeps concurrent task navigation and server wait reasons attached to the matching run", () => {
    const member = { ...epicRun.members[0]!, status: "running" as const };
    const other = {
      ...run,
      state: { ...run.state, runId: AgentControlRunOnceId.make("other") },
      stages: [],
    };
    const progress = agentControlEpicMemberProgress(member, [other, run]);
    expect(progress.threadId).toBe(run.stages.at(-1)?.threadId ?? null);
    expect(
      agentControlEpicMemberProgress({ ...member, waitReason: "dependencies" }, []).label,
    ).toContain("predecessor integrations");
    expect(agentControlEpicMemberProgress({ ...member, waitReason: "capacity" }, []).label).toBe(
      "Waiting for capacity",
    );
    expect(
      agentControlEpicMemberProgress({ ...member, waitReason: "integration" }, []).label,
    ).toContain("combined result");
    expect(
      agentControlEpicMemberProgress(
        { ...member, waitReason: "blocker", blocker: "Merge conflict" },
        [],
      ).label,
    ).toBe("Blocked: Merge conflict");
  });

  it("admits inspected scope independently of unrelated backlog ordering and retains stable command identity", () => {
    const readiness = { ...start, snapshot: { ...snapshot, tasks: [], nextTaskId: null } };
    expect(agentControlEpicStartBlockers(readiness, epicPreview)).toEqual([]);
    const input = agentControlEpicStartInput(snapshot, epicPreview);
    expect(input.expectedFingerprint).toBe(epicPreview.source.fingerprint);
    expect(input.expectedRevision).toBe(snapshot.projectState.revision);
    expect(
      agentControlEpicStartInput(decodeSnapshot(JSON.parse(JSON.stringify(snapshot))), epicPreview),
    ).toEqual(input);
    expect(
      agentControlEpicStartInput(snapshot, {
        ...epicPreview,
        source: { ...epicPreview.source, fingerprint: "changed" },
      }).commandId,
    ).not.toBe(input.commandId);
  });

  it("shows structural and intake blockers before start, including failed or missing preview", () => {
    const message = "Nested sub-issues are not supported";
    const preview = {
      ...epicPreview,
      canStart: false,
      blockers: [{ code: "nested-sub-issues", issueNumber: 101, message }],
    };
    expect(agentControlEpicStartBlockers(start, preview)).toContain(message);
    expect(agentControlEpicStartBlockers(start, { ...preview, canStart: true })).toEqual([]);
    expect(
      agentControlEpicStartBlockers(start, {
        ...epicPreview,
        source: {
          ...epicPreview.source,
          blockers: [
            {
              code: "missing-prerequisite",
              issueNumber: 101,
              message: "A different child requires an open external prerequisite",
            },
          ],
        },
      }),
    ).toEqual([]);
    expect(agentControlEpicStartBlockers(start, null)).toContain(
      "Inspect an Epic in this project before starting.",
    );
    expect(agentControlEpicStartBlockers({ ...start, policy: null }, epicPreview)).toContain(
      "Load the project's verification configuration before starting.",
    );
  });

  it("checks selected environment permissions for Epic start, resume, stop and clear", () => {
    const blocker = agentControlModeChangeBlocker(
      AsyncResult.success({ ...adminSession, scopes: AuthStandardClientScopes }),
    );
    expect(
      agentControlEpicStartBlockers({ ...start, modeChangeBlocker: blocker }, epicPreview),
    ).toContain(blocker);
    const readiness = {
      ...start,
      snapshot: { ...snapshot, epic: { ...epicRun, status: "blocked" as const } },
      modeChangeBlocker: blocker,
    };
    for (const action of ["resume", "stop", "clear"] as const) {
      expect(agentControlEpicControlAllowed(readiness, action)).toBe(false);
      expect(
        agentControlEpicControlAllowed(
          { ...readiness, modeChangeBlocker: null, connected: false },
          action,
        ),
      ).toBe(false);
      expect(
        agentControlEpicControlAllowed(
          { ...readiness, modeChangeBlocker: null, pending: true },
          action,
        ),
      ).toBe(false);
    }
  });

  it("resumes only the same Epic and preserves control identity at its revision", () => {
    const readiness = {
      ...start,
      snapshot: { ...snapshot, epic: epicRun, armed: { enabled: false } },
    };
    expect(agentControlEpicControlAllowed(readiness, "resume")).toBe(true);
    expect(
      agentControlEpicControlAllowed(
        {
          ...readiness,
          snapshot: {
            ...readiness.snapshot,
            epic: {
              ...epicRun,
              status: "blocked",
              members: epicRun.members.map((member) => ({ ...member, status: "failed" })),
            },
          },
        },
        "resume",
      ),
    ).toBe(false);
    expect(
      agentControlEpicControlAllowed(
        { ...readiness, snapshot: { ...readiness.snapshot, armed: { enabled: true } } },
        "resume",
      ),
    ).toBe(false);
    const input = agentControlEpicControlInput(epicRun, "resume");
    expect(input).toMatchObject({ epicRunId: "epic-run", expectedRevision: 7 });
    expect(agentControlEpicControlInput({ ...epicRun }, "resume")).toEqual(input);
    expect(agentControlEpicControlInput({ ...epicRun, revision: 8 }, "resume").commandId).not.toBe(
      input.commandId,
    );
    expect(agentControlEpicControlInput(epicRun, "stop").commandId).not.toBe(input.commandId);
  });

  it("retains a completed or stopped scope until explicitly cleared with automation off", () => {
    const readiness = {
      ...start,
      snapshot: {
        ...snapshot,
        epic: { ...epicRun, status: "stopped" as const },
        armed: { enabled: false },
      },
    };
    expect(agentControlArmBlockers(readiness)).toContain(
      "This project has an Epic execution target. Use its resume or end controls.",
    );
    expect(agentControlStartBlockers(readiness)).toContain(
      "This project has an Epic execution target. Use its resume or end controls.",
    );
    expect(agentControlEpicControlAllowed(readiness, "resume")).toBe(false);
    expect(agentControlEpicControlAllowed(readiness, "stop")).toBe(false);
    expect(agentControlEpicControlAllowed(readiness, "clear")).toBe(true);
    expect(
      agentControlEpicControlAllowed(
        { ...readiness, snapshot: { ...readiness.snapshot, armed: { enabled: true } } },
        "clear",
      ),
    ).toBe(false);
    expect(
      agentControlArmBlockers({ ...readiness, snapshot: { ...readiness.snapshot, epic: null } }),
    ).toEqual([]);
  });

  it("never substitutes green child runs for final common-result verification", () => {
    const succeeded = { ...epicRun, status: "succeeded" as const };
    expect(agentControlEpicStatus(succeeded).tone).toBe("warning");
    const finalVerification = {
      status: "passed" as const,
      commitSha: "common",
      evidenceId: "final",
      detail: "Required checks passed",
      checks: stage.verification!.checks,
    };
    expect(agentControlEpicStatus({ ...succeeded, finalVerification }).tone).toBe("success");
    expect(
      agentControlEpicStatus({
        ...succeeded,
        finalVerification,
        checks: [...succeeded.checks, { ...succeeded.checks[0]!, id: "second-required" }],
      }).tone,
    ).toBe("warning");
    for (const verification of [
      { ...finalVerification, commitSha: "different-commit" },
      { ...finalVerification, checks: [] },
      { ...finalVerification, status: "failed" as const },
      {
        ...finalVerification,
        checks: finalVerification.checks.map((check) => ({ ...check, status: "failed" as const })),
      },
      {
        ...finalVerification,
        checks: finalVerification.checks.map((check) => ({ ...check, completedAt: null })),
      },
    ])
      expect(agentControlEpicStatus({ ...succeeded, finalVerification: verification }).tone).toBe(
        "warning",
      );
  });
});

const verifiedEpic: AgentControlEpicRuntimeView = {
  ...epicRun,
  status: "succeeded",
  finalVerification: {
    status: "passed",
    commitSha: "common",
    evidenceId: "common-evidence",
    detail: "Required checks passed",
    checks: stage.verification!.checks,
  },
};
const handoffPreview: AgentControlEpicHandoffPreview = {
  projectId: verifiedEpic.projectId,
  epicRunId: verifiedEpic.epicRunId,
  repository: verifiedEpic.source.repository,
  targetBranch: "main",
  commitSha: "common",
  branchName: null,
  canPublish: true,
  blockers: [],
  handoff: null,
};
const publishedHandoff: AgentControlEpicHandoff = {
  intentId: "handoff",
  status: "published",
  repository: verifiedEpic.source.repository,
  targetBranch: "main",
  baseCommitSha: "base",
  commitSha: "common",
  branchName: "t3auto/epic-run",
  verificationEvidenceId: "common-evidence",
  requestedAt: timestamp,
  updatedAt: timestamp,
  pullRequest: {
    number: 102,
    url: "https://github.com/test/project/pull/102",
    state: "open",
    isDraft: true,
    headSha: "common",
    baseBranch: "main",
  },
  error: null,
};
const handoffReadiness = {
  epic: verifiedEpic,
  connected: true,
  pending: false,
  permissionBlocker: null,
  preview: handoffPreview,
};

describe("Epic human review handoff", () => {
  it("requires known write permission from the selected environment", () => {
    const { scopes: _scopes, ...unknownScopes } = adminSession;
    for (const session of [
      AsyncResult.initial<AuthSessionState>(),
      AsyncResult.waiting(AsyncResult.success(adminSession)),
      AsyncResult.fail(new Error("Offline")),
      AsyncResult.success(unknownScopes),
      AsyncResult.success({ ...adminSession, authenticated: false }),
      AsyncResult.success({ ...adminSession, scopes: AuthStandardClientScopes }),
    ]) {
      const permissionBlocker = agentControlEpicHandoffPermissionBlocker(session);
      expect(permissionBlocker).not.toBeNull();
      expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, permissionBlocker })).toContain(
        permissionBlocker,
      );
    }
    expect(agentControlEpicHandoffPermissionBlocker(AsyncResult.success(adminSession))).toBeNull();
    expect(agentControlEpicHandoffBlockers(handoffReadiness)).toEqual([]);
  });

  it("allows initial publication before the server allocates its unique review branch", () => {
    expect(handoffPreview.branchName).toBeNull();
    expect(handoffPreview.handoff).toBeNull();
    expect(agentControlEpicHandoffBlockers(handoffReadiness)).toEqual([]);
    expect(
      agentControlEpicPublishHandoffInput(verifiedEpic, handoffPreview).expectedTargetBranch,
    ).toBe("main");
  });

  it("binds explicit publication to the inspected accepted commit and target", () => {
    const input = agentControlEpicPublishHandoffInput(verifiedEpic, handoffPreview);
    expect(input).toMatchObject({
      projectId: verifiedEpic.projectId,
      epicRunId: "epic-run",
      expectedCommitSha: "common",
      expectedTargetBranch: "main",
      expectedRevision: 7,
    });
    expect(agentControlEpicPublishHandoffInput({ ...verifiedEpic }, { ...handoffPreview })).toEqual(
      input,
    );
    expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, pending: true })).not.toEqual([]);
    expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, connected: false })).not.toEqual(
      [],
    );
    for (const preview of [
      { ...handoffPreview, projectId: ProjectId.make("other-project") },
      { ...handoffPreview, epicRunId: "other-run" },
      { ...handoffPreview, commitSha: "unverified-head" },
      {
        ...handoffPreview,
        repository: { ...handoffPreview.repository, repositoryNodeId: "foreign-repo" },
      },
      {
        ...handoffPreview,
        repository: { ...handoffPreview.repository, nameWithOwner: "other/repo" },
      },
      { ...handoffPreview, targetBranch: null },
      { ...handoffPreview, canPublish: false },
    ]) {
      expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, preview })).not.toEqual([]);
      expect(() => agentControlEpicPublishHandoffInput(verifiedEpic, preview)).toThrow();
    }
  });

  it("blocks missing or mismatched common verification and forwards concrete remote blockers", () => {
    for (const epic of [
      { ...verifiedEpic, finalVerification: null },
      { ...verifiedEpic, acceptedCommitSha: "different" },
      { ...verifiedEpic, finalVerification: { ...verifiedEpic.finalVerification!, checks: [] } },
    ])
      expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, epic })).not.toEqual([]);
    const message = "The remote branch belongs to a different Epic run.";
    expect(
      agentControlEpicHandoffBlockers({
        ...handoffReadiness,
        preview: {
          ...handoffPreview,
          canPublish: false,
          blockers: [{ code: "foreign-branch", issueNumber: null, message }],
        },
      }),
    ).toContain(message);
  });

  it("retains published links across snapshot reloads and history, including closed or merged PRs", () => {
    for (const state of ["open", "closed", "merged"] as const) {
      const epic = {
        ...verifiedEpic,
        handoff: { ...publishedHandoff, pullRequest: { ...publishedHandoff.pullRequest!, state } },
      };
      const restored = decodeSnapshot(
        JSON.parse(JSON.stringify({ ...snapshot, epic: null, epicHistory: [epic] })),
      );
      const saved = restored.epicHistory![0]!;
      expect(saved.handoff?.pullRequest?.url).toBe(publishedHandoff.pullRequest!.url);
      expect(saved.handoff?.pullRequest?.state).toBe(state);
      expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, epic: saved })).toContain(
        "This Epic already has a pull request. Open the saved pull request to review it.",
      );
    }
  });

  it("allows retry of a failed durable handoff without losing the local result", () => {
    const epic = {
      ...verifiedEpic,
      handoff: {
        ...publishedHandoff,
        status: "failed" as const,
        pullRequest: null,
        error: { code: "offline", message: "GitHub is unavailable" },
      },
    };
    expect(agentControlEpicHandoffBlockers({ ...handoffReadiness, epic })).toEqual([]);
    expect(agentControlEpicPublishHandoffInput(epic, handoffPreview).expectedCommitSha).toBe(
      "common",
    );
    expect(epic.acceptedCommitSha).toBe("common");
    expect(epic.finalVerification).toEqual(verifiedEpic.finalVerification);
    expect(
      agentControlEpicHandoffBlockers({
        ...handoffReadiness,
        epic: { ...epic, handoff: { ...epic.handoff, status: "publishing" } },
      }),
    ).not.toEqual([]);
  });
});

describe("Epic queue client state", () => {
  const entry = {
    entryId: "a",
    source: epicPreview.source,
    approvedAt: timestamp,
    epicRunId: epicRun.epicRunId,
    status: "active" as const,
    blockers: [],
  };
  const queued: AgentControlRunOnceSnapshot = {
    ...snapshot,
    epic: epicRun,
    armed: { enabled: true },
    epicQueue: {
      projectId: snapshot.projectId,
      revision: 4,
      entries: [
        entry,
        { ...entry, entryId: "b", epicRunId: null, status: "pending" },
        { ...entry, entryId: "c", epicRunId: null, status: "pending" },
      ],
      nextEntryId: "b",
      waitReason: "Waiting for human review and merge.",
      nextCheckAt: null,
    },
  };

  it("allows explicit approval during active work without treating open dependencies as rejection", () => {
    const readiness = {
      ...start,
      snapshot: { ...snapshot, epic: epicRun, runs: [run] },
      preflight: null,
      policy: null,
    };
    const blockedPreview = { ...epicPreview, canStart: false };
    expect(agentControlEpicQueueApproveBlockers(readiness, blockedPreview)).toEqual([]);
    expect(
      agentControlEpicQueueApproveBlockers({ ...readiness, snapshot: queued }, blockedPreview),
    ).toContain("This Epic is already in the approved queue.");
    expect(
      agentControlEpicQueueApproveBlockers(readiness, {
        ...blockedPreview,
        projectId: ProjectId.make("other"),
      }),
    ).toContain("Inspect an Epic in this project before approving it.");
  });

  it("blocks queue edits for unknown, refreshing, revoked and wrong-environment permissions", () => {
    for (const session of [
      AsyncResult.initial<AuthSessionState>(),
      AsyncResult.waiting(AsyncResult.success(adminSession)),
      AsyncResult.fail(new Error("reconnect")),
      AsyncResult.success({ ...adminSession, scopes: AuthStandardClientScopes }),
      AsyncResult.success({ ...adminSession, authenticated: false }),
    ]) {
      expect(
        agentControlEpicQueueChangeBlockers({
          ...start,
          modeChangeBlocker: agentControlModeChangeBlocker(session),
        }).length,
      ).toBeGreaterThan(0);
    }
    for (const overrides of [{ connected: false }, { pending: true }, { snapshot: null }]) {
      expect(
        agentControlEpicQueueChangeBlockers({ ...start, ...overrides }).length,
      ).toBeGreaterThan(0);
    }
    expect(agentControlEpicQueueChangeBlockers(start)).toEqual([]);
  });

  it("requires fresh write authority, disarm and removal of waiting approvals before leaving", () => {
    const finished = {
      ...queued,
      armed: { enabled: false },
      runs: [],
      epic: { ...epicRun, status: "succeeded" as const },
      epicQueue: { ...queued.epicQueue!, entries: [{ ...entry, status: "merged" as const }] },
    };
    const ready = { ...start, snapshot: finished };
    expect(agentControlEpicQueueLeaveBlockers(ready)).toEqual([]);
    expect(agentControlEpicControlAllowed(ready, "clear")).toBe(false);
    for (const overrides of [
      { connected: false },
      { pending: true },
      { snapshot: null },
      { modeChangeBlocker: "Checking permissions" },
    ]) {
      expect(agentControlEpicQueueLeaveBlockers({ ...ready, ...overrides }).length).toBeGreaterThan(
        0,
      );
    }
    expect(
      agentControlEpicQueueLeaveBlockers({ ...ready, snapshot: queued }).length,
    ).toBeGreaterThan(0);
    expect(
      agentControlEpicQueueLeaveBlockers({
        ...ready,
        snapshot: { ...finished, runs: [{ ...run, state: { ...run.state, status: "active" } }] },
      }).length,
    ).toBeGreaterThan(0);
    expect(
      agentControlEpicQueueLeaveBlockers({
        ...ready,
        snapshot: {
          ...finished,
          projectState: { ...finished.projectState, mode: "paused", pausedFromMode: "run-once" },
        },
      }).length,
    ).toBeGreaterThan(0);
    const disabled = {
      ...finished,
      epic: null,
      epicQueue: { ...finished.epicQueue, enabled: false, entries: [] },
    };
    expect(agentControlEpicQueueView(disabled)).toBeNull();
    expect(agentControlEpicStartBlockers({ ...ready, snapshot: disabled }, epicPreview)).toEqual(
      [],
    );
    expect(
      agentControlEpicQueueApproveBlockers({ ...ready, snapshot: disabled }, epicPreview),
    ).toEqual([]);
    expect(
      agentControlEpicQueueChangeInput(disabled, {
        kind: "approve",
        epicNumber: epicPreview.source.epic.number,
        expectedFingerprint: epicPreview.source.fingerprint,
      }).expectedRevision,
    ).toBe(finished.epicQueue.revision);
  });

  it("moves the complete pending order and never moves active or merged entries", () => {
    expect(agentControlEpicQueueMoveInput(queued, "c", -1)?.action).toEqual({
      kind: "reorder",
      entryIds: ["c", "b"],
    });
    expect(agentControlEpicQueueMoveInput(queued, "b", 1)?.action).toEqual({
      kind: "reorder",
      entryIds: ["c", "b"],
    });
    expect(agentControlEpicQueueMoveInput(queued, "a", 1)).toBeNull();
    expect(agentControlEpicQueueMoveInput(queued, "b", -1)).toBeNull();
    expect(agentControlEpicQueueMoveInput(queued, "c", 1)).toBeNull();
    expect(
      agentControlEpicQueueMoveInput(
        {
          ...queued,
          epicQueue: {
            ...queued.epicQueue!,
            entries: [{ ...entry, status: "merged" }],
          },
        },
        "a",
        1,
      ),
    ).toBeNull();
  });

  it("keeps identities stable at a revision and changes identity for a new order or revision", () => {
    const action = { kind: "remove" as const, entryId: "b" };
    const first = agentControlEpicQueueChangeInput(queued, action);
    expect(agentControlEpicQueueChangeInput(queued, action)).toEqual(first);
    expect(first.expectedRevision).toBe(4);
    expect(
      agentControlEpicQueueChangeInput(
        {
          ...queued,
          epicQueue: {
            ...queued.epicQueue!,
            revision: 5,
          },
        },
        action,
      ).commandId,
    ).not.toBe(first.commandId);
    expect(
      agentControlEpicQueueChangeInput(queued, { ...action, entryId: "c" }).commandId,
    ).not.toBe(first.commandId);
    expect(agentControlEpicQueueChangeInput(snapshot, action).expectedRevision).toBe(0);
  });

  it("shows the server's next candidate, active Epic and review wait independently of local success", () => {
    expect(agentControlEpicQueueView(queued)).toMatchObject({
      active: { entryId: "a" },
      next: { entryId: "b" },
      waitReason: "Waiting for human review and merge.",
    });
    expect(agentControlArmedStatus(queued).label).toContain("human review and merge");
    expect(
      agentControlArmedStatus({ ...queued, armed: { ...queued.armed!, enabled: false } }),
    ).toMatchObject({ enabled: false });
    expect(agentControlEpicQueueView(snapshot)).toBeNull();
    const empty = {
      ...queued,
      epic: null,
      epicQueue: {
        ...queued.epicQueue!,
        entries: [],
        nextEntryId: null,
        waitReason: null,
      },
    };
    expect(agentControlArmedStatus(empty).label).toContain("eligible approved Epic");
  });

  it("allows re-arm while a queued Epic waits and keeps manual starts out of queue mode", () => {
    expect(
      agentControlArmBlockers({
        ...start,
        snapshot: {
          ...queued,
          runs: [],
          armed: { ...queued.armed!, enabled: false },
        },
      }),
    ).toEqual([]);
    expect(agentControlEpicStartBlockers({ ...start, snapshot: queued }, epicPreview)).toContain(
      "Approve this Epic for the queue and enable Armed to start it.",
    );
    expect(agentControlStartBlockers({ ...start, snapshot: { ...queued, epic: null } })).toContain(
      "This project uses an approved Epic queue. Enable Armed to continue it.",
    );
    expect(
      agentControlEpicControlAllowed(
        {
          ...start,
          snapshot: {
            ...queued,
            armed: { enabled: false },
            runs: [],
            epic: { ...epicRun, status: "succeeded" },
          },
        },
        "clear",
      ),
    ).toBe(false);
  });
});
