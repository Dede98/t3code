import { type AgentControlArmedDispatch, type ProjectId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlRunOnceController } from "../../runOnce/Services/AgentControlRunOnceController.ts";
import { withAgentControlRunOnceProjectFence } from "../../runOnce/context.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { AgentControlTaskIntakeReactor } from "../../task/Services/AgentControlTaskIntakeReactor.ts";
import { superviseAgentControlRunOnceListener } from "../../runOnce/Layers/AgentControlRunOnceController.ts";
import {
  activateArmedDispatch,
  claimArmedDispatch,
  finishArmedDispatch,
  loadArmedCatchUpProjectIds,
} from "../authority.ts";
import { AgentControlArmedError } from "../model.ts";
import {
  AgentControlArmedScheduler,
  type AgentControlArmedSchedulerShape,
} from "../Services/AgentControlArmedScheduler.ts";

const PROJECT_WORKERS = 2;
const CLAIM_DURATION_MS = 30_000;
const RETRY_BASE_MS = 25;
const RETRY_MAX_MS = 1_000;
const RETRY_LIMIT = 6;
const isArmedError = Schema.is(AgentControlArmedError);

export interface AgentControlArmedSchedulerHooks {
  readonly afterClaim?: (dispatch: AgentControlArmedDispatch) => Effect.Effect<void>;
  readonly afterModeEvent?: (dispatch: AgentControlArmedDispatch) => Effect.Effect<void>;
  readonly afterActivation?: (dispatch: AgentControlArmedDispatch) => Effect.Effect<void>;
  readonly beforeRunOnce?: (dispatch: AgentControlArmedDispatch) => Effect.Effect<void>;
  readonly afterRunOnce?: (dispatch: AgentControlArmedDispatch) => Effect.Effect<void>;
}

export interface AgentControlArmedSchedulerOptions {
  readonly hooks?: AgentControlArmedSchedulerHooks;
  readonly claimDurationMs?: number;
  readonly retryLimit?: number;
}

const fail = (
  projectId: ProjectId,
  reason: AgentControlArmedError["reason"],
  cause?: unknown,
  retryAt?: string,
) =>
  new AgentControlArmedError({
    projectId,
    reason,
    ...(cause === undefined ? {} : { cause }),
    ...(retryAt === undefined ? {} : { retryAt }),
  });

export const makeAgentControlArmedWorkScheduler = Effect.fn("makeAgentControlArmedWorkScheduler")(
  function* (
    processProject: AgentControlArmedSchedulerShape["processProject"],
    awaitReady: Effect.Effect<void>,
    retryLimit: number,
    reportFailure: (failure: AgentControlArmedError) => Effect.Effect<void> = () => Effect.void,
  ) {
    const wakeups = yield* Queue.dropping<void>(PROJECT_WORKERS);
    // One coalesced retry wakeup drives one scoped timer coordinator. Delayed
    // projects are data in the keyed map below; they never allocate one Fiber
    // (and one timer) per project.
    const retryWakeups = yield* Queue.dropping<void>(1);
    const work = yield* Ref.make({
      pending: new Set<ProjectId>(),
      running: new Set<ProjectId>(),
      dirty: new Set<ProjectId>(),
      retries: new Map<ProjectId, number>(),
      delayed: new Map<ProjectId, number>(),
    });
    const signal = Queue.offer(wakeups, undefined).pipe(Effect.asVoid);
    const signalRetryCoordinator = Queue.offer(retryWakeups, undefined).pipe(Effect.asVoid);
    const schedule = (projectId: ProjectId) =>
      Ref.modify(work, (state) => {
        if (state.running.has(projectId)) {
          const dirty = new Set(state.dirty);
          dirty.add(projectId);
          return [false, { ...state, dirty }] as const;
        }
        // A durable source/task wakeup is only a prompt to rescan. While a
        // transient retry is delayed, duplicate prompts must not erase either
        // its retry budget or deadline. A genuinely newer durable epoch is still
        // observed by the already scheduled authoritative rescan.
        if (state.pending.has(projectId) || state.delayed.has(projectId)) {
          return [false, state] as const;
        }
        const pending = new Set(state.pending);
        pending.add(projectId);
        return [true, { ...state, pending }] as const;
      }).pipe(
        Effect.flatMap((added) =>
          (added ? signal : Effect.void).pipe(Effect.andThen(signalRetryCoordinator)),
        ),
      );
    const claim = Ref.modify(work, (state) => {
      const projectId = state.pending.values().next().value;
      if (projectId === undefined) return [null, state] as const;
      const pending = new Set(state.pending);
      const running = new Set(state.running);
      pending.delete(projectId);
      running.add(projectId);
      return [
        { projectId, retry: state.retries.get(projectId) ?? 0, hasPending: pending.size > 0 },
        { ...state, pending, running },
      ] as const;
    });
    const run = (projectId: ProjectId, retry: number) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(processProject(projectId));
        if (Exit.isSuccess(exit)) return null;
        if (exit.cause.reasons.some(Cause.isInterruptReason))
          return yield* Effect.failCause(exit.cause);
        const failures = exit.cause.reasons
          .filter(Cause.isFailReason)
          .map((reason) => reason.error);
        const transient =
          failures.length === exit.cause.reasons.length &&
          failures.every(
            (error) =>
              isArmedError(error) &&
              (error.reason === "persistence" ||
                error.reason === "source-unavailable" ||
                error.reason === "source-watermark-stale"),
          );
        const retryAt = failures.find(
          (error): error is AgentControlArmedError =>
            isArmedError(error) && error.retryAt !== undefined,
        )?.retryAt;
        if (retryAt !== undefined) {
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          const target = DateTime.toEpochMillis(DateTime.makeUnsafe(retryAt));
          if (target > now) return target - now;
        }
        if (!transient || retry >= retryLimit) {
          const reported =
            failures.find((error): error is AgentControlArmedError => isArmedError(error)) ??
            fail(projectId, "projection-corrupt", exit.cause);
          return yield* reportFailure(reported).pipe(
            Effect.andThen(
              Effect.logError("Armed project work failed closed", {
                projectId,
                retries: retry,
                cause: exit.cause,
              }),
            ),
            Effect.as(null),
          );
        }
        return Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** Math.min(5, retry));
      });
    const finish = (projectId: ProjectId, delay: number | null) =>
      Effect.gen(function* () {
        const retryAt = delay === null ? null : DateTime.toEpochMillis(yield* DateTime.now) + delay;
        return yield* Ref.modify(work, (state) => {
          const pending = new Set(state.pending);
          const running = new Set(state.running);
          const dirty = new Set(state.dirty);
          const retries = new Map(state.retries);
          const delayed = new Map(state.delayed);
          running.delete(projectId);
          const changed = dirty.delete(projectId);
          if (changed && delay === null) {
            retries.delete(projectId);
            delayed.delete(projectId);
            pending.add(projectId);
          } else if (delay === null) {
            retries.delete(projectId);
            delayed.delete(projectId);
          } else {
            retries.set(projectId, (retries.get(projectId) ?? 0) + 1);
            delayed.set(projectId, retryAt!);
          }
          return [
            { hasPending: pending.size > 0, hasDelayed: delayed.size > 0 },
            { pending, running, dirty, retries, delayed },
          ] as const;
        }).pipe(
          Effect.flatMap((result) =>
            (result.hasPending ? signal : Effect.void).pipe(
              Effect.andThen(result.hasDelayed ? signalRetryCoordinator : Effect.void),
            ),
          ),
        );
      });
    const promoteDueRetries = Effect.fn("AgentControlArmed.promoteDueRetries")(function* () {
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      return yield* Ref.modify(work, (state) => {
        const pending = new Set(state.pending);
        const delayed = new Map(state.delayed);
        let nextAt: number | null = null;
        for (const [projectId, retryAt] of delayed) {
          if (retryAt <= now) {
            delayed.delete(projectId);
            if (!state.running.has(projectId)) pending.add(projectId);
          } else if (nextAt === null || retryAt < nextAt) {
            nextAt = retryAt;
          }
        }
        return [
          { hasPending: pending.size > state.pending.size, nextAt },
          { ...state, pending, delayed },
        ] as const;
      });
    });
    const retryCoordinator = Effect.fn("AgentControlArmed.retryCoordinator")(function* () {
      while (true) {
        yield* Queue.take(retryWakeups);
        while (true) {
          const promoted = yield* promoteDueRetries();
          if (promoted.hasPending) yield* signal;
          if (promoted.nextAt === null) break;
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          if (promoted.nextAt <= now) continue;
          yield* Effect.raceFirst(
            Effect.sleep(Duration.millis(promoted.nextAt - now)),
            Queue.take(retryWakeups),
          );
        }
      }
    });
    const worker = Effect.fn("AgentControlArmed.projectWorker")(function* () {
      yield* awaitReady;
      while (true) {
        yield* Queue.take(wakeups);
        const next = yield* claim;
        if (next === null) continue;
        if (next.hasPending) yield* signal;
        yield* finish(next.projectId, yield* run(next.projectId, next.retry));
      }
    });
    yield* Effect.forEach(
      Array.from({ length: PROJECT_WORKERS }),
      () => worker().pipe(Effect.forkScoped),
      { discard: true },
    );
    yield* retryCoordinator().pipe(Effect.forkScoped);
    return schedule;
  },
);

export const make = Effect.fn("AgentControlArmedScheduler.make")(function* (
  options: AgentControlArmedSchedulerOptions = {},
) {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const projectEngine = yield* AgentControlEngine;
  const taskEngine = yield* AgentControlTaskEngine;
  const taskIntake = yield* AgentControlTaskIntakeReactor;
  const runOnce = yield* AgentControlRunOnceController;
  const ownerId = `armed-controller-${yield* crypto.randomUUIDv4}`;
  const runtimeFailure = yield* Deferred.make<never, AgentControlArmedError>();
  const reportFailure = (failure: AgentControlArmedError) =>
    Deferred.fail(runtimeFailure, failure).pipe(Effect.ignore);
  const claimDurationMs = Math.max(1, Math.floor(options.claimDurationMs ?? CLAIM_DURATION_MS));
  const retryLimit = Math.max(0, Math.floor(options.retryLimit ?? RETRY_LIMIT));

  const loadState = Effect.fn("AgentControlArmed.loadDispatchState")(function* (
    projectId: ProjectId,
    dispatchId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT dispatch.status, dispatch.activation_event_id AS "activationEventId",
        dispatch.updated_at AS "updatedAt", project.mode,
        project.paused_from_mode AS "pausedFromMode"
      FROM main.agent_control_armed_dispatch_states dispatch
      JOIN main.agent_control_project_states project ON project.project_id = dispatch.project_id
      WHERE dispatch.project_id = ${projectId} AND dispatch.dispatch_id = ${dispatchId}
    `.pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
    const row = rows[0];
    if (
      rows.length !== 1 ||
      row === undefined ||
      typeof row.status !== "string" ||
      typeof row.mode !== "string" ||
      typeof row.updatedAt !== "string"
    )
      return yield* fail(projectId, "projection-corrupt");
    return row;
  });

  const recoverActivationEvent = Effect.fn("AgentControlArmed.recoverActivationEvent")(function* (
    projectId: ProjectId,
    dispatch: AgentControlArmedDispatch,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT event_id AS "eventId", sequence, stream_version AS "streamVersion",
        occurred_at AS "occurredAt"
      FROM main.agent_control_events
      WHERE aggregate_kind = 'project-controller' AND stream_id = ${projectId}
        AND command_id = ${dispatch.commandId}
        AND event_type = 'agentControl.project.mode.changed'
        AND actor_authority = 'system'
    `;
    if (rows.length === 0) return null;
    const row = rows[0];
    if (
      rows.length !== 1 ||
      row === undefined ||
      typeof row.eventId !== "string" ||
      typeof row.sequence !== "number" ||
      typeof row.streamVersion !== "number" ||
      typeof row.occurredAt !== "string"
    )
      return yield* fail(projectId, "authority-conflict");
    return {
      eventId: row.eventId,
      sequence: row.sequence,
      streamVersion: row.streamVersion,
      occurredAt: row.occurredAt,
    };
  });

  const activate = Effect.fn("AgentControlArmed.activate")(function* (
    projectId: ProjectId,
    dispatch: AgentControlArmedDispatch,
  ) {
    let event = yield* recoverActivationEvent(projectId, dispatch);
    if (event === null) {
      const dispatched = yield* Effect.exit(
        projectEngine.dispatchSystem({
          commandId: dispatch.commandId,
          projectId,
          expectedRevision: dispatch.projectRevision,
          mode: "run-once",
        }),
      );
      if (Exit.isFailure(dispatched)) {
        const project = yield* projectEngine
          .getProjectState({ projectId })
          .pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
        if (project.mode !== "armed") {
          const state = yield* loadState(projectId, dispatch.dispatchId);
          if (state.status === "claimed") {
            yield* finishArmedDispatch(
              sql,
              dispatch,
              "superseded",
              project.updatedAt ?? dispatch.claimedAt,
            );
          }
          return false;
        }
        const state = yield* loadState(projectId, dispatch.dispatchId);
        if (state.status === "superseded") return false;
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        if (
          state.status === "claimed" &&
          now >= DateTime.toEpochMillis(DateTime.makeUnsafe(dispatch.expiresAt))
        ) {
          return yield* fail(projectId, "persistence", dispatched.cause, dispatch.expiresAt);
        }
        // Source/reconcile/task authority can advance on another native
        // connection after the claim transaction. Migration 064 rejects the
        // stale system event; retry only after the immutable claim expires so
        // a fresh epoch and fence are selected without spinning.
        return yield* fail(
          projectId,
          "source-watermark-stale",
          dispatched.cause,
          dispatch.expiresAt,
        );
      }
      event = yield* recoverActivationEvent(projectId, dispatch);
      if (event === null) return yield* fail(projectId, "authority-conflict");
      if (options.hooks?.afterModeEvent !== undefined)
        yield* options.hooks.afterModeEvent(dispatch);
    }
    yield* activateArmedDispatch(sql, dispatch, event);
    if (options.hooks?.afterActivation !== undefined)
      yield* options.hooks.afterActivation(dispatch);
    return true;
  });

  const processProject: AgentControlArmedSchedulerShape["processProject"] = (projectId) =>
    Effect.gen(function* () {
      const dispatch = yield* withAgentControlRunOnceProjectFence(
        projectId,
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const claimedAt = DateTime.formatIso(now);
          const expiresAt = DateTime.formatIso(
            DateTime.add(now, { milliseconds: claimDurationMs }),
          );
          const claimed = yield* claimArmedDispatch(sql, {
            projectId,
            ownerId,
            claimedAt,
            expiresAt,
          });
          if (claimed._tag === "inactive" || claimed._tag === "no-candidate") return null;
          if (claimed._tag === "busy") {
            return yield* fail(projectId, "persistence", undefined, claimed.retryAt);
          }
          if (options.hooks?.afterClaim !== undefined)
            yield* options.hooks.afterClaim(claimed.dispatch);
          const state = yield* loadState(projectId, claimed.dispatch.dispatchId);
          if (state.status === "claimed" && !(yield* activate(projectId, claimed.dispatch))) {
            return null;
          }
          return claimed.dispatch;
        }),
      );
      if (dispatch === null) return;
      if (options.hooks?.beforeRunOnce !== undefined) yield* options.hooks.beforeRunOnce(dispatch);
      const preflight = yield* loadState(projectId, dispatch.dispatchId);
      if (preflight.status !== "activated") return;
      // A crash can occur after Run-Once durably reset an Armed-origin run but
      // before the dispatch publication was terminalized. That is successful
      // completion, never a Human supersession.
      if (preflight.mode === "armed" && preflight.pausedFromMode === null) {
        yield* finishArmedDispatch(sql, dispatch, "completed", preflight.updatedAt as string);
        return;
      }
      if (preflight.mode !== "run-once" || preflight.pausedFromMode !== null) {
        yield* finishArmedDispatch(sql, dispatch, "superseded", preflight.updatedAt as string);
        return;
      }
      yield* runOnce
        .processProject(projectId)
        .pipe(
          Effect.mapError((cause) =>
            fail(
              projectId,
              cause.reason === "persistence" || cause.reason === "source-unavailable"
                ? "persistence"
                : "authority-conflict",
              cause,
            ),
          ),
        );
      if (options.hooks?.afterRunOnce !== undefined) yield* options.hooks.afterRunOnce(dispatch);
      const state = yield* loadState(projectId, dispatch.dispatchId);
      if (state.status !== "activated") return;
      if (state.mode === "armed" && state.pausedFromMode === null) {
        yield* finishArmedDispatch(sql, dispatch, "completed", state.updatedAt as string);
        return;
      }
      if (state.mode !== "run-once") {
        yield* finishArmedDispatch(sql, dispatch, "superseded", state.updatedAt as string);
      }
    }).pipe(
      Effect.mapError((cause) =>
        isArmedError(cause) ? cause : fail(projectId, "persistence", cause),
      ),
    );

  const recover: AgentControlArmedSchedulerShape["recover"] = Effect.gen(function* () {
    const [foreignKeys, integrity] = yield* Effect.all([
      sql<Record<string, unknown>>`PRAGMA main.foreign_key_check`,
      sql<{ readonly integrity_check: unknown }>`PRAGMA main.integrity_check`,
    ]).pipe(Effect.mapError((cause) => fail("armed-recovery" as ProjectId, "persistence", cause)));
    if (
      foreignKeys.length !== 0 ||
      integrity.length !== 1 ||
      integrity[0]?.integrity_check !== "ok"
    )
      return yield* fail("armed-recovery" as ProjectId, "projection-corrupt");
    const projects = yield* loadArmedCatchUpProjectIds(sql).pipe(
      Effect.mapError((cause) =>
        isArmedError(cause) ? cause : fail("armed-recovery" as ProjectId, "persistence", cause),
      ),
    );
    yield* Effect.forEach(projects, processProject, {
      concurrency: PROJECT_WORKERS,
      discard: true,
    });
  });

  const prepare: AgentControlArmedSchedulerShape["prepare"] = (activation) =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>();
      const schedule = yield* makeAgentControlArmedWorkScheduler(
        processProject,
        Deferred.await(ready).pipe(Effect.andThen(activation.await)),
        retryLimit,
        reportFailure,
      );
      const catchUp = loadArmedCatchUpProjectIds(sql).pipe(
        Effect.flatMap((projects) => Effect.forEach(projects, schedule, { discard: true })),
        Effect.mapError((cause) =>
          isArmedError(cause) ? cause : fail("armed-recovery" as ProjectId, "persistence", cause),
        ),
      );
      if (
        projectEngine.subscribeDomainEvents === undefined ||
        taskIntake.subscribeCompletions === undefined
      ) {
        return yield* fail("armed-recovery" as ProjectId, "source-unavailable");
      }
      yield* superviseAgentControlRunOnceListener(
        projectEngine.subscribeDomainEvents,
        catchUp,
        (event) =>
          event.type === "agentControl.project.mode.changed" && event.payload.mode === "armed"
            ? schedule(event.aggregateId)
            : Effect.void,
        "project",
      );
      yield* superviseAgentControlRunOnceListener(
        taskEngine.subscribeDomainEvents,
        catchUp,
        (event) =>
          schedule(
            "projectId" in event.payload ? event.payload.projectId : event.payload.source.projectId,
          ),
        "task",
      );
      yield* superviseAgentControlRunOnceListener(
        taskIntake.subscribeCompletions,
        catchUp,
        schedule,
        "task",
      );
      yield* recover;
      yield* Deferred.succeed(ready, undefined);
    });

  return AgentControlArmedScheduler.of({
    awaitFailure: Deferred.await(runtimeFailure),
    recover,
    processProject,
    prepare,
  });
});

export const layer = Layer.effect(AgentControlArmedScheduler, make());
