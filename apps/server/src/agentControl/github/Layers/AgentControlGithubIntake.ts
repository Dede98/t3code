import {
  AgentControlGithubClearTrackerConfigInput,
  type AgentControlGithubCommandResult,
  type AgentControlGithubEventDraft,
  AgentControlGithubPollOnceInput,
  AgentControlGithubProjectInput,
  AgentControlGithubRpcError,
  AgentControlGithubSetTrackerConfigInput,
  type AgentControlGithubTrackerSettings,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { type AgentControlCommandAuthority } from "../../AgentControlCommandAuthority.ts";
import { RepositoryIdentityResolver } from "../../../project/RepositoryIdentityResolver.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
  AgentControlStreamVersionConflictError,
} from "../../Errors.ts";
import { createDefaultGithubIntakeState, projectGithubIntakeEvent } from "../projector.ts";
import {
  AgentControlGithubIntake,
  type AgentControlGithubIntakeShape,
} from "../Services/AgentControlGithubIntake.ts";
import { AgentControlGithubEventStore } from "../Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubProjection } from "../Services/AgentControlGithubProjection.ts";
import { AgentControlGithubStateRepository } from "../Services/AgentControlGithubStateRepository.ts";
import {
  GithubIssueTrackerClient,
  type GithubIssueTrackerClientError,
} from "../Services/GithubIssueTrackerClient.ts";

type Operation = AgentControlGithubRpcError["operation"];
const CURSOR_OVERLAP_SECONDS = 120;
const MAX_TRUSTED_LOGINS = 100;
const decodeProjectInput = Schema.decodeUnknownEffect(AgentControlGithubProjectInput);
const decodeSetInput = Schema.decodeUnknownEffect(AgentControlGithubSetTrackerConfigInput);
const decodeClearInput = Schema.decodeUnknownEffect(AgentControlGithubClearTrackerConfigInput);
const decodePollInput = Schema.decodeUnknownEffect(AgentControlGithubPollOnceInput);
const isPersistenceSqlError = Schema.is(AgentControlPersistenceSqlError);
const isPersistenceDecodeError = Schema.is(AgentControlPersistenceDecodeError);
const isStreamConflict = Schema.is(AgentControlStreamVersionConflictError);

const rpcError = (
  code: AgentControlGithubRpcError["code"],
  operation: Operation,
  projectId: ProjectId,
) => new AgentControlGithubRpcError({ code, operation, projectId });

const mapPersistence = (operation: Operation, projectId: ProjectId, error: unknown) => {
  if (isStreamConflict(error)) return rpcError("revision-conflict", operation, projectId);
  if (isPersistenceSqlError(error) || isPersistenceDecodeError(error)) {
    return rpcError("internal-persistence-error", operation, projectId);
  }
  return rpcError("internal-persistence-error", operation, projectId);
};

const normalizeLogin = (login: string) => login.trim().toLocaleLowerCase("en-US");
const sameSettings = (
  left: AgentControlGithubTrackerSettings,
  right: AgentControlGithubTrackerSettings,
) =>
  left.trackerKind === right.trackerKind &&
  left.readyLabel === right.readyLabel &&
  left.pausedLabel === right.pausedLabel &&
  left.pollIntervalSeconds === right.pollIntervalSeconds &&
  left.trustedLogins.length === right.trustedLogins.length &&
  left.trustedLogins.every((login, index) => login === right.trustedLogins[index]);
const sameRepository = (
  left: { readonly repositoryNodeId: string; readonly nameWithOwner: string },
  right: { readonly repositoryNodeId: string; readonly nameWithOwner: string },
) =>
  left.repositoryNodeId === right.repositoryNodeId &&
  left.nameWithOwner.toLocaleLowerCase("en-US") === right.nameWithOwner.toLocaleLowerCase("en-US");

const makeIntake = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlGithubEventStore;
  const projection = yield* AgentControlGithubProjection;
  const states = yield* AgentControlGithubStateRepository;
  const repositoryIdentityResolver = yield* RepositoryIdentityResolver;
  const github = yield* GithubIssueTrackerClient;
  const activePolls = new Set<string>();

  const decode = <A, I>(
    decoder: (input: I) => Effect.Effect<A, Schema.SchemaError>,
    raw: I,
    operation: Operation,
    fallbackProjectId: ProjectId,
  ) =>
    decoder(raw).pipe(Effect.mapError(() => rpcError("validation", operation, fallbackProjectId)));

  const ensureProject = Effect.fn("AgentControlGithubIntake.ensureProject")(function* (
    projectId: ProjectId,
    operation: Operation,
  ) {
    const rows = yield* sql<{ readonly workspaceRoot: unknown; readonly deletedAt: unknown }>`
      SELECT workspace_root AS "workspaceRoot", deleted_at AS "deletedAt"
      FROM projection_projects
      WHERE project_id = ${projectId}
    `.pipe(Effect.mapError(() => rpcError("internal-persistence-error", operation, projectId)));
    const project = rows[0];
    if (project === undefined) return yield* rpcError("project-missing", operation, projectId);
    if (project.deletedAt !== null) return yield* rpcError("project-deleted", operation, projectId);
    if (typeof project.workspaceRoot !== "string" || project.workspaceRoot.trim().length === 0) {
      return yield* rpcError("internal-persistence-error", operation, projectId);
    }
    return project.workspaceRoot;
  });

  const getState = Effect.fn("AgentControlGithubIntake.getState")(function* (
    projectId: ProjectId,
    operation: Operation,
  ) {
    const state = yield* states
      .get(projectId)
      .pipe(Effect.mapError((error) => mapPersistence(operation, projectId, error)));
    return Option.getOrElse(state, () => createDefaultGithubIntakeState(projectId));
  });

  const commandFingerprint = Effect.fn("AgentControlGithubIntake.commandFingerprint")(function* (
    parts: ReadonlyArray<string>,
    operation: Operation,
    projectId: ProjectId,
  ) {
    const canonical = parts.map((part) => `${part.length}:${part}`).join("");
    return yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical)).pipe(
      Effect.map(Encoding.encodeHex),
      Effect.mapError(() => rpcError("internal-persistence-error", operation, projectId)),
    );
  });

  const loadStateAtRevision = Effect.fn("AgentControlGithubIntake.loadStateAtRevision")(function* (
    projectId: ProjectId,
    revision: number,
    operation: Operation,
  ) {
    let state = createDefaultGithubIntakeState(projectId);
    while (state.revision < revision) {
      const page = yield* events
        .readStream(projectId, state.revision, Math.min(500, revision - state.revision))
        .pipe(Effect.mapError((error) => mapPersistence(operation, projectId, error)));
      if (page.length === 0) {
        return yield* rpcError("internal-persistence-error", operation, projectId);
      }
      for (const event of page) {
        state = yield* projectGithubIntakeEvent(state, event).pipe(
          Effect.mapError(() => rpcError("internal-persistence-error", operation, projectId)),
        );
      }
    }
    return state;
  });

  const replayReceipt = Effect.fn("AgentControlGithubIntake.replayReceipt")(function* (input: {
    readonly commandId: CommandId;
    readonly fingerprint: string;
    readonly authority: AgentControlCommandAuthority;
    readonly projectId: ProjectId;
    readonly operation: Operation;
  }) {
    const receipt = yield* receipts
      .getByCommandId(input.commandId)
      .pipe(Effect.mapError((error) => mapPersistence(input.operation, input.projectId, error)));
    if (Option.isNone(receipt)) return Option.none<AgentControlGithubCommandResult>();
    const value = receipt.value;
    if (
      value.aggregateKind !== "github-intake" ||
      value.aggregateId !== input.projectId ||
      value.commandFingerprint !== input.fingerprint ||
      value.authority !== input.authority
    ) {
      return yield* rpcError("command-identity-mismatch", input.operation, input.projectId);
    }
    if (value.status === "rejected") {
      return yield* rpcError("command-previously-rejected", input.operation, input.projectId);
    }
    const state = yield* loadStateAtRevision(
      input.projectId,
      value.resultStreamVersion,
      input.operation,
    );
    return Option.some({
      state,
      resultSequence: value.resultSequence,
      eventCreated: value.eventCreated,
    });
  });

  const resolveRepository = Effect.fn("AgentControlGithubIntake.resolveRepository")(function* (
    projectId: ProjectId,
    workspaceRoot: string,
    operation: Operation,
  ) {
    const identity = yield* repositoryIdentityResolver.resolve(workspaceRoot);
    if (
      identity === null ||
      identity.provider !== "github" ||
      identity.owner === undefined ||
      identity.name === undefined
    ) {
      return yield* rpcError("repository-not-github", operation, projectId);
    }
    return {
      locator: { owner: identity.owner, name: identity.name },
      localNameWithOwner: `${identity.owner}/${identity.name}`,
    };
  });

  const makeEventId = (operation: Operation, projectId: ProjectId) =>
    crypto.randomUUIDv4.pipe(
      Effect.map(EventId.make),
      Effect.mapError(() => rpcError("internal-persistence-error", operation, projectId)),
    );

  const commitAccepted = Effect.fn("AgentControlGithubIntake.commitAccepted")(function* (input: {
    readonly commandId: CommandId;
    readonly fingerprint: string;
    readonly authority: AgentControlCommandAuthority;
    readonly projectId: ProjectId;
    readonly operation: Operation;
    readonly expectedRevision: number;
    readonly occurredAt: string;
    readonly draft: AgentControlGithubEventDraft | null;
  }) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const replay = yield* replayReceipt(input);
          if (Option.isSome(replay)) return replay.value;
          const current = yield* getState(input.projectId, input.operation);
          if (current.revision !== input.expectedRevision) {
            return yield* rpcError("revision-conflict", input.operation, input.projectId);
          }
          const persisted =
            input.draft === null
              ? []
              : yield* events
                  .append({
                    projectId: input.projectId,
                    expectedStreamVersion: current.revision,
                    events: [input.draft],
                  })
                  .pipe(
                    Effect.mapError((error) =>
                      mapPersistence(input.operation, input.projectId, error),
                    ),
                  );
          let next = current;
          for (const event of persisted) {
            yield* projection
              .projectEvent(event)
              .pipe(
                Effect.mapError((error) => mapPersistence(input.operation, input.projectId, error)),
              );
            next = yield* projectGithubIntakeEvent(next, event).pipe(
              Effect.mapError(() =>
                rpcError("internal-persistence-error", input.operation, input.projectId),
              ),
            );
          }
          yield* receipts
            .insert({
              commandId: input.commandId,
              commandFingerprint: input.fingerprint,
              authority: input.authority,
              aggregateKind: "github-intake",
              aggregateId: input.projectId,
              status: "accepted",
              resultSequence: next.sequence,
              resultStreamVersion: next.revision,
              eventCreated: persisted.length > 0,
              acceptedAt: input.occurredAt,
              errorCode: null,
            })
            .pipe(
              Effect.mapError((error) => mapPersistence(input.operation, input.projectId, error)),
            );
          return {
            state: next,
            resultSequence: next.sequence,
            eventCreated: persisted.length > 0,
          } satisfies AgentControlGithubCommandResult;
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", () =>
          Effect.fail(rpcError("internal-persistence-error", input.operation, input.projectId)),
        ),
      );
  });

  const getObserveState: AgentControlGithubIntakeShape["getObserveState"] = (rawInput) =>
    Effect.gen(function* () {
      const fallback = ProjectId.make(String(rawInput.projectId ?? "invalid"));
      const input = yield* decode(decodeProjectInput, rawInput, "get-observe-state", fallback);
      yield* ensureProject(input.projectId, "get-observe-state");
      return yield* getState(input.projectId, "get-observe-state");
    });

  const getTrackerConfig: AgentControlGithubIntakeShape["getTrackerConfig"] = (input) =>
    getObserveState(input).pipe(Effect.map((state) => state.config));

  const listObservedIssues: AgentControlGithubIntakeShape["listObservedIssues"] = (rawInput) =>
    Effect.gen(function* () {
      const fallback = ProjectId.make(String(rawInput.projectId ?? "invalid"));
      const input = yield* decode(decodeProjectInput, rawInput, "list-observed-issues", fallback);
      yield* ensureProject(input.projectId, "list-observed-issues");
      const issues = yield* states
        .listIssues(input.projectId)
        .pipe(
          Effect.mapError((error) =>
            mapPersistence("list-observed-issues", input.projectId, error),
          ),
        );
      return {
        projectId: input.projectId,
        issues: issues.map(
          ({
            body: _body,
            repositoryNodeId: _repositoryNodeId,
            timelineComplete: _timelineComplete,
            timelineEvents: _timelineEvents,
            ...summary
          }) => summary,
        ),
      };
    });

  const setTrackerConfig: AgentControlGithubIntakeShape["setTrackerConfig"] = (rawInput) =>
    Effect.gen(function* () {
      const fallback = ProjectId.make(String(rawInput.projectId ?? "invalid"));
      const input = yield* decode(decodeSetInput, rawInput, "set-tracker-config", fallback);
      const normalizedLogins = input.trustedLogins.map(normalizeLogin).toSorted();
      if (
        normalizedLogins.length > MAX_TRUSTED_LOGINS ||
        new Set(normalizedLogins).size !== normalizedLogins.length ||
        input.readyLabel.toLocaleLowerCase("en-US") === input.pausedLabel.toLocaleLowerCase("en-US")
      ) {
        return yield* rpcError("validation", "set-tracker-config", input.projectId);
      }
      const settings: AgentControlGithubTrackerSettings = {
        trackerKind: "github",
        readyLabel: input.readyLabel,
        pausedLabel: input.pausedLabel,
        trustedLogins: normalizedLogins,
        pollIntervalSeconds: input.pollIntervalSeconds,
      };
      const fingerprint = yield* commandFingerprint(
        [
          "agentControl.github.config.set",
          input.commandId,
          input.projectId,
          String(input.expectedRevision),
          settings.readyLabel,
          settings.pausedLabel,
          ...settings.trustedLogins,
          String(settings.pollIntervalSeconds),
        ],
        "set-tracker-config",
        input.projectId,
      );
      const workspaceRoot = yield* ensureProject(input.projectId, "set-tracker-config");
      const replay = yield* replayReceipt({
        commandId: input.commandId,
        fingerprint,
        authority: "human",
        projectId: input.projectId,
        operation: "set-tracker-config",
      });
      if (Option.isSome(replay)) return replay.value;
      const current = yield* getState(input.projectId, "set-tracker-config");
      if (current.revision !== input.expectedRevision) {
        return yield* rpcError("revision-conflict", "set-tracker-config", input.projectId);
      }
      const local = yield* resolveRepository(input.projectId, workspaceRoot, "set-tracker-config");
      const repository = yield* github
        .resolveRepository({
          cwd: workspaceRoot,
          locator: local.locator,
        })
        .pipe(
          Effect.mapError((error) => rpcError(error.code, "set-tracker-config", input.projectId)),
        );
      if (
        repository.nameWithOwner.toLocaleLowerCase("en-US") !==
          local.localNameWithOwner.toLocaleLowerCase("en-US") ||
        (current.config !== null && !sameRepository(current.config.repository, repository))
      ) {
        return yield* rpcError(
          "repository-identity-conflict",
          "set-tracker-config",
          input.projectId,
        );
      }
      const occurredAt = DateTime.formatIso(yield* DateTime.now);
      const eventId = yield* makeEventId("set-tracker-config", input.projectId);
      const unchanged =
        current.config !== null &&
        sameRepository(current.config.repository, repository) &&
        sameSettings(current.config.settings, settings);
      return yield* commitAccepted({
        commandId: input.commandId,
        fingerprint,
        authority: "human",
        projectId: input.projectId,
        operation: "set-tracker-config",
        expectedRevision: input.expectedRevision,
        occurredAt,
        draft: unchanged
          ? null
          : {
              eventId,
              type: "agentControl.github.config.set",
              aggregateKind: "github-intake",
              aggregateId: input.projectId,
              occurredAt,
              commandId: input.commandId,
              causationEventId: null,
              correlationId: input.commandId,
              authority: "human",
              payload: {
                projectId: input.projectId,
                settings,
                repository,
                configuredAt: occurredAt,
              },
              metadata: { schemaVersion: 1 },
            },
      });
    });

  const clearTrackerConfig: AgentControlGithubIntakeShape["clearTrackerConfig"] = (rawInput) =>
    Effect.gen(function* () {
      const fallback = ProjectId.make(String(rawInput.projectId ?? "invalid"));
      const input = yield* decode(decodeClearInput, rawInput, "clear-tracker-config", fallback);
      const fingerprint = yield* commandFingerprint(
        [
          "agentControl.github.config.clear",
          input.commandId,
          input.projectId,
          String(input.expectedRevision),
        ],
        "clear-tracker-config",
        input.projectId,
      );
      yield* ensureProject(input.projectId, "clear-tracker-config");
      const replay = yield* replayReceipt({
        commandId: input.commandId,
        fingerprint,
        authority: "human",
        projectId: input.projectId,
        operation: "clear-tracker-config",
      });
      if (Option.isSome(replay)) return replay.value;
      const current = yield* getState(input.projectId, "clear-tracker-config");
      if (current.revision !== input.expectedRevision) {
        return yield* rpcError("revision-conflict", "clear-tracker-config", input.projectId);
      }
      const occurredAt = DateTime.formatIso(yield* DateTime.now);
      const eventId = yield* makeEventId("clear-tracker-config", input.projectId);
      return yield* commitAccepted({
        commandId: input.commandId,
        fingerprint,
        authority: "human",
        projectId: input.projectId,
        operation: "clear-tracker-config",
        expectedRevision: input.expectedRevision,
        occurredAt,
        draft:
          current.config === null
            ? null
            : {
                eventId,
                type: "agentControl.github.config.cleared",
                aggregateKind: "github-intake",
                aggregateId: input.projectId,
                occurredAt,
                commandId: input.commandId,
                causationEventId: null,
                correlationId: input.commandId,
                authority: "human",
                payload: { projectId: input.projectId, clearedAt: occurredAt },
                metadata: { schemaVersion: 1 },
              },
      });
    });

  const pollOnce: AgentControlGithubIntakeShape["pollOnce"] = (rawInput) =>
    Effect.gen(function* () {
      const fallback = ProjectId.make(String(rawInput.projectId ?? "invalid"));
      const input = yield* decode(decodePollInput, rawInput, "poll-once", fallback);
      const fingerprint = yield* commandFingerprint(
        [
          "agentControl.github.poll.once",
          input.commandId,
          input.projectId,
          String(input.expectedRevision),
        ],
        "poll-once",
        input.projectId,
      );
      const workspaceRoot = yield* ensureProject(input.projectId, "poll-once");
      const replay = yield* replayReceipt({
        commandId: input.commandId,
        fingerprint,
        authority: "controller",
        projectId: input.projectId,
        operation: "poll-once",
      });
      if (Option.isSome(replay)) return replay.value;
      const acquired = yield* Effect.sync(() => {
        if (activePolls.has(input.projectId)) return false;
        activePolls.add(input.projectId);
        return true;
      });
      if (!acquired) return yield* rpcError("poll-in-progress", "poll-once", input.projectId);

      return yield* Effect.gen(function* () {
        const current = yield* getState(input.projectId, "poll-once");
        if (current.revision !== input.expectedRevision) {
          return yield* rpcError("revision-conflict", "poll-once", input.projectId);
        }
        if (current.config === null) {
          return yield* rpcError("tracker-not-configured", "poll-once", input.projectId);
        }
        const attemptedAt = DateTime.formatIso(yield* DateTime.now);
        const localResult = yield* Effect.result(
          resolveRepository(input.projectId, workspaceRoot, "poll-once"),
        );
        let clientError: GithubIssueTrackerClientError | null = null;
        let identityInvalid = false;
        let polled: {
          readonly repository: typeof current.config.repository;
          readonly issues: ReadonlyArray<
            import("@t3tools/contracts").AgentControlGithubIssueSnapshot
          >;
        } | null = null;
        if (localResult._tag === "Failure") {
          identityInvalid = true;
        } else if (
          localResult.success.localNameWithOwner.toLocaleLowerCase("en-US") !==
          current.config.repository.nameWithOwner.toLocaleLowerCase("en-US")
        ) {
          identityInvalid = true;
        } else {
          const knownIssues = yield* states
            .listIssues(input.projectId)
            .pipe(Effect.mapError((error) => mapPersistence("poll-once", input.projectId, error)));
          const since =
            current.cursor === null
              ? null
              : DateTime.formatIso(
                  DateTime.subtract(DateTime.makeUnsafe(current.cursor.lastSuccessfulPollAt), {
                    seconds: CURSOR_OVERLAP_SECONDS,
                  }),
                );
          const result = yield* Effect.result(
            github.pollIssues({
              cwd: workspaceRoot,
              locator: localResult.success.locator,
              expectedRepository: current.config.repository,
              settings: current.config.settings,
              knownIssues,
              since,
            }),
          );
          if (result._tag === "Failure") clientError = result.failure;
          else polled = result.success;
        }
        const completedAt = DateTime.formatIso(yield* DateTime.now);
        const eventId = yield* makeEventId("poll-once", input.projectId);
        const errorCode = identityInvalid
          ? ("repository-identity-changed" as const)
          : clientError?.code;
        return yield* commitAccepted({
          commandId: input.commandId,
          fingerprint,
          authority: "controller",
          projectId: input.projectId,
          operation: "poll-once",
          expectedRevision: input.expectedRevision,
          occurredAt: completedAt,
          draft:
            errorCode !== undefined
              ? {
                  eventId,
                  type: "agentControl.github.poll.failed",
                  aggregateKind: "github-intake",
                  aggregateId: input.projectId,
                  occurredAt: completedAt,
                  commandId: input.commandId,
                  causationEventId: null,
                  correlationId: input.commandId,
                  authority: "controller",
                  payload: {
                    projectId: input.projectId,
                    attemptedAt,
                    completedAt,
                    errorCode,
                    invalidateCursor:
                      errorCode === "repository-identity-changed" ||
                      errorCode === "issue-repository-changed",
                  },
                  metadata: { schemaVersion: 1 },
                }
              : {
                  eventId,
                  type: "agentControl.github.poll.succeeded",
                  aggregateKind: "github-intake",
                  aggregateId: input.projectId,
                  occurredAt: completedAt,
                  commandId: input.commandId,
                  causationEventId: null,
                  correlationId: input.commandId,
                  authority: "controller",
                  payload: {
                    projectId: input.projectId,
                    repository: polled!.repository,
                    attemptedAt,
                    completedAt,
                    cursor: {
                      // The cursor is the lower high-water mark captured before
                      // listing began. It is committed only after every issue
                      // and timeline page succeeds, preventing long polls from
                      // opening a gap before the next overlap window.
                      lastSuccessfulPollAt: attemptedAt,
                      overlapSeconds: CURSOR_OVERLAP_SECONDS,
                    },
                    issues: polled!.issues,
                  },
                  metadata: { schemaVersion: 1 },
                },
        });
      }).pipe(Effect.ensuring(Effect.sync(() => activePolls.delete(input.projectId))));
    });

  return AgentControlGithubIntake.of({
    getTrackerConfig,
    setTrackerConfig,
    clearTrackerConfig,
    getObserveState,
    listObservedIssues,
    pollOnce,
  });
});

export const layer = Layer.effect(AgentControlGithubIntake, makeIntake);
