import type { SessionKey, SessionStore, SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { writeFileStringAtomically } from "../../atomicWrite.ts";
import {
  type ClaudeSessionSnapshot,
  type ClaudeSessionStoreShape,
  ClaudeSessionStoreError,
} from "../Services/ClaudeSessionStore.ts";

const MAIN_TRANSCRIPT_SUFFIX = ".jsonl";
const AGENT_METADATA_TYPE = "agent_metadata";
const MAIN_TRANSCRIPT_STATE_TYPES = ["last-prompt", "mode"] as const;
const MAIN_TRANSCRIPT_STATE_TYPE_SET = new Set<string>(MAIN_TRANSCRIPT_STATE_TYPES);
const DEFAULT_READINESS_TIMEOUT_MS = 5_000;
const READINESS_POLL_INTERVAL_MS = 50;
const encodeUnknownJsonString = Schema.encodeSync(Schema.UnknownFromJsonString);
const decodeUnknownJsonString = Schema.decodeUnknownEffect(Schema.UnknownFromJsonString);
const isClaudeSessionStoreError = Schema.is(ClaudeSessionStoreError);

export interface ClaudeNativeResumeStoreOptions {
  readonly sessionId: string;
  readonly targetConfigDirPath: string;
  readonly expectedAssistantUuid?: string | undefined;
  readonly readinessTimeoutMs?: number | undefined;
}

export interface ClaudeNativeResumeStoreDependencies {
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
}

export interface ClaudeNativeSessionImportOptions {
  readonly sessionId: string;
  readonly sourceConfigDirPath: string;
  readonly projectKey: string;
  readonly expectedAssistantUuid?: string | undefined;
}

function storeError(operation: string, detail: string, cause?: unknown): ClaudeSessionStoreError {
  return new ClaudeSessionStoreError({
    operation,
    detail,
    ...(cause === undefined ? {} : { cause }),
  });
}

function isSessionStoreEntry(value: unknown): value is SessionStoreEntry {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "type" in value &&
    typeof value.type === "string"
  );
}

function encodeJsonLines(entries: ReadonlyArray<SessionStoreEntry>): string {
  return entries.length === 0
    ? ""
    : `${entries.map((entry) => encodeUnknownJsonString(entry)).join("\n")}\n`;
}

function hasExpectedCheckpoint(
  snapshot: ClaudeSessionSnapshot,
  expectedAssistantUuid: string | undefined,
): boolean {
  if (expectedAssistantUuid === undefined) return snapshot.entries.length > 0;
  return snapshot.entries.some((entry) => entry.uuid === expectedAssistantUuid);
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Claude can normalize assistant accounting after mirroring an entry, while
 * retaining the same UUID and message content. These fields do not participate
 * in resume history. Everything else, including content and tool calls, remains
 * part of the structural comparison.
 */
function transcriptEntriesAreEquivalent(
  left: SessionStoreEntry | undefined,
  right: SessionStoreEntry | undefined,
): boolean {
  if (Equal.equals(left, right)) return true;
  if (
    left === undefined ||
    right === undefined ||
    left.type !== "assistant" ||
    right.type !== "assistant" ||
    typeof left.uuid !== "string" ||
    left.uuid !== right.uuid ||
    !isJsonObject(left.message) ||
    !isJsonObject(right.message)
  ) {
    return false;
  }

  const { stop_reason: _leftStopReason, usage: _leftUsage, ...leftMessage } = left.message;
  const { stop_reason: _rightStopReason, usage: _rightUsage, ...rightMessage } = right.message;
  return Equal.equals({ ...left, message: leftMessage }, { ...right, message: rightMessage });
}

function entriesArePrefix(
  prefix: ReadonlyArray<SessionStoreEntry>,
  entries: ReadonlyArray<SessionStoreEntry>,
): boolean {
  return (
    prefix.length <= entries.length &&
    prefix.every((entry, index) => transcriptEntriesAreEquivalent(entry, entries[index]))
  );
}

function splitMainTranscriptEntries(entries: ReadonlyArray<SessionStoreEntry>): {
  readonly orderedEntries: ReadonlyArray<SessionStoreEntry>;
  readonly latestStateEntries: ReadonlyMap<string, SessionStoreEntry>;
} {
  const orderedEntries: Array<SessionStoreEntry> = [];
  const latestStateEntries = new Map<string, SessionStoreEntry>();
  for (const entry of entries) {
    if (MAIN_TRANSCRIPT_STATE_TYPE_SET.has(entry.type)) {
      latestStateEntries.set(entry.type, entry);
    } else {
      orderedEntries.push(entry);
    }
  }
  return { orderedEntries, latestStateEntries };
}

/**
 * Claude rewrites or drops older main-transcript state snapshots while keeping
 * the ordered conversation intact. Compare the effective (latest) state only
 * when both sides have the same ordered history. Once the ordered history is a
 * strict prefix, that ordering is sufficient to prove which snapshot is newer.
 */
function mainEntriesArePrefix(
  prefix: ReadonlyArray<SessionStoreEntry>,
  entries: ReadonlyArray<SessionStoreEntry>,
): boolean {
  const prefixParts = splitMainTranscriptEntries(prefix);
  const entryParts = splitMainTranscriptEntries(entries);
  if (!entriesArePrefix(prefixParts.orderedEntries, entryParts.orderedEntries)) return false;
  if (prefixParts.orderedEntries.length < entryParts.orderedEntries.length) return true;

  return MAIN_TRANSCRIPT_STATE_TYPES.every((type) => {
    const prefixState = prefixParts.latestStateEntries.get(type);
    if (prefixState === undefined) return true;
    const state = entryParts.latestStateEntries.get(type);
    return state !== undefined && Equal.equals(prefixState, state);
  });
}

function subkeyEntriesArePrefix(
  prefix: ReadonlyArray<SessionStoreEntry>,
  entries: ReadonlyArray<SessionStoreEntry>,
): boolean {
  const prefixTranscript = prefix.filter((entry) => entry.type !== AGENT_METADATA_TYPE);
  const transcript = entries.filter((entry) => entry.type !== AGENT_METADATA_TYPE);
  if (!entriesArePrefix(prefixTranscript, transcript)) return false;

  const prefixMetadata = prefix.findLast((entry) => entry.type === AGENT_METADATA_TYPE);
  if (prefixMetadata === undefined) return true;
  const metadata = entries.findLast((entry) => entry.type === AGENT_METADATA_TYPE);
  return metadata !== undefined && Equal.equals(prefixMetadata, metadata);
}

/**
 * Compare transcript content without treating the project key as history.
 * Subkeys are keyed by their stable relative path; entry order within each
 * transcript remains significant.
 */
function snapshotIsPrefix(prefix: ClaudeSessionSnapshot, snapshot: ClaudeSessionSnapshot): boolean {
  if (
    prefix.sessionId !== snapshot.sessionId ||
    !mainEntriesArePrefix(prefix.entries, snapshot.entries)
  ) {
    return false;
  }

  const snapshotSubkeys = new Map(snapshot.subkeys.map((subkey) => [subkey.subpath, subkey]));
  return prefix.subkeys.every((subkey) => {
    const snapshotSubkey = snapshotSubkeys.get(subkey.subpath);
    return (
      snapshotSubkey !== undefined && subkeyEntriesArePrefix(subkey.entries, snapshotSubkey.entries)
    );
  });
}

function describeEntryMismatch(
  sourceEntries: ReadonlyArray<SessionStoreEntry>,
  storedEntries: ReadonlyArray<SessionStoreEntry>,
  label: string,
): string {
  const sharedLength = storedEntries.length;
  const commonLength = Math.min(sourceEntries.length, sharedLength);
  for (let index = 0; index < commonLength; index += 1) {
    const sourceEntry = sourceEntries[index];
    const storedEntry = storedEntries[index];
    if (!transcriptEntriesAreEquivalent(sourceEntry, storedEntry)) {
      const sourceType = sourceEntry === undefined ? "missing" : sourceEntry.type;
      const storedType = storedEntry === undefined ? "missing" : storedEntry.type;
      return `${label} entry ${index + 1} differs (source type ${encodeUnknownJsonString(sourceType)}, shared-store type ${encodeUnknownJsonString(storedType)}).`;
    }
  }
  return `${label} entry counts differ (source ${sourceEntries.length}, shared store ${sharedLength}).`;
}

function describeSnapshotDivergence(
  sourceSnapshot: ClaudeSessionSnapshot,
  storedSnapshot: ClaudeSessionSnapshot,
): string {
  const sourceMain = splitMainTranscriptEntries(sourceSnapshot.entries);
  const storedMain = splitMainTranscriptEntries(storedSnapshot.entries);
  if (
    !entriesArePrefix(sourceMain.orderedEntries, storedMain.orderedEntries) &&
    !entriesArePrefix(storedMain.orderedEntries, sourceMain.orderedEntries)
  ) {
    return describeEntryMismatch(
      sourceMain.orderedEntries,
      storedMain.orderedEntries,
      "Main ordered transcript",
    );
  }
  if (sourceMain.orderedEntries.length === storedMain.orderedEntries.length) {
    for (const type of MAIN_TRANSCRIPT_STATE_TYPES) {
      const sourceState = sourceMain.latestStateEntries.get(type);
      const storedState = storedMain.latestStateEntries.get(type);
      if (
        sourceState !== undefined &&
        storedState !== undefined &&
        !Equal.equals(sourceState, storedState)
      ) {
        return `Main effective ${encodeUnknownJsonString(type)} state differs.`;
      }
    }
  }

  const sourceSubkeys = new Map(sourceSnapshot.subkeys.map((subkey) => [subkey.subpath, subkey]));
  const storedSubkeys = new Map(storedSnapshot.subkeys.map((subkey) => [subkey.subpath, subkey]));
  const commonSubpaths = [...sourceSubkeys.keys()]
    .filter((subpath) => storedSubkeys.has(subpath))
    .sort();
  for (const subpath of commonSubpaths) {
    const sourceSubkey = sourceSubkeys.get(subpath);
    const storedSubkey = storedSubkeys.get(subpath);
    if (sourceSubkey === undefined || storedSubkey === undefined) continue;
    const sourceTranscript = sourceSubkey.entries.filter(
      (entry) => entry.type !== AGENT_METADATA_TYPE,
    );
    const storedTranscript = storedSubkey.entries.filter(
      (entry) => entry.type !== AGENT_METADATA_TYPE,
    );
    if (
      !entriesArePrefix(sourceTranscript, storedTranscript) &&
      !entriesArePrefix(storedTranscript, sourceTranscript)
    ) {
      return describeEntryMismatch(
        sourceTranscript,
        storedTranscript,
        `Subagent transcript ${encodeUnknownJsonString(subpath)}`,
      );
    }
    const sourceMetadata = sourceSubkey.entries.findLast(
      (entry) => entry.type === AGENT_METADATA_TYPE,
    );
    const storedMetadata = storedSubkey.entries.findLast(
      (entry) => entry.type === AGENT_METADATA_TYPE,
    );
    if (
      sourceMetadata !== undefined &&
      storedMetadata !== undefined &&
      !Equal.equals(sourceMetadata, storedMetadata)
    ) {
      return `Subagent metadata ${encodeUnknownJsonString(subpath)} differs.`;
    }
  }

  const sourceOnlySubpath = [...sourceSubkeys.keys()].sort().find((key) => !storedSubkeys.has(key));
  const storedOnlySubpath = [...storedSubkeys.keys()].sort().find((key) => !sourceSubkeys.has(key));
  if (sourceOnlySubpath !== undefined || storedOnlySubpath !== undefined) {
    return `Subagent transcript sets differ (source-only ${encodeUnknownJsonString(sourceOnlySubpath ?? "none")}, shared-store-only ${encodeUnknownJsonString(storedOnlySubpath ?? "none")}).`;
  }
  return "Newer history exists on both sides in different transcript components.";
}

function validateProjectKey(projectKey: string): void {
  if (!/^[a-zA-Z0-9-]+$/.test(projectKey)) {
    throw storeError(
      "nativeResume:validateProjectKey",
      `Refusing unsafe Claude project key '${projectKey}'.`,
    );
  }
}

function validateSubpath(subpath: string): void {
  if (
    subpath.length === 0 ||
    subpath.startsWith("/") ||
    subpath.startsWith("\\") ||
    /^[a-zA-Z]:[\\/]/.test(subpath) ||
    subpath.split(/[\\/]/).includes("..")
  ) {
    throw storeError(
      "nativeResume:validateSubpath",
      `Refusing unsafe Claude session subpath '${subpath}'.`,
    );
  }
}

const parseTranscriptFile = Effect.fn("ClaudeNativeResumeStore.parseTranscriptFile")(function* (
  filePath: string,
  fileSystem: FileSystem.FileSystem,
) {
  const before = yield* fileSystem
    .stat(filePath)
    .pipe(
      Effect.mapError((cause) =>
        storeError(
          "nativeResume:statTranscript",
          `Failed to inspect Claude transcript '${filePath}'.`,
          cause,
        ),
      ),
    );
  const contents = yield* fileSystem
    .readFileString(filePath)
    .pipe(
      Effect.mapError((cause) =>
        storeError(
          "nativeResume:readTranscript",
          `Failed to read Claude transcript '${filePath}'.`,
          cause,
        ),
      ),
    );
  const after = yield* fileSystem
    .stat(filePath)
    .pipe(
      Effect.mapError((cause) =>
        storeError(
          "nativeResume:statTranscript",
          `Failed to inspect Claude transcript '${filePath}' after reading it.`,
          cause,
        ),
      ),
    );
  const beforeMtime = before.mtime._tag === "Some" ? before.mtime.value.getTime() : undefined;
  const afterMtime = after.mtime._tag === "Some" ? after.mtime.value.getTime() : undefined;
  if (before.size !== after.size || beforeMtime !== afterMtime) {
    return yield* storeError(
      "nativeResume:transcriptChanged",
      `Claude transcript '${filePath}' changed while it was being synchronized.`,
    );
  }
  const entries: Array<SessionStoreEntry> = [];
  for (const [index, line] of contents.split(/\r?\n/).entries()) {
    if (line.trim().length === 0) continue;
    const decoded = yield* decodeUnknownJsonString(line).pipe(
      Effect.mapError((cause) =>
        storeError(
          "nativeResume:parseTranscript",
          `Claude transcript '${filePath}' contains invalid JSON on line ${index + 1}.`,
          cause,
        ),
      ),
    );
    if (!isSessionStoreEntry(decoded)) {
      return yield* storeError(
        "nativeResume:validateTranscript",
        `Claude transcript '${filePath}' contains an entry without a string type on line ${index + 1}.`,
      );
    }
    entries.push(decoded);
  }
  return entries;
});

const findNativeMainTranscript = Effect.fn("ClaudeNativeResumeStore.findNativeMainTranscript")(
  function* (
    configDirPath: string,
    projectKey: string,
    sessionId: string,
    dependencies: ClaudeNativeResumeStoreDependencies,
  ) {
    const { fileSystem, path } = dependencies;
    const projectsRoot = path.join(configDirPath, "projects");
    const exactPath = path.join(projectsRoot, projectKey, `${sessionId}${MAIN_TRANSCRIPT_SUFFIX}`);
    if (yield* fileSystem.exists(exactPath).pipe(Effect.orElseSucceed(() => false))) {
      return exactPath;
    }

    const relativeEntries = yield* fileSystem
      .readDirectory(projectsRoot, { recursive: true })
      .pipe(Effect.orElseSucceed(() => []));
    const candidates = relativeEntries
      .filter((entry) => {
        const segments = entry.replaceAll("\\", "/").split("/");
        return segments.length === 2 && segments[1] === `${sessionId}${MAIN_TRANSCRIPT_SUFFIX}`;
      })
      .map((entry) => path.join(projectsRoot, entry));
    if (candidates.length === 0) return null;

    const candidatesWithMtime = yield* Effect.forEach(candidates, (candidate) =>
      fileSystem.stat(candidate).pipe(
        Effect.map((info) => ({
          candidate,
          mtime: info.mtime._tag === "Some" ? info.mtime.value.getTime() : 0,
        })),
        Effect.orElseSucceed(() => ({ candidate, mtime: 0 })),
      ),
    );
    candidatesWithMtime.sort((left, right) => right.mtime - left.mtime);
    return candidatesWithMtime[0]?.candidate ?? null;
  },
);

const readNativeSession = Effect.fn("ClaudeNativeResumeStore.readNativeSession")(function* (
  configDirPath: string,
  projectKey: string,
  sessionId: string,
  dependencies: ClaudeNativeResumeStoreDependencies,
) {
  const { fileSystem, path } = dependencies;
  const mainFilePath = yield* findNativeMainTranscript(
    configDirPath,
    projectKey,
    sessionId,
    dependencies,
  );
  if (mainFilePath === null) return null;

  const entries = yield* parseTranscriptFile(mainFilePath, fileSystem);
  const sessionRoot = mainFilePath.slice(0, -MAIN_TRANSCRIPT_SUFFIX.length);
  const subagentsRoot = path.join(sessionRoot, "subagents");
  const relativeFiles = yield* fileSystem
    .readDirectory(subagentsRoot, { recursive: true })
    .pipe(Effect.orElseSucceed(() => []));
  const transcriptFiles = relativeFiles.filter((entry) => entry.endsWith(MAIN_TRANSCRIPT_SUFFIX));
  const subkeys = yield* Effect.forEach(transcriptFiles, (relativeFile) =>
    Effect.gen(function* () {
      const filePath = path.join(subagentsRoot, relativeFile);
      const relativeToSession = path.relative(sessionRoot, filePath).replaceAll("\\", "/");
      const subpath = relativeToSession.slice(0, -MAIN_TRANSCRIPT_SUFFIX.length);
      validateSubpath(subpath);
      const subkeyEntries = [...(yield* parseTranscriptFile(filePath, fileSystem))];
      const metadataPath = filePath.slice(0, -MAIN_TRANSCRIPT_SUFFIX.length) + ".meta.json";
      if (yield* fileSystem.exists(metadataPath).pipe(Effect.orElseSucceed(() => false))) {
        const metadataText = yield* fileSystem
          .readFileString(metadataPath)
          .pipe(
            Effect.mapError((cause) =>
              storeError(
                "nativeResume:readMetadata",
                `Failed to read Claude subagent metadata '${metadataPath}'.`,
                cause,
              ),
            ),
          );
        const metadata = yield* decodeUnknownJsonString(metadataText).pipe(
          Effect.mapError((cause) =>
            storeError(
              "nativeResume:parseMetadata",
              `Claude subagent metadata '${metadataPath}' is invalid JSON.`,
              cause,
            ),
          ),
        );
        if (typeof metadata !== "object" || metadata === null || Array.isArray(metadata)) {
          return yield* storeError(
            "nativeResume:validateMetadata",
            `Claude subagent metadata '${metadataPath}' is not an object.`,
          );
        }
        subkeyEntries.push({ ...metadata, type: AGENT_METADATA_TYPE });
      }
      return { subpath, entries: subkeyEntries };
    }),
  );

  return {
    projectKey,
    sessionId,
    entries,
    subkeys,
  } satisfies ClaudeSessionSnapshot;
});

/**
 * Import an existing native Claude transcript into the shared store without
 * spawning Claude or consuming provider usage.
 */
export const importClaudeNativeSessionToStore = Effect.fn(
  "ClaudeNativeResumeStore.importNativeSessionToStore",
)(function* (
  sharedStore: ClaudeSessionStoreShape,
  options: ClaudeNativeSessionImportOptions,
  dependencies: ClaudeNativeResumeStoreDependencies,
) {
  validateProjectKey(options.projectKey);
  const storedSnapshot = yield* Effect.tryPromise({
    try: () =>
      sharedStore.loadSession({
        projectKey: options.projectKey,
        sessionId: options.sessionId,
      }),
    catch: (cause) =>
      storeError(
        "nativeResume:checkManualImport",
        `Failed to inspect shared Claude session '${options.sessionId}'.`,
        cause,
      ),
  });
  const sourceSnapshot = yield* readNativeSession(
    options.sourceConfigDirPath,
    options.projectKey,
    options.sessionId,
    dependencies,
  );
  if (!sourceSnapshot) {
    return yield* storeError(
      "nativeResume:importNotFound",
      `Claude session '${options.sessionId}' was not found in the source account.`,
    );
  }

  if (storedSnapshot) {
    const storeIsSourcePrefix = snapshotIsPrefix(storedSnapshot, sourceSnapshot);
    const sourceIsStorePrefix = snapshotIsPrefix(sourceSnapshot, storedSnapshot);
    if (sourceIsStorePrefix) {
      if (!hasExpectedCheckpoint(storedSnapshot, options.expectedAssistantUuid)) {
        return yield* storeError(
          "nativeResume:importNotFound",
          `Claude session '${options.sessionId}' does not contain the expected checkpoint in the newest available transcript.`,
        );
      }
      return { state: "already-synced" as const, snapshot: storedSnapshot };
    }
    if (!storeIsSourcePrefix) {
      const divergence = describeSnapshotDivergence(sourceSnapshot, storedSnapshot);
      return yield* storeError(
        "nativeResume:importDiverged",
        `Claude session '${options.sessionId}' differs between the source account and shared store; refusing to overwrite either transcript. ${divergence}`,
      );
    }
  }

  if (!hasExpectedCheckpoint(sourceSnapshot, options.expectedAssistantUuid)) {
    return yield* storeError(
      "nativeResume:importNotFound",
      `Claude session '${options.sessionId}' does not contain the expected checkpoint in the newest available transcript.`,
    );
  }
  yield* Effect.tryPromise({
    try: () => sharedStore.replaceSession(sourceSnapshot),
    catch: (cause) =>
      storeError(
        "nativeResume:importManual",
        `Failed to import native Claude session '${options.sessionId}'.`,
        cause,
      ),
  });
  return { state: "imported" as const, snapshot: sourceSnapshot };
});

/**
 * Build a query-local SDK SessionStore that preserves the real target
 * CLAUDE_CONFIG_DIR. Anthropic SDK 0.3.207 treats a null first main load as
 * native resume, so we materialize only projects/<session> before returning
 * null. Target credentials, settings, skills, hooks, and plugins are untouched.
 */
export function makeClaudeNativeResumeStore(
  sharedStore: ClaudeSessionStoreShape,
  options: ClaudeNativeResumeStoreOptions,
  dependencies: ClaudeNativeResumeStoreDependencies,
): SessionStore {
  const { fileSystem, path } = dependencies;
  let firstMainLoad = true;

  const materializeSession = Effect.fn("ClaudeNativeResumeStore.materializeSession")(function* (
    snapshot: ClaudeSessionSnapshot,
    projectKey: string,
  ) {
    validateProjectKey(projectKey);
    const projectDirectory = path.join(options.targetConfigDirPath, "projects", projectKey);
    const targetMainPath = path.join(
      projectDirectory,
      `${options.sessionId}${MAIN_TRANSCRIPT_SUFFIX}`,
    );
    const targetSessionRoot = path.join(projectDirectory, options.sessionId);
    yield* fileSystem
      .makeDirectory(projectDirectory, { recursive: true })
      .pipe(
        Effect.mapError((cause) =>
          storeError(
            "nativeResume:prepareTarget",
            `Failed to prepare Claude project directory '${projectDirectory}'.`,
            cause,
          ),
        ),
      );

    yield* Effect.scoped(
      Effect.gen(function* () {
        const tempDirectory = yield* fileSystem.makeTempDirectoryScoped({
          directory: projectDirectory,
          prefix: `${options.sessionId}.resume.`,
        });
        const tempSessionRoot = path.join(tempDirectory, options.sessionId);
        yield* fileSystem.makeDirectory(tempSessionRoot, { recursive: true });

        for (const subkey of snapshot.subkeys) {
          validateSubpath(subkey.subpath);
          const transcriptEntries = subkey.entries.filter(
            (entry) => entry.type !== AGENT_METADATA_TYPE,
          );
          const metadata = subkey.entries.findLast((entry) => entry.type === AGENT_METADATA_TYPE);
          const transcriptPath = path.join(tempSessionRoot, `${subkey.subpath}.jsonl`);
          if (transcriptEntries.length > 0) {
            yield* fileSystem.makeDirectory(path.dirname(transcriptPath), { recursive: true });
            yield* fileSystem.writeFileString(transcriptPath, encodeJsonLines(transcriptEntries));
            yield* fileSystem.chmod(transcriptPath, 0o600);
          }
          if (metadata !== undefined) {
            const { type: _type, ...metadataPayload } = metadata;
            const metadataPath = path.join(tempSessionRoot, `${subkey.subpath}.meta.json`);
            yield* fileSystem.makeDirectory(path.dirname(metadataPath), { recursive: true });
            yield* fileSystem.writeFileString(
              metadataPath,
              encodeUnknownJsonString(metadataPayload),
            );
            yield* fileSystem.chmod(metadataPath, 0o600);
          }
        }

        yield* fileSystem.remove(targetSessionRoot, { recursive: true, force: true });
        if (snapshot.subkeys.length > 0) {
          yield* fileSystem.rename(tempSessionRoot, targetSessionRoot);
        }
      }).pipe(
        Effect.mapError((cause) =>
          isClaudeSessionStoreError(cause)
            ? cause
            : storeError(
                "nativeResume:materializeSubagents",
                `Failed to materialize Claude subagent transcripts for session '${options.sessionId}'.`,
                cause,
              ),
        ),
      ),
    );

    yield* writeFileStringAtomically({
      filePath: targetMainPath,
      contents: encodeJsonLines(snapshot.entries),
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.mapError((cause) =>
        storeError(
          "nativeResume:materializeMain",
          `Failed to materialize Claude transcript '${targetMainPath}'.`,
          cause,
        ),
      ),
    );
    yield* fileSystem
      .chmod(targetMainPath, 0o600)
      .pipe(
        Effect.mapError((cause) =>
          storeError(
            "nativeResume:protectMain",
            `Failed to protect Claude transcript '${targetMainPath}'.`,
            cause,
          ),
        ),
      );
  });

  const prepareNativeResume = Effect.fn("ClaudeNativeResumeStore.prepareNativeResume")(function* (
    key: SessionKey,
  ) {
    if (key.sessionId !== options.sessionId || key.subpath !== undefined) {
      return yield* storeError(
        "nativeResume:unexpectedLoad",
        `Expected the first SDK load for main session '${options.sessionId}'.`,
      );
    }
    validateProjectKey(key.projectKey);
    const timeoutMs = options.readinessTimeoutMs ?? DEFAULT_READINESS_TIMEOUT_MS;
    const deadline = (yield* Clock.currentTimeMillis) + Math.max(0, timeoutMs);

    let snapshot = yield* Effect.tryPromise({
      try: () => sharedStore.loadSession(key),
      catch: (cause) =>
        storeError(
          "nativeResume:loadShared",
          `Failed to load shared Claude session '${options.sessionId}'.`,
          cause,
        ),
    });
    if (!snapshot || !hasExpectedCheckpoint(snapshot, options.expectedAssistantUuid)) {
      const nativeSnapshot = yield* readNativeSession(
        options.targetConfigDirPath,
        key.projectKey,
        options.sessionId,
        dependencies,
      );
      if (nativeSnapshot && hasExpectedCheckpoint(nativeSnapshot, options.expectedAssistantUuid)) {
        yield* Effect.tryPromise({
          try: () => sharedStore.replaceSession(nativeSnapshot),
          catch: (cause) =>
            storeError(
              "nativeResume:importLegacy",
              `Failed to import native Claude session '${options.sessionId}'.`,
              cause,
            ),
        });
        snapshot = nativeSnapshot;
      }
    }

    while (!snapshot || !hasExpectedCheckpoint(snapshot, options.expectedAssistantUuid)) {
      if ((yield* Clock.currentTimeMillis) >= deadline) break;
      yield* Effect.sleep(READINESS_POLL_INTERVAL_MS);
      snapshot = yield* Effect.tryPromise({
        try: () => sharedStore.loadSession(key),
        catch: (cause) =>
          storeError(
            "nativeResume:waitForShared",
            `Failed while waiting for shared Claude session '${options.sessionId}'.`,
            cause,
          ),
      });
    }

    if (!snapshot || !hasExpectedCheckpoint(snapshot, options.expectedAssistantUuid)) {
      return yield* storeError(
        "nativeResume:notReady",
        `Claude session '${options.sessionId}' is not ready in the shared transcript store. Sync its local history before switching accounts, or restore the original account's local transcript.`,
      );
    }

    yield* materializeSession(snapshot, key.projectKey);
  });

  return {
    append: (key, entries) => sharedStore.append(key, entries),
    load: async (key) => {
      if (firstMainLoad) {
        firstMainLoad = false;
        await Effect.runPromise(prepareNativeResume(key));
        return null;
      }
      return sharedStore.load(key);
    },
    ...(sharedStore.listSessions
      ? { listSessions: (projectKey: string) => sharedStore.listSessions!(projectKey) }
      : {}),
    ...(sharedStore.listSessionSummaries
      ? {
          listSessionSummaries: (projectKey: string) =>
            sharedStore.listSessionSummaries!(projectKey),
        }
      : {}),
    ...(sharedStore.delete ? { delete: (key: SessionKey) => sharedStore.delete!(key) } : {}),
    ...(sharedStore.listSubkeys
      ? {
          listSubkeys: (key: { projectKey: string; sessionId: string }) =>
            sharedStore.listSubkeys!(key),
        }
      : {}),
  } satisfies SessionStore;
}
