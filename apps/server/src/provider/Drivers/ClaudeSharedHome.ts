import type { ClaudeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { expandHomePath } from "../../pathExpansion.ts";
import { resolveClaudeConfigDirPath } from "./ClaudeHome.ts";

const entries = ["skills", "CLAUDE.md", "rules"] as const;
const Manifest = Schema.fromJsonString(
  Schema.Array(Schema.Struct({ name: Schema.Literals(entries), target: Schema.String })),
);
const decodeManifest = Schema.decodeUnknownEffect(Manifest);
const encodeManifest = Schema.encodeEffect(Manifest);
const lock = Semaphore.makeUnsafe(1);

export class ClaudeSharedHomeError extends Schema.TaggedError<ClaudeSharedHomeError>()(
  "ClaudeSharedHomeError",
  { message: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

/** Share only Claude's user-authored context. The journal records links we own,
 * so clearing the setting never removes a user's pre-existing file or link.
 * Each journal update precedes link creation, allowing interrupted setup to retry.
 */
export const materializeClaudeSharedHome = Effect.fn("materializeClaudeSharedHome")(
  function* (
    config: Pick<ClaudeSettings, "configDirPath" | "homePath" | "sharedHomePath">,
    environment?: NodeJS.ProcessEnv,
  ) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const configDir = yield* resolveClaudeConfigDirPath(config, environment);
    const manifestPath = path.join(configDir, ".t3-shared-home.json");
    const shared = config.sharedHomePath.trim();
    const sharedDir = shared ? path.resolve(expandHomePath(shared)) : undefined;
    let owned = yield* fs.readFileString(manifestPath).pipe(
      Effect.flatMap(decodeManifest),
      Effect.catchTag("PlatformError", (error) =>
        error.reason._tag === "NotFound" ? Effect.succeed([]) : Effect.fail(error),
      ),
    );
    if (!sharedDir && owned.length === 0) return;

    const canonical = Effect.fn("ClaudeSharedHome.canonical")(function* (
      value: string,
    ): Effect.fn.Return<string, PlatformError.PlatformError> {
      return yield* fs
        .realPath(value)
        .pipe(
          Effect.catchTag("PlatformError", (error) =>
            error.reason._tag === "NotFound" && path.dirname(value) !== value
              ? canonical(path.dirname(value)).pipe(
                  Effect.map((parent) => path.join(parent, path.basename(value))),
                )
              : Effect.fail(error),
          ),
        );
    });
    const accountRoot = yield* canonical(configDir);
    const sharedRoot = sharedDir ? yield* canonical(sharedDir) : undefined;
    if (
      sharedRoot &&
      sharedRoot !== accountRoot &&
      (sharedRoot.startsWith(accountRoot + path.sep) ||
        accountRoot.startsWith(sharedRoot + path.sep))
    ) {
      return yield* new ClaudeSharedHomeError({
        message:
          "Claude's shared directory and account config directory must not be nested inside one another.",
      });
    }

    const readLink = (link: string) =>
      fs.readLink(link).pipe(
        Effect.flatMap((target) => {
          const absolute = path.resolve(path.dirname(link), target);
          return canonical(path.dirname(absolute)).pipe(
            Effect.map((parent) => path.join(parent, path.basename(absolute))),
          );
        }),
        Effect.catchTag("PlatformError", (error) => {
          if (error.reason._tag === "NotFound") return Effect.succeed(undefined);
          const cause = error.reason.cause;
          if (
            error.reason._tag === "Unknown" &&
            typeof cause === "object" &&
            cause !== null &&
            "code" in cause &&
            cause.code === "EINVAL"
          ) {
            return Effect.succeed(null);
          }
          return Effect.fail(error);
        }),
      );
    const conflict = (name: string) =>
      new ClaudeSharedHomeError({
        message: `Cannot share Claude '${name}': '${path.join(configDir, name)}' already contains a local file, directory, or a different link. Stop sessions using this config directory, back up and reconcile the contents with the shared directory, then move the conflicting entry aside and retry. T3 Code has not overwritten it.`,
      });

    // Preflight all entries before changing any of them, including on disable.
    const plan = yield* Effect.forEach(entries, (name) =>
      Effect.gen(function* () {
        const link = path.join(configDir, name);
        const current = yield* readLink(link);
        const previous = owned.find((entry) => entry.name === name)?.target;
        const target =
          sharedRoot && sharedRoot !== accountRoot ? path.join(sharedRoot, name) : undefined;
        if (previous && current !== undefined && current !== previous) return yield* conflict(name);
        if (target && current !== undefined && current !== previous && current !== target)
          return yield* conflict(name);
        return { name, link, current, previous, target };
      }),
    );

    const save = Effect.fn("ClaudeSharedHome.save")(function* () {
      if (owned.length === 0) {
        yield* fs.remove(manifestPath, { force: true });
        return;
      }
      yield* fs.makeDirectory(configDir, { recursive: true });
      const temporary = yield* fs.makeTempFileScoped({
        directory: configDir,
        prefix: ".t3-shared-home-",
      });
      yield* fs.writeFileString(temporary, yield* encodeManifest(owned));
      yield* fs.rename(temporary, manifestPath);
    });
    if (sharedRoot) {
      yield* fs.makeDirectory(sharedRoot, { recursive: true });
      for (const name of ["skills", "rules"]) {
        yield* fs.makeDirectory(path.join(sharedRoot, name), { recursive: true });
      }
    }
    for (const { name, link, current, previous, target } of plan) {
      if (current === target && current !== undefined) continue;
      if (previous) {
        if (current !== undefined) yield* fs.remove(link);
        owned = owned.filter((entry) => entry.name !== name);
        yield* save();
      }
      if (target) {
        owned = [...owned, { name, target }];
        yield* save();
        // CLAUDE.md may not exist yet. A dangling link lets a later edit become
        // visible to all accounts without copying or creating instruction text.
        yield* fs.symlink(target, link);
      }
    }
  },
  Effect.scoped,
  lock.withPermit,
  Effect.mapError((cause) =>
    cause._tag === "ClaudeSharedHomeError"
      ? cause
      : new ClaudeSharedHomeError({
          message: `Failed to prepare Claude's shared directory: ${cause.message}`,
          cause,
        }),
  ),
);
