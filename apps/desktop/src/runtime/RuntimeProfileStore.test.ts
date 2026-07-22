import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeProfileConfig,
  RuntimeProfileId,
  type RuntimeProfileConfig as RuntimeProfileConfigValue,
} from "@t3tools/contracts/runtimeProfile";
import { makeRuntimeProfileLayout } from "@t3tools/shared/runtimeProfile";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import * as RuntimeProfileStore from "./RuntimeProfileStore.ts";

const encodeProfileConfig = Schema.encodeSync(Schema.fromJsonString(RuntimeProfileConfig));

function makeConfig(
  profileId = RuntimeProfileId.make("dev"),
  port = 3773,
): RuntimeProfileConfigValue {
  return {
    schemaVersion: 1,
    profileId,
    port,
    createdAt: "2026-07-22T00:00:00.000Z",
    updatedAt: "2026-07-22T00:00:00.000Z",
  };
}

describe("RuntimeProfileStore", () => {
  it.effect("ensures the complete layout idempotently without touching state or logs", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });
        const config = makeConfig();

        assert.deepEqual(yield* store.ensureProfile(config), config);
        const layout = makeRuntimeProfileLayout(path, profilesRoot, config.profileId);
        yield* fs.writeFileString(path.join(layout.stateDirectory, "sentinel"), "state");
        yield* fs.writeFileString(path.join(layout.logsDirectory, "sentinel"), "logs");

        assert.deepEqual(yield* store.ensureProfile(config), config);
        assert.equal(
          yield* fs.readFileString(path.join(layout.stateDirectory, "sentinel")),
          "state",
        );
        assert.equal(yield* fs.readFileString(path.join(layout.logsDirectory, "sentinel")), "logs");
        for (const directory of [
          layout.runtimeDirectory,
          layout.launcherDirectory,
          layout.versionsDirectory,
          layout.stateDirectory,
          layout.logsDirectory,
          layout.runDirectory,
        ]) {
          assert.isTrue(yield* fs.exists(directory));
        }
        assert.isTrue(yield* fs.exists(layout.profileConfigPath));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects a conflicting existing profile configuration", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });
        yield* store.ensureProfile(makeConfig());

        const error = yield* store
          .ensureProfile(makeConfig(RuntimeProfileId.make("dev"), 4773))
          .pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeProfileConfigConflictError");
        if (error._tag === "RuntimeProfileConfigConflictError") {
          assert.deepEqual(error.conflictingFields, ["port"]);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("treats corrupt profile.json as a typed fail-closed error", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });
        const config = makeConfig();
        yield* store.ensureProfile(config);
        const layout = makeRuntimeProfileLayout(path, profilesRoot, config.profileId);
        yield* fs.writeFileString(layout.profileConfigPath, "{broken");

        const error = yield* store.getProfile(config.profileId).pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeProfileCorruptError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("lists healthy profiles while quarantining a corrupt profile", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });
        const healthy = makeConfig(RuntimeProfileId.make("dev"), 3773);
        const corrupt = makeConfig(RuntimeProfileId.make("custom:broken"), 3774);
        yield* store.ensureProfile(healthy);
        yield* store.ensureProfile(corrupt);
        const corruptLayout = makeRuntimeProfileLayout(path, profilesRoot, corrupt.profileId);
        yield* fs.writeFileString(corruptLayout.profileConfigPath, "not-json");

        const entries = yield* store.listProfiles;
        assert.deepInclude(entries, {
          status: "ready",
          profileId: healthy.profileId,
          config: healthy,
          currentRuntime: null,
        });
        assert.deepInclude(entries, {
          status: "quarantined",
          profileId: corrupt.profileId,
          reason: "profile-corrupt",
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("treats corrupt current.json as a typed fail-closed error", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });
        const config = makeConfig();
        yield* store.ensureProfile(config);
        const layout = makeRuntimeProfileLayout(path, profilesRoot, config.profileId);
        yield* fs.writeFileString(layout.currentRuntimePath, "{broken");

        const error = yield* store.getCurrentRuntime(config.profileId).pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeCurrentPointerCorruptError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not swallow genuine filesystem failures from listProfiles", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const realStore = yield* RuntimeProfileStore.make({ profilesRoot });
        yield* realStore.ensureProfile(makeConfig());
        const permissionError = PlatformError.systemError({
          _tag: "PermissionDenied",
          module: "FileSystem",
          method: "readDirectory",
        });
        const failingFileSystem = FileSystem.FileSystem.of({
          ...fs,
          readDirectory: (directory, options) =>
            directory === profilesRoot
              ? Effect.fail(permissionError)
              : fs.readDirectory(directory, options),
        });
        const store = yield* RuntimeProfileStore.make({ profilesRoot }).pipe(
          Effect.provideService(FileSystem.FileSystem, failingFileSystem),
        );

        const error = yield* store.listProfiles.pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeFilesystemError");
        assert.equal(error.operation, "read-directory");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("returns none for a profile that has not been created", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });

        assert.isTrue(Option.isNone(yield* store.getProfile(RuntimeProfileId.make("nightly"))));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects profile config files whose embedded id does not match the directory", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-store-" });
        const store = yield* RuntimeProfileStore.make({ profilesRoot });
        const dev = makeConfig(RuntimeProfileId.make("dev"));
        yield* store.ensureProfile(dev);
        const layout = makeRuntimeProfileLayout(path, profilesRoot, dev.profileId);
        yield* fs.writeFileString(
          layout.profileConfigPath,
          encodeProfileConfig(makeConfig(RuntimeProfileId.make("alpha"))),
        );

        const error = yield* store.getProfile(dev.profileId).pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeProfileCorruptError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
