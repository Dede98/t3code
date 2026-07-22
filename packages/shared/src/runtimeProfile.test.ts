import * as NodeServices from "@effect/platform-node/NodeServices";
import { RuntimeProfileId } from "@t3tools/contracts/runtimeProfile";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  isPathWithin,
  makeRuntimeProfileLayout,
  resolveRuntimeArtifactPath,
  runtimeProfileDirectoryName,
  runtimeProfileIdFromDirectoryName,
  runtimeVersionDirectoryName,
} from "./runtimeProfile.ts";

describe("runtime profile layout", () => {
  it("maps every profile id to a unique reversible portable directory", () => {
    const profileIds = [
      RuntimeProfileId.make("dev"),
      RuntimeProfileId.make("alpha"),
      RuntimeProfileId.make("nightly"),
      RuntimeProfileId.make("custom:mac-mini"),
    ];
    const directoryNames = profileIds.map(runtimeProfileDirectoryName);

    assert.deepEqual(directoryNames, ["dev", "alpha", "nightly", "custom-mac-mini"]);
    assert.equal(new Set(directoryNames).size, profileIds.length);
    assert.deepEqual(directoryNames.map(runtimeProfileIdFromDirectoryName), profileIds);
    assert.isUndefined(runtimeProfileIdFromDirectoryName("custom-../nightly"));
    assert.isUndefined(runtimeProfileIdFromDirectoryName("stable"));
  });

  it.effect("derives every profile path within a scoped temporary root", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-runtime-layout-" });
        const layout = makeRuntimeProfileLayout(
          path,
          profilesRoot,
          RuntimeProfileId.make("custom:mac-mini"),
        );

        assert.equal(layout.profileDirectoryName, "custom-mac-mini");
        assert.equal(layout.profileConfigPath, path.join(layout.profileDirectory, "profile.json"));
        assert.equal(layout.currentRuntimePath, path.join(layout.runtimeDirectory, "current.json"));
        assert.equal(layout.daemonLockPath, path.join(layout.runDirectory, "daemon.lock"));
        assert.equal(layout.discoveryPath, path.join(layout.runDirectory, "discovery.json"));
        assert.equal(
          layout.credentialServiceName,
          "com.t3tools.t3code.runtime-profile.custom-mac-mini",
        );

        for (const derivedPath of [
          layout.profileDirectory,
          layout.profileConfigPath,
          layout.runtimeDirectory,
          layout.currentRuntimePath,
          layout.launcherDirectory,
          layout.versionsDirectory,
          layout.stateDirectory,
          layout.logsDirectory,
          layout.runDirectory,
          layout.daemonLockPath,
          layout.discoveryPath,
        ]) {
          assert.isTrue(isPathWithin(path, profilesRoot, derivedPath));
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("resolves only safe artifact-relative paths", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const root = path.resolve("runtime-version");

      assert.equal(
        resolveRuntimeArtifactPath(path, root, "apps/server/dist/bin.mjs"),
        path.join(root, "apps/server/dist/bin.mjs"),
      );
      for (const unsafe of ["/bin.mjs", "../bin.mjs", "apps/../bin.mjs", "C:/bin.mjs"]) {
        assert.isUndefined(resolveRuntimeArtifactPath(path, root, unsafe));
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("derives a version directory only from validated manifest identity", () => {
    assert.equal(
      runtimeVersionDirectoryName({
        runtimeVersion: "0.0.29" as never,
        buildHash: "0123456789abcdef" as never,
      }),
      "0.0.29-0123456789abcdef",
    );
  });
});
