import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  RuntimeArtifactManifest,
  RuntimeProfileConfig,
  RuntimeProfileId,
} from "./runtimeProfile.ts";

const decodeProfileId = Schema.decodeUnknownSync(RuntimeProfileId);
const decodeProfileConfig = Schema.decodeUnknownSync(RuntimeProfileConfig);
const decodeManifest = Schema.decodeUnknownSync(RuntimeArtifactManifest);

const validManifest = {
  schemaVersion: 1,
  runtimeVersion: "0.0.29",
  buildHash: "0123456789abcdef",
  platform: "darwin",
  architecture: "arm64",
  entrypoint: "apps/server/dist/bin.mjs",
  nodeExecutable: "node/bin/node",
  files: [
    {
      path: "apps/server/dist/bin.mjs",
      byteSize: 4,
      sha256: "0".repeat(64),
    },
    {
      path: "node/bin/node",
      byteSize: 4,
      sha256: "1".repeat(64),
    },
  ],
};

describe("runtime profile contracts", () => {
  it.each(["dev", "alpha", "nightly", "custom:mac-mini"])("accepts profile id %s", (profileId) => {
    expect(decodeProfileId(profileId)).toBe(profileId);
  });

  it.each([
    "stable",
    "custom:",
    "custom:Mac-mini",
    "custom:-mac-mini",
    "custom:mac_mini",
    "custom:mac mini",
    "custom:../nightly",
    "custom:mac/mini",
    "custom:mac\\mini",
    `custom:${"a".repeat(64)}`,
  ])("rejects invalid or unsafe profile id %s", (profileId) => {
    expect(() => decodeProfileId(profileId)).toThrow();
  });

  it.each([1, 3773, 65_535])("accepts TCP port %s", (port) => {
    expect(
      decodeProfileConfig({
        schemaVersion: 1,
        profileId: "dev",
        port,
        createdAt: "2026-07-22T00:00:00.000Z",
        updatedAt: "2026-07-22T00:00:00.000Z",
      }).port,
    ).toBe(port);
  });

  it.each([0, -1, 65_536, 3773.5, "3773"])("rejects invalid TCP port %s", (port) => {
    expect(() =>
      decodeProfileConfig({
        schemaVersion: 1,
        profileId: "dev",
        port,
        createdAt: "2026-07-22T00:00:00.000Z",
        updatedAt: "2026-07-22T00:00:00.000Z",
      }),
    ).toThrow();
  });

  it("accepts a complete safe artifact manifest", () => {
    expect(decodeManifest(validManifest).entrypoint).toBe("apps/server/dist/bin.mjs");
  });

  it.each(["/apps/server/dist/bin.mjs", "../bin.mjs", "apps/../bin.mjs", "C:/bin.mjs"])(
    "rejects unsafe manifest path %s",
    (entrypoint) => {
      expect(() => decodeManifest({ ...validManifest, entrypoint })).toThrow();
    },
  );
});
