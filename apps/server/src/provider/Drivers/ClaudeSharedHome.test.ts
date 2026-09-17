import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { materializeClaudeSharedHome } from "./ClaudeSharedHome.ts";
import { discoverClaudeSkills } from "./ClaudeSkills.ts";

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs
    .makeTempDirectoryScoped({ prefix: "t3-claude-shared-" })
    .pipe(Effect.flatMap(fs.realPath));
  const sharedHomePath = path.join(root, "shared");
  const config = { homePath: "", configDirPath: path.join(root, "account-a"), sharedHomePath };
  const second = { ...config, configDirPath: path.join(root, "account-b") };
  return { fs, path, root, config, second };
});

it.layer(NodeServices.layer)("ClaudeSharedHome", (it) => {
  it.effect.skipIf(!symlinksSupported)(
    "shares skills and instructions across accounts, preserving private files and picker metadata",
    () =>
      Effect.gen(function* () {
        const { fs, path, config, second } = yield* fixture;
        for (const account of [config, second]) {
          yield* fs.makeDirectory(account.configDirPath, { recursive: true });
          yield* fs.writeFileString(
            path.join(account.configDirPath, ".credentials.json"),
            account.configDirPath,
          );
          yield* fs.writeFileString(path.join(account.configDirPath, "settings.json"), "{}");
        }
        yield* Effect.all(
          [materializeClaudeSharedHome(config), materializeClaudeSharedHome(second)],
          { concurrency: "unbounded" },
        );
        const skill = path.join(config.configDirPath, "skills", "shared-skill");
        yield* fs.makeDirectory(skill);
        yield* fs.writeFileString(
          path.join(skill, "SKILL.md"),
          "---\ndescription: Shared example\ndisable-model-invocation: true\n---\nDo the thing.",
        );
        yield* fs.writeFileString(
          path.join(config.configDirPath, "CLAUDE.md"),
          "Global instructions",
        );
        yield* fs.writeFileString(
          path.join(second.configDirPath, "rules", "style.md"),
          "Shared style",
        );
        expect(yield* fs.readFileString(path.join(second.configDirPath, "CLAUDE.md"))).toBe(
          "Global instructions",
        );
        expect(yield* fs.readFileString(path.join(config.configDirPath, "rules", "style.md"))).toBe(
          "Shared style",
        );
        const skills = yield* discoverClaudeSkills(second);
        expect(skills).toMatchObject([
          { name: "shared-skill", description: "Shared example", userInvocationOnly: true },
        ]);
        yield* materializeClaudeSharedHome(config);
        for (const account of [config, second]) {
          expect(
            yield* fs.readFileString(path.join(account.configDirPath, ".credentials.json")),
          ).toBe(account.configDirPath);
          expect(yield* fs.readFileString(path.join(account.configDirPath, "settings.json"))).toBe(
            "{}",
          );
        }
        expect(yield* fs.exists(path.join(config.sharedHomePath, ".credentials.json"))).toBe(false);
      }),
  );

  it.effect.skipIf(!symlinksSupported)(
    "switches roots and clears only T3-owned links, leaving shared content intact",
    () =>
      Effect.gen(function* () {
        const { fs, path, root, config } = yield* fixture;
        yield* materializeClaudeSharedHome(config);
        yield* fs.writeFileString(path.join(config.sharedHomePath, "CLAUDE.md"), "Original");
        const switched = { ...config, sharedHomePath: path.join(root, "other-shared") };
        yield* materializeClaudeSharedHome(switched);
        yield* fs.writeFileString(path.join(switched.sharedHomePath, "CLAUDE.md"), "Updated");
        expect(yield* fs.readFileString(path.join(config.configDirPath, "CLAUDE.md"))).toBe(
          "Updated",
        );
        yield* materializeClaudeSharedHome({ ...config, sharedHomePath: "" });
        expect(yield* fs.readDirectory(config.configDirPath)).toEqual([]);
        expect(yield* fs.readFileString(path.join(config.sharedHomePath, "CLAUDE.md"))).toBe(
          "Original",
        );
        expect(yield* fs.readFileString(path.join(switched.sharedHomePath, "CLAUDE.md"))).toBe(
          "Updated",
        );
        yield* fs.writeFileString(path.join(config.configDirPath, "CLAUDE.md"), "Private again");
        yield* materializeClaudeSharedHome({ ...config, sharedHomePath: "" });
        expect(yield* fs.readFileString(path.join(config.configDirPath, "CLAUDE.md"))).toBe(
          "Private again",
        );
      }),
  );

  for (const name of ["skills", "CLAUDE.md", "rules"] as const) {
    it.effect.skipIf(!symlinksSupported)(
      `rejects an existing ${name} before creating any links`,
      () =>
        Effect.gen(function* () {
          const { fs, path, config } = yield* fixture;
          yield* fs.makeDirectory(config.configDirPath);
          const local = path.join(config.configDirPath, name);
          if (name !== "CLAUDE.md") yield* fs.makeDirectory(local);
          const localFile = name === "CLAUDE.md" ? local : path.join(local, "existing.md");
          yield* fs.writeFileString(localFile, "Local content");
          const error = yield* materializeClaudeSharedHome(config).pipe(Effect.flip);
          expect(error.message).toContain(`Cannot share Claude '${name}'`);
          expect(yield* fs.readDirectory(config.configDirPath)).toEqual([name]);
          expect(yield* fs.readFileString(localFile)).toBe("Local content");
          expect(yield* fs.exists(config.sharedHomePath)).toBe(false);
        }),
    );
  }

  it.effect.skipIf(!symlinksSupported)(
    "preserves pre-existing links on disable and refuses unrelated links",
    () =>
      Effect.gen(function* () {
        const { fs, path, root, config } = yield* fixture;
        yield* fs.makeDirectory(config.configDirPath);
        const link = path.join(config.configDirPath, "CLAUDE.md");
        yield* fs.symlink(path.join(root, "unrelated.md"), link);
        expect((yield* materializeClaudeSharedHome(config).pipe(Effect.flip)).message).toContain(
          "Cannot share",
        );
        yield* fs.remove(link);
        yield* fs.symlink(path.join(config.sharedHomePath, "CLAUDE.md"), link);
        yield* materializeClaudeSharedHome(config);
        yield* materializeClaudeSharedHome({ ...config, sharedHomePath: "" });
        expect(yield* fs.readLink(link)).toBe(path.join(config.sharedHomePath, "CLAUDE.md"));
      }),
  );

  it.effect.skipIf(!symlinksSupported)(
    "protects files that replaced a managed link and recovers missing managed links",
    () =>
      Effect.gen(function* () {
        const { fs, path, config } = yield* fixture;
        yield* materializeClaudeSharedHome(config);
        const link = path.join(config.configDirPath, "CLAUDE.md");
        yield* fs.remove(link);
        yield* materializeClaudeSharedHome(config);
        expect(yield* fs.readLink(link)).toBe(path.join(config.sharedHomePath, "CLAUDE.md"));
        yield* fs.remove(link);
        yield* fs.writeFileString(link, "Replacement");
        expect(
          (yield* materializeClaudeSharedHome({ ...config, sharedHomePath: "" }).pipe(Effect.flip))
            .message,
        ).toContain("Cannot share");
        expect(yield* fs.readFileString(link)).toBe("Replacement");
        expect(yield* fs.readLink(path.join(config.configDirPath, "skills"))).toBe(
          path.join(config.sharedHomePath, "skills"),
        );
      }),
  );

  it.effect("accepts the primary account as shared root and rejects nested roots", () =>
    Effect.gen(function* () {
      const { fs, path, config } = yield* fixture;
      yield* materializeClaudeSharedHome({ ...config, configDirPath: config.sharedHomePath });
      expect(yield* fs.readDirectory(config.sharedHomePath)).toEqual(["rules", "skills"]);
      const error = yield* materializeClaudeSharedHome({
        ...config,
        sharedHomePath: path.join(config.configDirPath, "skills", "nested"),
      }).pipe(Effect.flip);
      expect(error.message).toContain("must not be nested");
      expect(yield* fs.exists(config.configDirPath)).toBe(false);
    }),
  );

  it.effect.skipIf(!symlinksSupported)(
    "supports shared skills that are themselves linked to another collection",
    () =>
      Effect.gen(function* () {
        const { fs, path, root, config } = yield* fixture;
        const collection = path.join(root, "skill-collection");
        yield* fs.makeDirectory(collection);
        yield* fs.makeDirectory(config.sharedHomePath);
        yield* fs.symlink(collection, path.join(config.sharedHomePath, "skills"));
        yield* materializeClaudeSharedHome(config);
        yield* materializeClaudeSharedHome(config);
        yield* fs.writeFileString(path.join(config.configDirPath, "skills", "marker"), "Shared");
        expect(yield* fs.readFileString(path.join(collection, "marker"))).toBe("Shared");
        yield* materializeClaudeSharedHome({ ...config, sharedHomePath: "" });
        expect(yield* fs.readLink(path.join(config.sharedHomePath, "skills"))).toBe(collection);
        expect(yield* fs.readFileString(path.join(collection, "marker"))).toBe("Shared");
      }),
  );
});
