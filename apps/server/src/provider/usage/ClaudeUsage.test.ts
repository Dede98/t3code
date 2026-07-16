import * as NodeCrypto from "node:crypto";

import { describe, expect, it } from "@effect/vitest";

import { claudeCredentialServiceName } from "./ClaudeUsage.ts";

describe("claudeCredentialServiceName", () => {
  it("uses Claude Code's unscoped service for the default and HOME layouts", () => {
    expect(
      claudeCredentialServiceName({
        configDirPath: "/Users/test/.claude",
        environment: {},
        hasConfiguredConfigDir: false,
      }),
    ).toBe("Claude Code-credentials");
  });

  it("scopes the service to the normalized CLAUDE_CONFIG_DIR", () => {
    const configDirPath = "/Users/test/.claude-work";
    const suffix = NodeCrypto.createHash("sha256").update(configDirPath).digest("hex").slice(0, 8);

    expect(
      claudeCredentialServiceName({
        configDirPath,
        environment: {},
        hasConfiguredConfigDir: true,
      }),
    ).toBe(`Claude Code-credentials-${suffix}`);
  });

  it("follows CLAUDE_SECURESTORAGE_CONFIG_DIR when Claude overrides secure storage", () => {
    const secureStoragePath = "/Volumes/secure/claude";
    const suffix = NodeCrypto.createHash("sha256")
      .update(secureStoragePath)
      .digest("hex")
      .slice(0, 8);

    expect(
      claudeCredentialServiceName({
        configDirPath: "/Users/test/.claude-work",
        environment: { CLAUDE_SECURESTORAGE_CONFIG_DIR: secureStoragePath },
        hasConfiguredConfigDir: true,
      }),
    ).toBe(`Claude Code-credentials-${suffix}`);
  });

  it("disables scoping for an explicitly empty secure storage override", () => {
    expect(
      claudeCredentialServiceName({
        configDirPath: "/Users/test/.claude-work",
        environment: { CLAUDE_SECURESTORAGE_CONFIG_DIR: "" },
        hasConfiguredConfigDir: true,
      }),
    ).toBe("Claude Code-credentials");
  });
});
