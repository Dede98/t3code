import type { ExternalMcpServerConfig } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  endsSignIn,
  toConfig,
  toDraft,
  validate,
  type HeaderDraft,
} from "./externalMcpServerDraft";

const stored: ExternalMcpServerConfig = {
  url: "https://tools.example.com/mcp",
  enabled: false,
  headers: [{ name: "Authorization", value: "", sensitive: true, valueRedacted: true }],
};

const withHeader = (patch: Partial<HeaderDraft>) => {
  const draft = toDraft("tools", stored);
  return { ...draft, headers: [{ ...draft.headers[0]!, ...patch }] };
};

describe("external MCP server drafts", () => {
  it.each([
    ["an untouched secret", {}, { valueRedacted: true, value: "" }],
    ["a case-only rename", { name: "authorization" }, { valueRedacted: true, value: "" }],
    ["Secret turned off and on again", { sensitive: true }, { valueRedacted: true, value: "" }],
    ["a new value", { value: "Bearer next" }, { value: "Bearer next", sensitive: true }],
  ])("keeps the stored secret only while it is unchanged: %s", (_case, patch, expected) => {
    const draft = withHeader(patch);
    expect(validate(draft, new Set(["tools"]), false)).toBeNull();
    const header = toConfig(draft, stored.enabled).headers[0]!;
    expect(header).toMatchObject(expected);
    if (!("valueRedacted" in expected)) expect(header.valueRedacted).toBeUndefined();
  });

  it.each([
    ["a rename", { name: "X-Api-Key" }],
    ["Secret turned off", { sensitive: false }],
  ])("asks for the value again instead of deleting the stored secret: %s", (_case, patch) => {
    expect(validate(withHeader(patch), new Set(["tools"]), false)).toMatch(/again/);
  });

  it.each([
    ["a taken id", { id: "tools" }, true],
    ["the reserved id", { id: "t3-code" }, true],
    ["plain HTTP to a remote host", { url: "http://tools.example.com/mcp" }, false],
    ["credentials in the URL", { url: "https://user:pw@tools.example.com/mcp" }, false],
    ["an over-long client ID", { auth: "oauth" as const, clientId: "x".repeat(513) }, false],
    ["an over-long scope", { auth: "oauth" as const, scopes: "x".repeat(257) }, false],
  ])("rejects what the server would refuse: %s", (_case, patch, isNew) => {
    const draft = { ...toDraft("tools", stored), headers: [], ...patch };
    expect(validate(draft, new Set(["tools"]), isNew)).not.toBeNull();
  });

  it("rejects a static Authorization header on an OAuth server", () => {
    const draft = { ...withHeader({ value: "Bearer x" }), auth: "oauth" as const };
    expect(validate(draft, new Set(), false)).toMatch(/sign-in provides/);
  });

  it("keeps the enabled flag and provider selection on save", () => {
    const config = toConfig(
      toDraft("tools", { ...stored, providerInstances: [] as never }),
      stored.enabled,
    );
    expect(config).toMatchObject({ enabled: false, providerInstances: [] });
  });

  it("warns before an edit drops the sign-in", () => {
    const oauth: ExternalMcpServerConfig = { ...stored, headers: [], auth: { type: "oauth" } };
    const draft = toDraft("tools", oauth);
    expect(endsSignIn(draft, oauth)).toBe(false);
    expect(endsSignIn({ ...draft, displayName: "Tools" }, oauth)).toBe(false);
    expect(endsSignIn({ ...draft, url: "https://other.example.com/mcp" }, oauth)).toBe(true);
    expect(endsSignIn({ ...draft, clientId: "abc" }, oauth)).toBe(true);
    expect(endsSignIn({ ...draft, auth: "headers" }, oauth)).toBe(true);
  });
});
