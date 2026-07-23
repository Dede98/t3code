import { assert, it } from "@effect/vitest";

import {
  canonicalAgentControlTaskSourceTimestamp,
  parseAgentControlTaskSourceTimestamp,
} from "./sourceTimestamp.ts";

it("accepts source timestamps with zero through three fractional digits", () => {
  const cases = [
    ["2026-07-23T10:00:00Z", "2026-07-23T10:00:00.000Z"],
    ["2026-07-23T10:00:00.1Z", "2026-07-23T10:00:00.100Z"],
    ["2026-07-23T10:00:00.12Z", "2026-07-23T10:00:00.120Z"],
    ["2026-07-23T10:00:00.123Z", "2026-07-23T10:00:00.123Z"],
    ["2026-07-23T12:00:00.12+02:00", "2026-07-23T10:00:00.120Z"],
  ] as const;

  for (const [input, canonical] of cases) {
    assert.equal(canonicalAgentControlTaskSourceTimestamp(input), canonical);
  }
});

it("rejects source timestamps with four or more fractional digits", () => {
  for (const input of [
    "2026-07-23T10:00:00.0001Z",
    "2026-07-23T10:00:00.0009Z",
    "2026-07-23T10:00:00.1234Z",
    "2026-07-23T10:00:00.123456789+02:00",
  ]) {
    assert.isNull(parseAgentControlTaskSourceTimestamp(input), input);
  }

  assert.isNull(canonicalAgentControlTaskSourceTimestamp("2026-07-23T10:00:00.0001Z"));
  assert.isNull(canonicalAgentControlTaskSourceTimestamp("2026-07-23T10:00:00.0009Z"));
});

it("continues to require a source timestamp timezone", () => {
  assert.isNull(parseAgentControlTaskSourceTimestamp("2026-07-23T10:00:00.123"));
});
