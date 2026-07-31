import * as NodeCrypto from "node:crypto";

import { assert, describe, it } from "@effect/vitest";

import {
  canonicalJson,
  combinedInitialPlanningEventDigest,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  parseCanonicalJsonObject,
  parseCanonicalJsonObjectBytes,
  parseJsonStrict,
  sha256Bytes,
  sha256Utf8,
} from "./eventEvidence.ts";

describe("Initial Planning canonical event evidence", () => {
  it("sorts every object key while preserving optional, null, and unknown raw fields", () => {
    const canonical = canonicalJson({
      z: null,
      payload: {
        unknown: { beta: 2, alpha: 1 },
        optional: null,
        list: [{ z: true, a: false }],
      },
      metadata: { provider: "codex", extension: "kept" },
      a: "ä",
    });

    assert.equal(
      canonical,
      '{"a":"ä","metadata":{"extension":"kept","provider":"codex"},"payload":{"list":[{"a":false,"z":true}],"optional":null,"unknown":{"alpha":1,"beta":2}},"z":null}',
    );
    assert.equal(canonicalJson({ ä: 1, z: 2, a: 3 }), '{"a":3,"z":2,"ä":1}');
    assert.deepStrictEqual(parseCanonicalJson(canonical), parseJsonStrict(canonical));
  });

  it("fails closed for duplicate, noncanonical, malformed, and trailing JSON", () => {
    for (const [name, source] of [
      ["duplicate key", '{"metadata":{"x":1,"x":2}}'],
      ["key order", '{"z":1,"a":2}'],
      ["interior whitespace", '{"a": 1}'],
      ["leading whitespace", ' {"a":1}'],
      ["trailing whitespace", '{"a":1}\n'],
      ["unicode escape", '{"a":"\\u00e4"}'],
      ["exponent", '{"a":1e0}'],
      ["decimal integer", '{"a":1.0}'],
      ["escaped slash", '{"a":"\\/"}'],
      ["alternate line-feed escape", '{"a":"\\u000a"}'],
      ["noncanonical nested object", '{"a":{"z":1,"b":2}}'],
      ["trailing comma", '{"a":1,}'],
      ["trailing JSON", '{"a":1} true'],
      ["non-object trailing JSON", "[] false"],
    ] as const) {
      assert.throws(() => parseCanonicalJson(source), /Invalid canonical JSON/u, name);
    }
  });

  it("requires an object root and its exact canonical key set", () => {
    assert.deepStrictEqual(parseCanonicalJsonObject('{"a":1,"b":2}', ["b", "a"]), {
      a: 1,
      b: 2,
    });
    for (const source of ["[]", '{"a":1}', '{"a":1,"b":2,"c":3}']) {
      assert.throws(() => parseCanonicalJsonObject(source, ["a", "b"]), /Invalid canonical JSON/u);
    }
  });

  it("hashes canonical UTF-8 bytes and binds the ordered pair of complete events", () => {
    const left = '{"metadata":{"note":"Grüße"},"sequence":7}';
    const right = '{"metadata":{},"sequence":8}';
    assert.equal(
      sha256Utf8(left),
      NodeCrypto.createHash("sha256").update(Buffer.from(left, "utf8")).digest("hex"),
    );
    assert.notEqual(
      combinedInitialPlanningEventDigest(left, right),
      combinedInitialPlanningEventDigest(right, left),
    );
    assert.equal(combinedInitialPlanningEventDigest(left, right).length, 64);
  });

  it("rejects every invalid UTF-8 class before JSON parsing and preserves real U+FFFD", () => {
    const invalidCases = [
      ["single-ff", [0xff]],
      ["continuation", [0x80]],
      ["truncated", [0xe2, 0x82]],
      ["overlong", [0xc0, 0xaf]],
      ["surrogate", [0xed, 0xa0, 0x80]],
      ["invalid-four-byte", [0xf4, 0x90, 0x80, 0x80]],
      ["mixed", [...Buffer.from('{"a":"ok', "utf8"), 0xff, ...Buffer.from('"}', "utf8")]],
    ] as const;

    for (const [name, values] of invalidCases) {
      const bytes = Uint8Array.from(values);
      assert.equal(new TextDecoder().decode(bytes).includes("\uFFFD"), true, name);
      assert.throws(() => decodeCanonicalUtf8Bytes(bytes), /invalid UTF-8 bytes/u, name);
      assert.notEqual(
        sha256Bytes(bytes),
        sha256Utf8(new TextDecoder().decode(bytes)),
        `${name}: corrupt and replacement digests`,
      );
    }

    const healthy = Buffer.from('{"value":"\uFFFD"}', "utf8");
    assert.deepStrictEqual(parseCanonicalJsonObjectBytes(healthy, ["value"]).value, {
      value: "\uFFFD",
    });
    assert.equal(decodeCanonicalUtf8Bytes(healthy), '{"value":"\uFFFD"}');
    assert.equal(sha256Bytes(healthy), sha256Utf8('{"value":"\uFFFD"}'));
  });
});
