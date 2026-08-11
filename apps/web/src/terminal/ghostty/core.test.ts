import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import wasmDataUrl from "./vendor/ghostty-vt.wasm?inline";
import writePtyWasmDataUrl from "./vendor/ghostty-write-pty.wasm?inline";
import { GhosttyTerminalCore, type GhosttyColor } from "./core";

function decodeWasmDataUrl(dataUrl: string): Uint8Array {
  const encoded = dataUrl.split(",", 2)[1];
  if (!encoded) throw new Error("The vendored Ghostty WASM data URL is invalid");
  return Uint8Array.from(atob(encoded), (character) => character.charCodeAt(0));
}

function wasmResponse(bytes: Uint8Array): Response {
  const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  return {
    ok: true,
    status: 200,
    arrayBuffer: async () => buffer,
  } as Response;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("GhosttyTerminalCore browser policy", () => {
  it("restores named ANSI colors, bold-bright rendering, and DEC focus reports", async () => {
    const responses = [
      wasmResponse(decodeWasmDataUrl(wasmDataUrl)),
      wasmResponse(decodeWasmDataUrl(writePtyWasmDataUrl)),
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        const response = responses.shift();
        if (!response) throw new Error("Unexpected Ghostty WASM fetch");
        return response;
      }),
    );
    const ansiColors: GhosttyColor[] = Array.from({ length: 16 }, (_, index) => ({
      r: index * 10,
      g: index * 10 + 1,
      b: index * 10 + 2,
    }));
    const core = await GhosttyTerminalCore.create(
      10,
      3,
      8,
      16,
      {
        foreground: { r: 230, g: 231, b: 232 },
        background: { r: 10, g: 11, b: 12 },
        cursor: { r: 240, g: 241, b: 242 },
        ansiColors,
      },
      () => {},
    );

    try {
      core.write("\u001b[31mR\u001b[1mB\u001b[31;47;1;7mI");
      const snapshot = core.snapshot();
      expect(snapshot.rowData[0]?.cells[0]?.foreground).toEqual(ansiColors[1]);
      expect(snapshot.rowData[0]?.cells[1]?.foreground).toEqual(ansiColors[9]);
      expect(snapshot.rowData[0]?.cells[1]?.bold).toBe(true);
      expect(snapshot.rowData[0]?.cells[2]?.foreground).toEqual(ansiColors[15]);
      expect(snapshot.rowData[0]?.cells[2]?.background).toEqual(ansiColors[1]);

      expect(core.encodeFocus(true)).toBe("");
      core.write("\u001b[?1004h");
      expect(core.encodeFocus(true)).toBe("\u001b[I");
      expect(core.encodeFocus(false)).toBe("\u001b[O");
      core.write("\u001b[?1004l");
      expect(core.encodeFocus(false)).toBe("");
    } finally {
      core.dispose();
    }
  });
});
