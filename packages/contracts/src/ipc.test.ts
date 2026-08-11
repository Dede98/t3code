import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ContextMenuItemSchema, DesktopEnvironmentBootstrapSchema } from "./ipc.ts";

const decodeContextMenuItem = Schema.decodeUnknownSync(ContextMenuItemSchema);

describe("ContextMenuItemSchema", () => {
  it("preserves the portable copy accelerator", () => {
    expect(
      decodeContextMenuItem({
        id: "copy",
        label: "Copy",
        accelerator: "copy",
      }),
    ).toEqual({ id: "copy", label: "Copy", accelerator: "copy" });
  });
});

describe("DesktopEnvironmentBootstrapSchema", () => {
  const decode = Schema.decodeUnknownSync(DesktopEnvironmentBootstrapSchema);

  it("preserves the concrete running distro separately from the backend id", () => {
    expect(
      decode({
        id: "wsl:default",
        label: "WSL (Ubuntu)",
        runningDistro: "Ubuntu",
        httpBaseUrl: "http://127.0.0.1:3774/",
        wsBaseUrl: "ws://127.0.0.1:3774/",
      }),
    ).toEqual({
      id: "wsl:default",
      label: "WSL (Ubuntu)",
      runningDistro: "Ubuntu",
      httpBaseUrl: "http://127.0.0.1:3774/",
      wsBaseUrl: "ws://127.0.0.1:3774/",
    });
  });

  it("allows non-running and non-WSL bootstraps to report no running distro", () => {
    expect(
      decode({
        id: "primary",
        label: "Windows",
        runningDistro: null,
        httpBaseUrl: null,
        wsBaseUrl: null,
      }).runningDistro,
    ).toBeNull();
  });
});
