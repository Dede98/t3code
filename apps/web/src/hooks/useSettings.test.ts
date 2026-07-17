import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { describe, expect, it } from "vite-plus/test";

import { mergeEnvironmentSettings, splitSettingsPatch } from "./useSettings";

describe("mergeEnvironmentSettings", () => {
  it("combines the selected environment's server settings with client preferences", () => {
    const serverSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [ProviderInstanceId.make("codex_remote")]: {
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
        },
      },
    };
    const clientSettings = {
      ...DEFAULT_CLIENT_SETTINGS,
      favorites: [
        {
          provider: ProviderInstanceId.make("codex_remote"),
          model: "gpt-5.4",
        },
      ],
    };

    const settings = mergeEnvironmentSettings(serverSettings, clientSettings);

    expect(settings.providerInstances).toBe(serverSettings.providerInstances);
    expect(settings.favorites).toBe(clientSettings.favorites);
  });
});

describe("splitSettingsPatch", () => {
  it("routes worktree branch naming settings to the server", () => {
    const result = splitSettingsPatch({
      worktreeBranchNameMode: "full",
      worktreeBranchPrefix: "team",
    });

    expect(result.serverPatch).toEqual({
      worktreeBranchNameMode: "full",
      worktreeBranchPrefix: "team",
    });
    expect(result.clientPatch).toEqual({});
  });

  it("routes the Claude cross-account continuation gate to the server", () => {
    const result = splitSettingsPatch({
      claudeCrossAccountContinuationEnabled: true,
    });

    expect(result.serverPatch).toEqual({
      claudeCrossAccountContinuationEnabled: true,
    });
    expect(result.clientPatch).toEqual({});
  });
});
