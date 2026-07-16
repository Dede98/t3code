import * as NodeCrypto from "node:crypto";

import type { ClaudeSettings, ProviderInstanceId, ProviderUsageSnapshot } from "@t3tools/contracts";
import { ProviderDriverKind } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

import { ProviderAdapterRequestError } from "../Errors.ts";
import { resolveClaudeConfigDirPath } from "../Drivers/ClaudeHome.ts";
import { projectClaudeUsageResponse } from "../providerUsageProjection.ts";

const DRIVER_KIND = ProviderDriverKind.make("claudeAgent");
const CLAUDE_CREDENTIAL_SERVICE = "Claude Code-credentials";
const ClaudeCredential = Schema.Struct({
  claudeAiOauth: Schema.Struct({ accessToken: Schema.String }),
});
const decodeClaudeCredential = Schema.decodeUnknownEffect(Schema.fromJsonString(ClaudeCredential));

const decodeAccessToken = (raw: string) =>
  decodeClaudeCredential(raw).pipe(
    Effect.map((credential) => credential.claudeAiOauth.accessToken),
  );

export function claudeCredentialServiceName(input: {
  readonly configDirPath: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly hasConfiguredConfigDir: boolean;
}): string {
  const secureStoragePath = input.environment.CLAUDE_SECURESTORAGE_CONFIG_DIR;
  const hasScopedSecureStorage =
    secureStoragePath !== undefined
      ? secureStoragePath.length > 0
      : input.hasConfiguredConfigDir || Boolean(input.environment.CLAUDE_CONFIG_DIR?.trim());
  if (!hasScopedSecureStorage) return CLAUDE_CREDENTIAL_SERVICE;

  const hashSource = (secureStoragePath ?? input.configDirPath).normalize("NFC");
  const suffix = NodeCrypto.createHash("sha256").update(hashSource).digest("hex").slice(0, 8);
  return `${CLAUDE_CREDENTIAL_SERVICE}-${suffix}`;
}

export const readClaudeUsage = Effect.fn("readClaudeUsage")(function* (input: {
  readonly providerInstanceId: ProviderInstanceId;
  readonly config: Pick<ClaudeSettings, "configDirPath" | "homePath">;
  readonly environment: NodeJS.ProcessEnv;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const httpClient = yield* HttpClient.HttpClient;
  const platform = yield* HostProcessPlatform;
  const configDirPath = yield* resolveClaudeConfigDirPath(input.config, input.environment);
  const credentialService = claudeCredentialServiceName({
    configDirPath,
    environment: input.environment,
    hasConfiguredConfigDir: Boolean(input.config.configDirPath.trim()),
  });
  const fileCredential = fileSystem
    .readFileString(`${configDirPath}/.credentials.json`)
    .pipe(Effect.flatMap(decodeAccessToken));
  const keychainCredential = spawner
    .string(ChildProcess.make("security", ["find-generic-password", "-s", credentialService, "-w"]))
    .pipe(
      Effect.map((raw) => raw.trim()),
      Effect.flatMap(decodeAccessToken),
    );
  const accessToken = yield* fileCredential.pipe(
    Effect.catch(() => (platform === "darwin" ? keychainCredential : Effect.fail(null))),
    Effect.mapError(
      () =>
        new ProviderAdapterRequestError({
          provider: DRIVER_KIND,
          method: "usage/read",
          detail: "Claude credentials were not found for this provider instance.",
        }),
    ),
  );
  const response = yield* HttpClientRequest.get("https://api.anthropic.com/api/oauth/usage").pipe(
    HttpClientRequest.bearerToken(accessToken),
    HttpClientRequest.setHeader("content-type", "application/json"),
    httpClient.execute,
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(Schema.Unknown)),
    Effect.mapError(
      () =>
        new ProviderAdapterRequestError({
          provider: DRIVER_KIND,
          method: "usage/read",
          detail: "The Claude usage endpoint could not be read.",
        }),
    ),
  );
  const observedAt = DateTime.formatIso(yield* DateTime.now);
  const usage = projectClaudeUsageResponse({
    providerInstanceId: input.providerInstanceId,
    driver: DRIVER_KIND,
    observedAt,
    response,
  });
  if (usage === null) {
    return yield* new ProviderAdapterRequestError({
      provider: DRIVER_KIND,
      method: "usage/read",
      detail: "Claude returned no usage limits for this account.",
    });
  }
  return usage satisfies ProviderUsageSnapshot;
});
