// @effect-diagnostics nodeBuiltinImport:off - The server-side callback listener needs node:http.
import * as NodeHttp from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import {
  ExternalMcpOAuthError,
  type ExternalMcpOAuthCancelInput,
  type ExternalMcpOAuthCompleteInput,
  type ExternalMcpOAuthServerInput,
  type ExternalMcpOAuthStartInput,
  type ExternalMcpOAuthStatus,
  type ExternalMcpOAuthStatuses,
  type ExternalMcpServerConfig,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Base64Url from "effect/encoding/Base64Url";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as OAuthClient from "./ExternalMcpOAuthClient.ts";

const FLOW_TTL = Duration.minutes(10);
const FLOW_CLOSE_DELAY = Duration.seconds(2);
/** Refresh this long before the reported expiry so a request never carries a dying token. */
const REFRESH_SKEW_MS = 60_000;
const CALLBACK_PATH = "/callback";

const OAuthToken = Schema.Struct({
  accessToken: Schema.String,
  refreshToken: Schema.optionalKey(Schema.String),
  expiresAt: Schema.optionalKey(Schema.Number),
  scope: Schema.optionalKey(Schema.String),
});

/** One file per server: the registration survives sign-out, the token does not. */
const StoredRecord = Schema.Struct({
  serverUrl: Schema.String,
  configuredClientId: Schema.NullOr(Schema.String),
  issuer: Schema.String,
  authorizationEndpoint: Schema.String,
  tokenEndpoint: Schema.String,
  registrationEndpoint: Schema.NullOr(Schema.String),
  resource: Schema.String,
  scopes: Schema.Array(Schema.String),
  redirectUri: Schema.String,
  clientId: Schema.String,
  clientSecret: Schema.optionalKey(Schema.String),
  tokenEndpointAuthMethod: Schema.Literals(["none", "client_secret_post", "client_secret_basic"]),
  token: Schema.optionalKey(OAuthToken),
  needsLogin: Schema.optionalKey(Schema.String),
});
type StoredRecord = typeof StoredRecord.Type;
const StoredRecordJson = Schema.fromJsonString(StoredRecord);
const decodeRecord = Schema.decodeUnknownOption(StoredRecordJson);
const encodeRecord = Schema.encodeSync(StoredRecordJson);

interface Flow {
  readonly flowId: string;
  readonly state: string;
  readonly verifier: string;
  readonly authorizationUrl: string;
  readonly expiresAt: number;
  readonly serverReceivesCallback: boolean;
  readonly scope: Scope.Closeable;
}

interface State {
  /** OAuth servers in settings. */
  readonly servers: ReadonlyMap<string, ExternalMcpServerConfig>;
  readonly records: ReadonlyMap<string, StoredRecord>;
  readonly flows: ReadonlyMap<string, Flow>;
  readonly errors: ReadonlyMap<string, string>;
}

const secretName = (serverId: string) =>
  `mcp-oauth-${Base64Url.encode(new TextEncoder().encode(serverId))}`;

const oauthServers = (settings: ServerSettings) =>
  new Map(
    Object.entries(settings.externalMcpServers).filter(
      ([, server]) => server.auth?.type === "oauth",
    ),
  );

const configuredClientId = (server: ExternalMcpServerConfig) =>
  server.auth?.type === "oauth" ? (server.auth.clientId ?? null) : null;

/** A record is only valid for the URL and client it was made for. */
const recordMatches = (record: StoredRecord, server: ExternalMcpServerConfig) =>
  record.serverUrl === server.url && record.configuredClientId === configuredClientId(server);

const connectedIds = (state: State): ReadonlySet<string> =>
  new Set([...state.servers.keys()].filter((id) => state.records.get(id)?.token !== undefined));

const sameSet = (left: ReadonlySet<string>, right: ReadonlySet<string>) =>
  left.size === right.size && [...left].every((id) => right.has(id));

const isoAt = (epochMs: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMs));

function statusOf(state: State, serverId: string): ExternalMcpOAuthStatus {
  const record = state.records.get(serverId);
  const flow = state.flows.get(serverId);
  return {
    state:
      record?.token !== undefined
        ? "connected"
        : record?.needsLogin !== undefined
          ? "needs-login"
          : "signed-out",
    issuer: record?.issuer ?? null,
    expiresAt: record?.token?.expiresAt === undefined ? null : isoAt(record.token.expiresAt),
    message: state.errors.get(serverId) ?? record?.needsLogin ?? null,
    flow:
      flow === undefined
        ? null
        : {
            flowId: flow.flowId,
            authorizationUrl: flow.authorizationUrl,
            serverReceivesCallback: flow.serverReceivesCallback,
            expiresAt: isoAt(flow.expiresAt),
          },
  };
}

const statusesOf = (state: State): ExternalMcpOAuthStatuses =>
  Object.fromEntries(
    [...state.servers.keys()].map((id) => [id, statusOf(state, id)]),
  ) as ExternalMcpOAuthStatuses;

const oauthError = (message: string) => new ExternalMcpOAuthError({ message });

/**
 * Validates a redirect against the flow that produced it. Browsers can hide `http://` in the
 * address bar, so a pasted URL may lack its scheme.
 */
export function readCallback(
  value: string,
  input: { readonly redirectUri: string; readonly state: string; readonly issuer: string },
): { readonly code: string } | { readonly error: string } {
  const expected = new URL(input.redirectUri);
  const pasted = value.trim();
  const candidate = /^[a-z][a-z\d+.-]*:\/\//iu.test(pasted)
    ? pasted
    : `${expected.protocol}//${pasted}`;
  if (!URL.canParse(candidate)) throw new Error("That is not the redirect URL of this sign-in.");
  const url = new URL(candidate);
  const single = (key: string) => {
    const values = url.searchParams.getAll(key);
    return values.length === 1 ? values[0] : undefined;
  };
  const iss = single("iss");
  if (
    url.origin !== expected.origin ||
    url.pathname !== expected.pathname ||
    single("state") !== input.state ||
    (iss !== undefined && iss.replace(/\/+$/u, "") !== input.issuer.replace(/\/+$/u, ""))
  ) {
    throw new Error("That is not the redirect URL of this sign-in.");
  }
  const error = single("error");
  if (error !== undefined) return { error: single("error_description") ?? error };
  const code = single("code");
  if (!code) throw new Error("The redirect URL carries no authorization code.");
  return { code };
}

const CALLBACK_HTML = (title: string, body: string) =>
  `<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><title>T3 Code</title><style>body{font-family:system-ui;display:grid;place-items:center;min-height:90vh;margin:0}main{max-width:360px;padding:32px}h1{font-size:24px}p{line-height:1.6;opacity:.7}</style></head><body><main><h1>${title}</h1><p>${body}</p></main></body></html>`;

export interface ExternalMcpOAuthShape {
  readonly statuses: Stream.Stream<ExternalMcpOAuthStatuses>;
  readonly connectedServerIds: Effect.Effect<ReadonlySet<string>>;
  /** Emits the current set first, then each change. */
  readonly connectedServerIdsChanges: Stream.Stream<ReadonlySet<string>>;
  readonly start: (
    input: ExternalMcpOAuthStartInput,
  ) => Effect.Effect<ExternalMcpOAuthStatus, ExternalMcpOAuthError>;
  readonly complete: (
    input: ExternalMcpOAuthCompleteInput,
  ) => Effect.Effect<ExternalMcpOAuthStatus, ExternalMcpOAuthError>;
  readonly cancel: (input: ExternalMcpOAuthCancelInput) => Effect.Effect<ExternalMcpOAuthStatus>;
  readonly logout: (input: ExternalMcpOAuthServerInput) => Effect.Effect<ExternalMcpOAuthStatus>;
  /**
   * The current access token, refreshed when it is about to expire or when `rejected` is the
   * token the upstream server just refused. None means the user has to sign in again.
   */
  readonly accessToken: (
    serverId: string,
    rejected?: string,
  ) => Effect.Effect<Option.Option<string>>;
  /** The upstream refused a freshly refreshed token: the user has to sign in again. */
  readonly invalidate: (serverId: string, token: string) => Effect.Effect<void>;
}

export class ExternalMcpOAuth extends Context.Service<ExternalMcpOAuth, ExternalMcpOAuthShape>()(
  "t3/mcp/ExternalMcpOAuth",
) {}

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const serverSettings = yield* ServerSettingsService;
  const httpClient = yield* HttpClient.HttpClient;
  const random = yield* SecureRandom.SecureRandom;
  const digest = yield* Digest.Digest;
  // Open sign-in listeners close with the service.
  const serviceScope = yield* Effect.scope;
  // Guards state and secret writes only; network calls run outside it.
  const mutex = yield* Semaphore.make(1);
  const refreshLocks = new Map<string, Semaphore.Semaphore>();
  const refreshLock = Effect.fn("ExternalMcpOAuth.refreshLock")(function* (serverId: string) {
    const existing = refreshLocks.get(serverId);
    if (existing !== undefined) return existing;
    const created = yield* Semaphore.make(1);
    refreshLocks.set(serverId, created);
    return created;
  });
  const state = yield* SubscriptionRef.make<State>({
    servers: new Map(),
    records: new Map(),
    flows: new Map(),
    errors: new Map(),
  });
  const withHttp = <A, E>(effect: Effect.Effect<A, E, HttpClient.HttpClient>) =>
    Effect.provideService(effect, HttpClient.HttpClient, httpClient);
  const randomToken = (bytes: number) =>
    random.randomBytes(bytes).pipe(Effect.orDie, Effect.map(Base64Url.encode));

  const persist = (serverId: string, record: StoredRecord | undefined) =>
    (record === undefined
      ? secrets.remove(secretName(serverId))
      : secrets.set(secretName(serverId), new TextEncoder().encode(encodeRecord(record)))
    ).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Could not persist MCP OAuth credential", { serverId, cause }),
      ),
    );

  const load = (serverId: string) =>
    secrets.get(secretName(serverId)).pipe(
      Effect.map(Option.flatMap((bytes) => decodeRecord(new TextDecoder().decode(bytes)))),
      Effect.orElseSucceed(() => Option.none<StoredRecord>()),
    );

  const setRecord = (serverId: string, record: StoredRecord | undefined) =>
    persist(serverId, record).pipe(
      Effect.andThen(
        SubscriptionRef.update(state, (current) => {
          const records = new Map(current.records);
          if (record === undefined) records.delete(serverId);
          else records.set(serverId, record);
          return { ...current, records };
        }),
      ),
    );

  const setError = (serverId: string, message: string | undefined) =>
    SubscriptionRef.update(state, (current) => {
      const errors = new Map(current.errors);
      if (message === undefined) errors.delete(serverId);
      else errors.set(serverId, message);
      return { ...current, errors };
    });

  const takeFlow = Effect.fn("ExternalMcpOAuth.takeFlow")(function* (
    serverId: string,
    flowId?: string,
  ) {
    const flow = (yield* SubscriptionRef.get(state)).flows.get(serverId);
    if (flow === undefined || (flowId !== undefined && flow.flowId !== flowId)) return undefined;
    yield* SubscriptionRef.update(state, (current) => {
      const flows = new Map(current.flows);
      flows.delete(serverId);
      return { ...current, flows };
    });
    return flow;
  });

  /**
   * For code running inside the flow's own scope, the callback request or the expiry timer:
   * closing it there would interrupt that code before the browser gets its page.
   */
  const closeLater = (scope: Scope.Closeable) =>
    Scope.close(scope, Exit.void).pipe(Effect.delay(FLOW_CLOSE_DELAY), Effect.forkDetach);

  /** Frees the callback port before a new flow binds it. */
  const dropFlow = Effect.fn("ExternalMcpOAuth.dropFlow")(function* (serverId: string) {
    const flow = yield* takeFlow(serverId);
    if (flow !== undefined) yield* Scope.close(flow.scope, Exit.void);
  });

  const currentStatus = (serverId: string) =>
    SubscriptionRef.get(state).pipe(Effect.map((current) => statusOf(current, serverId)));

  const reconcile = Effect.fn("ExternalMcpOAuth.reconcile")(function* (
    settings: ServerSettings,
    initial: boolean,
  ) {
    yield* mutex.withPermit(
      Effect.gen(function* () {
        const current = yield* SubscriptionRef.get(state);
        const next = oauthServers(settings);
        const records = new Map(current.records);
        for (const [id] of current.servers) {
          if (next.has(id)) continue;
          // Sign-in state ends with the server or its OAuth setting.
          records.delete(id);
          yield* persist(id, undefined);
          yield* dropFlow(id);
        }
        for (const [id, server] of next) {
          if (!initial && !current.servers.has(id)) {
            // A server added now starts signed out, even where an old record was left behind.
            records.delete(id);
            yield* persist(id, undefined);
            continue;
          }
          const record = records.get(id) ?? Option.getOrUndefined(yield* load(id));
          if (record === undefined) continue;
          if (recordMatches(record, server)) {
            records.set(id, record);
          } else {
            records.delete(id);
            yield* persist(id, undefined);
            yield* dropFlow(id);
          }
        }
        yield* SubscriptionRef.update(state, (latest) => ({
          ...latest,
          servers: next,
          records,
        }));
      }),
    );
  });

  yield* reconcile(yield* serverSettings.getSettings.pipe(Effect.orDie), true);
  yield* serverSettings.streamChanges.pipe(
    Stream.runForEach((settings) =>
      reconcile(settings, false).pipe(
        Effect.catchCause((cause) => Effect.logWarning("MCP OAuth reconcile failed", cause)),
      ),
    ),
    Effect.forkScoped,
  );

  /** The record, only while settings still name the server it was made for. */
  const liveRecord = (current: State, serverId: string) => {
    const server = current.servers.get(serverId);
    const record = current.records.get(serverId);
    return server !== undefined && record !== undefined && recordMatches(record, server)
      ? record
      : undefined;
  };

  const requireServer = (serverId: string) =>
    SubscriptionRef.get(state).pipe(
      Effect.flatMap((current) => {
        const server = current.servers.get(serverId);
        return server === undefined
          ? Effect.fail(oauthError(`${serverId} is not an OAuth MCP server.`))
          : Effect.succeed(server);
      }),
    );

  /** Reuses the stored registration while the authorization server and client stay the same. */
  const ensureRegistration = Effect.fn("ExternalMcpOAuth.ensureRegistration")(function* (
    serverId: string,
    server: ExternalMcpServerConfig,
  ) {
    const scopes = server.auth?.type === "oauth" ? server.auth.scopes : undefined;
    const discovered = yield* withHttp(OAuthClient.discover(server.url, scopes));
    const existing = (yield* SubscriptionRef.get(state)).records.get(serverId);
    if (
      existing !== undefined &&
      existing.issuer === discovered.issuer &&
      existing.tokenEndpoint === discovered.tokenEndpoint
    ) {
      const { token: _token, needsLogin: _needsLogin, ...kept } = existing;
      return { ...kept, ...toRecordEndpoints(discovered) } satisfies StoredRecord;
    }
    const [high = 0, low = 0] = yield* random.randomBytes(2).pipe(Effect.orDie);
    // Fixed per registration: authorization servers may match the redirect URI exactly.
    const port = 49_152 + (((high << 8) | low) % 16_000);
    const redirectUri = `http://127.0.0.1:${port}${CALLBACK_PATH}`;
    const clientId = configuredClientId(server);
    const registration =
      clientId === null
        ? yield* withHttp(OAuthClient.register(discovered, redirectUri))
        : ({ clientId, tokenEndpointAuthMethod: "none" } satisfies OAuthClient.ClientRegistration);
    return {
      serverUrl: server.url,
      configuredClientId: clientId,
      ...toRecordEndpoints(discovered),
      redirectUri,
      clientId: registration.clientId,
      ...(registration.clientSecret === undefined
        ? {}
        : { clientSecret: registration.clientSecret }),
      tokenEndpointAuthMethod: registration.tokenEndpointAuthMethod,
    } satisfies StoredRecord;
  });

  const registrationOf = (record: StoredRecord): OAuthClient.ClientRegistration => ({
    clientId: record.clientId,
    ...(record.clientSecret === undefined ? {} : { clientSecret: record.clientSecret }),
    tokenEndpointAuthMethod: record.tokenEndpointAuthMethod,
  });

  const finish = Effect.fn("ExternalMcpOAuth.finish")(function* (
    serverId: string,
    flowId: string,
    callbackUrl: string,
  ) {
    const taken = yield* mutex.withPermit(
      Effect.gen(function* () {
        const record = liveRecord(yield* SubscriptionRef.get(state), serverId);
        // A code is good once: the flow closes before it is redeemed.
        const flow = yield* takeFlow(serverId, flowId);
        if (flow === undefined || record === undefined) {
          if (flow !== undefined) yield* closeLater(flow.scope);
          return yield* oauthError("This sign-in is no longer open. Start it again.");
        }
        return { record, flow };
      }),
    );
    return yield* redeem(serverId, taken.record, taken.flow, callbackUrl).pipe(
      Effect.ensuring(closeLater(taken.flow.scope)),
    );
  });

  const redeem = Effect.fn("ExternalMcpOAuth.redeem")(function* (
    serverId: string,
    record: StoredRecord,
    flow: Flow,
    callbackUrl: string,
  ) {
    if ((yield* Clock.currentTimeMillis) > flow.expiresAt) {
      return yield* oauthError("This sign-in expired. Start it again.");
    }
    const callback = yield* Effect.try({
      try: () =>
        readCallback(callbackUrl, {
          redirectUri: record.redirectUri,
          state: flow.state,
          issuer: record.issuer,
        }),
      catch: (cause) => oauthError(cause instanceof Error ? cause.message : String(cause)),
    });
    if ("error" in callback) {
      return yield* oauthError(`Sign-in was not completed: ${callback.error}`);
    }
    const taken = { record, code: callback.code, verifier: flow.verifier };
    const token = yield* withHttp(
      OAuthClient.requestToken(taken.record.tokenEndpoint, registrationOf(taken.record), {
        grant_type: "authorization_code",
        code: taken.code,
        redirect_uri: taken.record.redirectUri,
        code_verifier: taken.verifier,
        resource: taken.record.resource,
      }),
    ).pipe(Effect.mapError((error) => oauthError(error.message)));
    return yield* mutex.withPermit(
      Effect.gen(function* () {
        const record = liveRecord(yield* SubscriptionRef.get(state), serverId);
        if (record === undefined || record.clientId !== taken.record.clientId) {
          return yield* oauthError("The server's settings changed during sign-in. Try again.");
        }
        const { needsLogin: _cleared, ...rest } = record;
        yield* setRecord(serverId, { ...rest, token });
        yield* setError(serverId, undefined);
        return statusOf(yield* SubscriptionRef.get(state), serverId);
      }),
    );
  });

  /** Binds the registered redirect port on this machine; false when it is taken. */
  const listen = (serverId: string, flowId: string, redirectUri: string, scope: Scope.Scope) =>
    HttpRouter.serve(
      HttpRouter.add(
        "GET",
        CALLBACK_PATH,
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const result = yield* finish(
            serverId,
            flowId,
            new URL(request.originalUrl, redirectUri).toString(),
          ).pipe(
            Effect.tapError((error) => setError(serverId, error.message)),
            Effect.result,
          );
          const headers = { "cache-control": "no-store", "referrer-policy": "no-referrer" };
          return HttpServerResponse.text(
            Result.isSuccess(result)
              ? CALLBACK_HTML("Return to T3 Code", "Signed in. You can close this tab.")
              : CALLBACK_HTML("Sign-in failed", "Return to T3 Code and try again."),
            {
              status: Result.isSuccess(result) ? 200 : 400,
              contentType: "text/html; charset=utf-8",
              headers,
            },
          );
        }),
      ),
      { disableListenLog: true, disableLogger: true },
    ).pipe(
      Layer.provide(
        NodeHttpServer.layer(NodeHttp.createServer, {
          host: "127.0.0.1",
          port: Number(new URL(redirectUri).port),
          disablePreemptiveShutdown: true,
        }),
      ),
      Layer.buildWithScope(scope),
      Effect.as(true),
      Effect.catchCause((cause) =>
        Effect.logInfo("MCP OAuth callback port unavailable; waiting for a pasted URL", cause).pipe(
          Effect.as(false),
        ),
      ),
    );

  const start: ExternalMcpOAuthShape["start"] = Effect.fn("ExternalMcpOAuth.start")(
    function* (input) {
      const server = yield* requireServer(input.serverId);
      yield* setError(input.serverId, undefined);
      const registration = yield* ensureRegistration(input.serverId, server).pipe(
        Effect.mapError((error) => oauthError(error.message)),
        Effect.tapError((error) => setError(input.serverId, error.message)),
      );
      return yield* mutex.withPermit(
        Effect.gen(function* () {
          const current = yield* SubscriptionRef.get(state);
          const latest = current.servers.get(input.serverId);
          if (latest === undefined || !recordMatches(registration, latest)) {
            return yield* oauthError("The server's settings changed during sign-in. Try again.");
          }
          yield* dropFlow(input.serverId);
          // A refresh may have rotated the token while discovery ran; keep the newest one.
          const previous = current.records.get(input.serverId);
          const record: StoredRecord =
            previous?.clientId === registration.clientId
              ? {
                  ...registration,
                  ...(previous.token === undefined ? {} : { token: previous.token }),
                  ...(previous.needsLogin === undefined ? {} : { needsLogin: previous.needsLogin }),
                }
              : registration;
          yield* setRecord(input.serverId, record);
          const verifier = yield* randomToken(32);
          const challenge = Base64Url.encode(
            yield* digest
              .digest(Digest.sha256, new TextEncoder().encode(verifier))
              .pipe(Effect.orDie),
          );
          const oauthState = yield* randomToken(24);
          const flowId = yield* randomToken(16);
          const authorizationUrl = OAuthClient.authorizationUrl({
            server: {
              issuer: record.issuer,
              authorizationEndpoint: record.authorizationEndpoint,
              tokenEndpoint: record.tokenEndpoint,
              registrationEndpoint: record.registrationEndpoint ?? undefined,
              resource: record.resource,
              scopes: record.scopes,
            },
            clientId: record.clientId,
            redirectUri: record.redirectUri,
            codeChallenge: challenge,
            state: oauthState,
          });
          const scope = yield* Scope.fork(serviceScope);
          const serverReceivesCallback =
            input.callbackMode === "server" &&
            (yield* listen(input.serverId, flowId, record.redirectUri, scope));
          const expiresAt = (yield* Clock.currentTimeMillis) + Duration.toMillis(FLOW_TTL);
          yield* Effect.sleep(FLOW_TTL).pipe(
            Effect.andThen(takeFlow(input.serverId, flowId)),
            Effect.andThen(closeLater(scope)),
            Effect.forkIn(scope),
          );
          yield* SubscriptionRef.update(state, (current) => {
            const flows = new Map(current.flows);
            flows.set(input.serverId, {
              flowId,
              state: oauthState,
              verifier,
              authorizationUrl,
              expiresAt,
              serverReceivesCallback,
              scope,
            });
            return { ...current, flows };
          });
          return statusOf(yield* SubscriptionRef.get(state), input.serverId);
        }),
      );
    },
  );

  const complete: ExternalMcpOAuthShape["complete"] = (input) =>
    finish(input.serverId, input.flowId, input.callbackUrl).pipe(
      Effect.tapError((error) => setError(input.serverId, error.message)),
      Effect.withSpan("ExternalMcpOAuth.complete"),
    );

  const cancel: ExternalMcpOAuthShape["cancel"] = Effect.fn("ExternalMcpOAuth.cancel")(
    function* (input) {
      const flow = yield* takeFlow(input.serverId, input.flowId);
      if (flow !== undefined) yield* Scope.close(flow.scope, Exit.void);
      return yield* currentStatus(input.serverId);
    },
  );

  const logout: ExternalMcpOAuthShape["logout"] = Effect.fn("ExternalMcpOAuth.logout")(
    function* (input) {
      yield* mutex.withPermit(
        Effect.gen(function* () {
          yield* dropFlow(input.serverId);
          yield* setError(input.serverId, undefined);
          const record = (yield* SubscriptionRef.get(state)).records.get(input.serverId);
          if (record === undefined) return;
          const { token: _token, needsLogin: _needsLogin, ...registration } = record;
          yield* setRecord(input.serverId, registration);
        }),
      );
      return yield* currentStatus(input.serverId);
    },
  );

  const isStale = (token: typeof OAuthToken.Type, now: number, rejected: string | undefined) =>
    (rejected !== undefined && token.accessToken === rejected) ||
    (token.expiresAt !== undefined && now >= token.expiresAt - REFRESH_SKEW_MS);

  const withoutToken = (record: StoredRecord, needsLogin: string): StoredRecord => {
    const { token: _token, ...rest } = record;
    return { ...rest, needsLogin };
  };

  /** Writes only if settings and the token are still what the caller started from. */
  const writeIfUnchanged = (
    serverId: string,
    token: typeof OAuthToken.Type,
    next: (record: StoredRecord) => StoredRecord,
  ) =>
    mutex.withPermit(
      Effect.gen(function* () {
        const record = liveRecord(yield* SubscriptionRef.get(state), serverId);
        if (record?.token?.accessToken !== token.accessToken) return false;
        yield* setRecord(serverId, next(record));
        return true;
      }),
    );

  const accessToken: ExternalMcpOAuthShape["accessToken"] = Effect.fn(
    "ExternalMcpOAuth.accessToken",
  )(function* (serverId, rejected) {
    const fast = liveRecord(yield* SubscriptionRef.get(state), serverId)?.token;
    if (fast === undefined) return Option.none();
    if (!isStale(fast, yield* Clock.currentTimeMillis, rejected)) {
      return Option.some(fast.accessToken);
    }
    const lock = yield* refreshLock(serverId);
    return yield* lock.withPermit(
      Effect.gen(function* () {
        const record = liveRecord(yield* SubscriptionRef.get(state), serverId);
        const token = record?.token;
        if (record === undefined || token === undefined) return Option.none<string>();
        const now = yield* Clock.currentTimeMillis;
        // Another request refreshed it while this one waited.
        if (!isStale(token, now, rejected)) return Option.some(token.accessToken);
        if (token.refreshToken === undefined) {
          yield* writeIfUnchanged(serverId, token, (current) =>
            withoutToken(current, "The sign-in expired. Sign in again."),
          );
          return Option.none<string>();
        }
        const refreshed = yield* withHttp(
          OAuthClient.requestToken(record.tokenEndpoint, registrationOf(record), {
            grant_type: "refresh_token",
            refresh_token: token.refreshToken,
            resource: record.resource,
          }),
        ).pipe(Effect.result);
        if (Result.isSuccess(refreshed)) {
          const written = yield* writeIfUnchanged(serverId, token, (current) => ({
            ...current,
            token: refreshed.success,
          }));
          if (written) return Option.some(refreshed.success.accessToken);
          // A sign-in replaced the token meanwhile; that one is newer.
          const latest = liveRecord(yield* SubscriptionRef.get(state), serverId)?.token;
          return latest === undefined ? Option.none<string>() : Option.some(latest.accessToken);
        }
        if (refreshed.failure.permanent) {
          yield* writeIfUnchanged(serverId, token, (current) =>
            withoutToken(current, refreshed.failure.message),
          );
          return Option.none<string>();
        }
        yield* Effect.logWarning("MCP OAuth refresh failed; will retry", {
          serverId,
          message: refreshed.failure.message,
        });
        // A transient failure keeps a token that has not actually expired yet.
        return token.expiresAt !== undefined && now >= token.expiresAt
          ? Option.none<string>()
          : Option.some(token.accessToken);
      }),
    );
  });

  const invalidate: ExternalMcpOAuthShape["invalidate"] = Effect.fn("ExternalMcpOAuth.invalidate")(
    function* (serverId, token) {
      yield* writeIfUnchanged(serverId, { accessToken: token }, (record) =>
        withoutToken(record, "The MCP server refused the sign-in. Sign in again."),
      );
    },
  );

  return ExternalMcpOAuth.of({
    // Settings edits unrelated to MCP update the state too; clients only hear real changes.
    statuses: SubscriptionRef.changes(state).pipe(
      Stream.map(statusesOf),
      Stream.changesWith((left, right) => JSON.stringify(left) === JSON.stringify(right)),
    ),
    connectedServerIds: SubscriptionRef.get(state).pipe(Effect.map(connectedIds)),
    connectedServerIdsChanges: SubscriptionRef.changes(state).pipe(
      Stream.map(connectedIds),
      Stream.changesWith(sameSet),
    ),
    start,
    complete,
    cancel,
    logout,
    accessToken,
    invalidate,
  });
});

function toRecordEndpoints(server: OAuthClient.AuthorizationServer) {
  return {
    issuer: server.issuer,
    authorizationEndpoint: server.authorizationEndpoint,
    tokenEndpoint: server.tokenEndpoint,
    registrationEndpoint: server.registrationEndpoint ?? null,
    resource: server.resource,
    scopes: [...server.scopes],
  };
}

export const layer = Layer.effect(ExternalMcpOAuth, make);
