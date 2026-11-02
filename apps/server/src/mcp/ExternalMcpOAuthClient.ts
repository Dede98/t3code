/**
 * The client half of the MCP authorization spec: discovery (RFC 9728, RFC 8414), dynamic
 * client registration (RFC 7591), and the authorization-code + PKCE token endpoint calls.
 */
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

const REQUEST_TIMEOUT = Duration.seconds(20);

export class ExternalMcpOAuthClientError extends Schema.TaggedError<ExternalMcpOAuthClientError>()(
  "ExternalMcpOAuthClientError",
  {
    message: Schema.String,
    /** The grant or client is gone for good; signing in again is the only fix. */
    permanent: Schema.Boolean,
  },
) {}

const fail = (message: string, permanent = false) =>
  Effect.fail(new ExternalMcpOAuthClientError({ message, permanent }));

const isLoopbackHost = (hostname: string) =>
  hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";

/**
 * Same rule as MCP URLs in settings: HTTPS, or HTTP on loopback. A remote server's metadata
 * may not point at loopback, or it could steer this machine's requests to local services.
 */
export function isSecureEndpoint(value: string, allowLoopback = true): boolean {
  if (!URL.canParse(value)) return false;
  const url = new URL(value);
  if (url.username || url.password) return false;
  if (isLoopbackHost(url.hostname)) {
    return allowLoopback && (url.protocol === "http:" || url.protocol === "https:");
  }
  return url.protocol === "https:";
}

/** RFC 9728 §3.3: the metadata must describe this server, or its tokens would go elsewhere. */
function resourceCovers(resource: string, serverUrl: string): boolean {
  if (!URL.canParse(resource)) return false;
  const claimed = new URL(resource);
  const actual = new URL(serverUrl);
  const path = (value: string) => value.replace(/\/+$/u, "");
  return (
    claimed.origin === actual.origin &&
    (path(claimed.pathname) === "" ||
      path(actual.pathname) === path(claimed.pathname) ||
      path(actual.pathname).startsWith(`${path(claimed.pathname)}/`))
  );
}

const ProtectedResourceMetadata = Schema.Struct({
  resource: Schema.optional(Schema.String),
  authorization_servers: Schema.optional(Schema.Array(Schema.String)),
  scopes_supported: Schema.optional(Schema.Array(Schema.String)),
});

const AuthorizationServerMetadata = Schema.Struct({
  issuer: Schema.String,
  authorization_endpoint: Schema.String,
  token_endpoint: Schema.String,
  registration_endpoint: Schema.optional(Schema.String),
  code_challenge_methods_supported: Schema.optional(Schema.Array(Schema.String)),
  scopes_supported: Schema.optional(Schema.Array(Schema.String)),
});

const RegisteredClient = Schema.Struct({
  client_id: Schema.String,
  client_secret: Schema.optional(Schema.String),
  token_endpoint_auth_method: Schema.optional(Schema.String),
});

const TokenResponse = Schema.Struct({
  access_token: Schema.String,
  token_type: Schema.optional(Schema.String),
  expires_in: Schema.optional(Schema.Number),
  refresh_token: Schema.optional(Schema.String),
  scope: Schema.optional(Schema.String),
});

const ErrorResponse = Schema.Struct({
  error: Schema.String,
  error_description: Schema.optional(Schema.String),
});
const decodeErrorResponse = Schema.decodeUnknownOption(ErrorResponse);

export interface AuthorizationServer {
  readonly issuer: string;
  readonly authorizationEndpoint: string;
  readonly tokenEndpoint: string;
  readonly registrationEndpoint: string | undefined;
  /** RFC 8707 resource indicator for the MCP server. */
  readonly resource: string;
  readonly scopes: ReadonlyArray<string>;
}

export interface ClientRegistration {
  readonly clientId: string;
  readonly clientSecret?: string;
  readonly tokenEndpointAuthMethod: "none" | "client_secret_post" | "client_secret_basic";
}

export interface OAuthToken {
  readonly accessToken: string;
  readonly refreshToken?: string;
  /** Epoch milliseconds; absent when the server did not say. */
  readonly expiresAt?: number;
  readonly scope?: string;
}

/** Pulls one auth-param out of a `WWW-Authenticate: Bearer` challenge. */
export function readChallengeParam(header: string | undefined, name: string): string | undefined {
  if (header === undefined) return undefined;
  const match = new RegExp(`(?:^|[\\s,])${name}="([^"]*)"`, "iu").exec(header);
  return match?.[1];
}

const getJson = Effect.fn("ExternalMcpOAuthClient.getJson")(
  function* <A, I>(schema: Schema.Codec<A, I>, url: string) {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client
      .execute(HttpClientRequest.get(url).pipe(HttpClientRequest.acceptJson))
      .pipe(Effect.timeout(REQUEST_TIMEOUT));
    if (response.status < 200 || response.status >= 300) return Option.none<A>();
    return Option.some(yield* HttpClientResponse.schemaBodyJson(schema)(response));
  },
  (effect) => Effect.orElseSucceed(effect, () => Option.none()),
);

/** RFC 9728 §3.1: the well-known suffix goes between the host and the resource path. */
function wellKnownUrls(base: string, suffix: string): ReadonlyArray<string> {
  const url = new URL(base);
  const path = url.pathname.replace(/\/+$/u, "");
  return [
    ...(path ? [`${url.origin}/.well-known/${suffix}${path}`] : []),
    `${url.origin}/.well-known/${suffix}`,
  ];
}

const firstSome = Effect.fn("ExternalMcpOAuthClient.firstSome")(function* <A, R>(
  attempts: ReadonlyArray<Effect.Effect<Option.Option<A>, never, R>>,
) {
  for (const attempt of attempts) {
    const result = yield* attempt;
    if (Option.isSome(result)) return result;
  }
  return Option.none<A>();
});

const sameIssuer = (left: string, right: string) =>
  left.replace(/\/+$/u, "") === right.replace(/\/+$/u, "");

/**
 * Finds the authorization server for an MCP server: the 401 challenge's `resource_metadata`
 * first, then the well-known protected-resource documents, then the MCP origin itself.
 */
export const discover = Effect.fn("ExternalMcpOAuthClient.discover")(function* (
  serverUrl: string,
  configuredScopes: ReadonlyArray<string> | undefined,
) {
  const client = yield* HttpClient.HttpClient;
  const probe = yield* client
    .execute(
      HttpClientRequest.post(serverUrl).pipe(
        HttpClientRequest.setHeader("accept", "application/json, text/event-stream"),
        HttpClientRequest.bodyJsonUnsafe({
          jsonrpc: "2.0",
          id: 0,
          method: "initialize",
          params: {
            protocolVersion: "2025-06-18",
            capabilities: {},
            clientInfo: { name: "T3 Code", version: "1" },
          },
        }),
      ),
    )
    .pipe(
      Effect.timeout(REQUEST_TIMEOUT),
      Effect.asSome,
      Effect.orElseSucceed(() => Option.none<HttpClientResponse.HttpClientResponse>()),
    );
  const challenge = Option.isSome(probe) ? probe.value.headers["www-authenticate"] : undefined;
  const challengeMetadataUrl = readChallengeParam(challenge, "resource_metadata");
  const allowLoopback = isLoopbackHost(new URL(serverUrl).hostname);
  const resourceMetadata = yield* firstSome([
    ...(challengeMetadataUrl !== undefined && isSecureEndpoint(challengeMetadataUrl, allowLoopback)
      ? [getJson(ProtectedResourceMetadata, challengeMetadataUrl)]
      : []),
    ...wellKnownUrls(serverUrl, "oauth-protected-resource").map((url) =>
      getJson(ProtectedResourceMetadata, url),
    ),
  ]);

  const resource =
    Option.isSome(resourceMetadata) && resourceMetadata.value.resource !== undefined
      ? resourceMetadata.value.resource
      : serverUrl;
  if (!resourceCovers(resource, serverUrl)) {
    return yield* fail("The MCP server's OAuth metadata describes a different server.");
  }
  const issuer = Option.isSome(resourceMetadata)
    ? resourceMetadata.value.authorization_servers?.[0]
    : new URL(serverUrl).origin;
  if (issuer === undefined || !isSecureEndpoint(issuer, allowLoopback)) {
    return yield* fail("The MCP server does not name a usable authorization server.");
  }
  const issuerUrl = new URL(issuer);
  const issuerPath = issuerUrl.pathname.replace(/\/+$/u, "");
  const metadata = yield* firstSome([
    ...wellKnownUrls(issuer, "oauth-authorization-server").map((url) =>
      getJson(AuthorizationServerMetadata, url),
    ),
    ...wellKnownUrls(issuer, "openid-configuration").map((url) =>
      getJson(AuthorizationServerMetadata, url),
    ),
    ...(issuerPath
      ? [
          getJson(
            AuthorizationServerMetadata,
            `${issuerUrl.origin}${issuerPath}/.well-known/openid-configuration`,
          ),
        ]
      : []),
  ]);
  if (Option.isNone(metadata)) {
    return yield* fail(`No OAuth metadata was found for ${issuerUrl.host}.`);
  }
  const server = metadata.value;
  if (!sameIssuer(server.issuer, issuer)) {
    return yield* fail(`The authorization server metadata names a different issuer.`);
  }
  if (
    server.code_challenge_methods_supported !== undefined &&
    !server.code_challenge_methods_supported.includes("S256")
  ) {
    return yield* fail("The authorization server does not support PKCE (S256).");
  }
  for (const endpoint of [
    server.authorization_endpoint,
    server.token_endpoint,
    ...(server.registration_endpoint === undefined ? [] : [server.registration_endpoint]),
  ]) {
    if (!isSecureEndpoint(endpoint, allowLoopback)) {
      return yield* fail("The authorization server advertises an insecure endpoint.");
    }
  }
  const challengeScope = readChallengeParam(challenge, "scope");
  const scopes =
    configuredScopes ??
    (challengeScope !== undefined
      ? challengeScope.split(" ").filter(Boolean)
      : Option.isSome(resourceMetadata)
        ? (resourceMetadata.value.scopes_supported ?? [])
        : []);
  return {
    issuer: server.issuer,
    authorizationEndpoint: server.authorization_endpoint,
    tokenEndpoint: server.token_endpoint,
    registrationEndpoint: server.registration_endpoint,
    resource,
    scopes,
  } satisfies AuthorizationServer;
});

export const register = Effect.fn("ExternalMcpOAuthClient.register")(function* (
  server: AuthorizationServer,
  redirectUri: string,
) {
  if (server.registrationEndpoint === undefined) {
    return yield* fail(
      "This authorization server does not offer dynamic client registration. Enter a client ID for it in Settings.",
    );
  }
  const client = yield* HttpClient.HttpClient;
  const response = yield* client
    .execute(
      HttpClientRequest.post(server.registrationEndpoint).pipe(
        HttpClientRequest.acceptJson,
        HttpClientRequest.bodyJsonUnsafe({
          client_name: "T3 Code",
          redirect_uris: [redirectUri],
          grant_types: ["authorization_code", "refresh_token"],
          response_types: ["code"],
          token_endpoint_auth_method: "none",
          ...(server.scopes.length > 0 ? { scope: server.scopes.join(" ") } : {}),
        }),
      ),
    )
    .pipe(
      Effect.timeout(REQUEST_TIMEOUT),
      Effect.catch(() => fail("Could not reach the authorization server to register T3 Code.")),
    );
  if (response.status < 200 || response.status >= 300) {
    const body = yield* response.json.pipe(Effect.orElseSucceed(() => undefined));
    const error = decodeErrorResponse(body);
    return yield* fail(
      `The authorization server refused to register T3 Code${
        Option.isSome(error) ? `: ${error.value.error_description ?? error.value.error}` : "."
      }`,
    );
  }
  const registered = yield* HttpClientResponse.schemaBodyJson(RegisteredClient)(response).pipe(
    Effect.catch(() => fail("The authorization server sent an unreadable client registration.")),
  );
  const method = registered.token_endpoint_auth_method;
  return {
    clientId: registered.client_id,
    ...(registered.client_secret === undefined ? {} : { clientSecret: registered.client_secret }),
    tokenEndpointAuthMethod:
      registered.client_secret === undefined
        ? "none"
        : method === "client_secret_basic"
          ? "client_secret_basic"
          : "client_secret_post",
  } satisfies ClientRegistration;
});

export function authorizationUrl(input: {
  readonly server: AuthorizationServer;
  readonly clientId: string;
  readonly redirectUri: string;
  readonly codeChallenge: string;
  readonly state: string;
}): string {
  const url = new URL(input.server.authorizationEndpoint);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", input.clientId);
  url.searchParams.set("redirect_uri", input.redirectUri);
  url.searchParams.set("code_challenge", input.codeChallenge);
  url.searchParams.set("code_challenge_method", "S256");
  url.searchParams.set("state", input.state);
  url.searchParams.set("resource", input.server.resource);
  if (input.server.scopes.length > 0) url.searchParams.set("scope", input.server.scopes.join(" "));
  return url.toString();
}

const isPermanentStatus = (status: number) =>
  status >= 400 && status < 500 && status !== 408 && status !== 429;

const PERMANENT_ERRORS = new Set(["invalid_grant", "invalid_client", "unauthorized_client"]);

export const requestToken = Effect.fn("ExternalMcpOAuthClient.requestToken")(function* (
  tokenEndpoint: string,
  registration: ClientRegistration,
  params: Readonly<Record<string, string>>,
) {
  const client = yield* HttpClient.HttpClient;
  const body: Record<string, string> = { ...params };
  let request = HttpClientRequest.post(tokenEndpoint).pipe(HttpClientRequest.acceptJson);
  if (registration.tokenEndpointAuthMethod === "client_secret_basic") {
    request = HttpClientRequest.basicAuth(
      request,
      encodeURIComponent(registration.clientId),
      encodeURIComponent(registration.clientSecret ?? ""),
    );
  } else {
    body.client_id = registration.clientId;
    if (registration.tokenEndpointAuthMethod === "client_secret_post") {
      body.client_secret = registration.clientSecret ?? "";
    }
  }
  const response = yield* client.execute(request.pipe(HttpClientRequest.bodyUrlParams(body))).pipe(
    Effect.timeout(REQUEST_TIMEOUT),
    Effect.catch(() => fail("Could not reach the authorization server.")),
  );
  if (response.status < 200 || response.status >= 300) {
    const errorBody = yield* response.json.pipe(Effect.orElseSucceed(() => undefined));
    const error = decodeErrorResponse(errorBody);
    const code = Option.isSome(error) ? error.value.error : undefined;
    return yield* fail(
      `The authorization server rejected the request${
        Option.isSome(error) ? `: ${error.value.error_description ?? error.value.error}` : "."
      }`,
      code !== undefined ? PERMANENT_ERRORS.has(code) : isPermanentStatus(response.status),
    );
  }
  const token = yield* HttpClientResponse.schemaBodyJson(TokenResponse)(response).pipe(
    Effect.catch(() => fail("The authorization server sent an unreadable token response.")),
  );
  if (token.token_type !== undefined && token.token_type.toLowerCase() !== "bearer") {
    return yield* fail(`Unsupported token type ${token.token_type}.`);
  }
  const now = yield* Clock.currentTimeMillis;
  return {
    accessToken: token.access_token,
    ...((token.refresh_token ?? params.refresh_token)
      ? { refreshToken: token.refresh_token ?? params.refresh_token }
      : {}),
    ...(token.expires_in === undefined ? {} : { expiresAt: now + token.expires_in * 1_000 }),
    ...(token.scope === undefined ? {} : { scope: token.scope }),
  } satisfies OAuthToken;
});
