import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/http";

import * as OAuthClient from "./ExternalMcpOAuthClient.ts";

type Route = (body: URLSearchParams) => Response | "unreachable";

const fakeHttp = (routes: Readonly<Record<string, Route>>) => {
  const requested: Array<string> = [];
  const sent: Array<{ url: string; headers: Readonly<Record<string, string>> }> = [];
  const client = HttpClient.make((request) => {
    requested.push(request.url);
    sent.push({ url: request.url, headers: request.headers });
    const body = new URLSearchParams(
      request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "",
    );
    const route = routes[request.url];
    const response = route === undefined ? new Response(null, { status: 404 }) : route(body);
    return response === "unreachable"
      ? Effect.fail(
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({ request, cause: new Error("offline") }),
          }),
        )
      : Effect.succeed(HttpClientResponse.fromWeb(request, response));
  });
  return { client, requested, sent };
};

const json =
  (body: unknown, status = 200) =>
  () =>
    Response.json(body, { status });

const authServer = (issuer: string, overrides: Record<string, unknown> = {}) =>
  json({
    issuer,
    authorization_endpoint: `${issuer}/authorize`,
    token_endpoint: `${issuer}/token`,
    registration_endpoint: `${issuer}/register`,
    code_challenge_methods_supported: ["S256"],
    ...overrides,
  });

const unauthorized = (challenge?: string) => () =>
  new Response(null, {
    status: 401,
    ...(challenge === undefined ? {} : { headers: { "www-authenticate": challenge } }),
  });

const discoverWith = (
  routes: Readonly<Record<string, Route>>,
  serverUrl: string,
  configuredScopes?: ReadonlyArray<string>,
) => {
  const http = fakeHttp(routes);
  return OAuthClient.discover(serverUrl, configuredScopes).pipe(
    Effect.provideService(HttpClient.HttpClient, http.client),
    Effect.map((server) => ({ server, requested: http.requested })),
  );
};

describe("ExternalMcpOAuthClient.discover", () => {
  it.effect("follows the challenge's resource_metadata and takes the challenge scope", () =>
    Effect.gen(function* () {
      const { server, requested } = yield* discoverWith(
        {
          "https://mcp.example.com/mcp": unauthorized(
            'Bearer resource_metadata="https://mcp.example.com/meta/prm", scope="read write"',
          ),
          "https://mcp.example.com/meta/prm": json({
            resource: "https://mcp.example.com/mcp",
            authorization_servers: ["https://auth.example.com"],
            scopes_supported: ["ignored"],
          }),
          "https://auth.example.com/.well-known/oauth-authorization-server": authServer(
            "https://auth.example.com",
          ),
        },
        "https://mcp.example.com/mcp",
      );
      expect(server).toEqual({
        issuer: "https://auth.example.com",
        authorizationEndpoint: "https://auth.example.com/authorize",
        tokenEndpoint: "https://auth.example.com/token",
        registrationEndpoint: "https://auth.example.com/register",
        resource: "https://mcp.example.com/mcp",
        scopes: ["read", "write"],
      });
      expect(requested).not.toContain(
        "https://mcp.example.com/.well-known/oauth-protected-resource/mcp",
      );
    }),
  );

  it.effect("tries the path-suffixed protected-resource document before the root one", () =>
    Effect.gen(function* () {
      const { server, requested } = yield* discoverWith(
        {
          "https://mcp.example.com/.well-known/oauth-protected-resource": json({
            authorization_servers: ["https://auth.example.com"],
            scopes_supported: ["tools"],
          }),
          "https://auth.example.com/.well-known/oauth-authorization-server": authServer(
            "https://auth.example.com",
          ),
        },
        "https://mcp.example.com/v1/mcp",
      );
      expect(requested.slice(0, 3)).toEqual([
        "https://mcp.example.com/v1/mcp",
        "https://mcp.example.com/.well-known/oauth-protected-resource/v1/mcp",
        "https://mcp.example.com/.well-known/oauth-protected-resource",
      ]);
      expect(server.resource).toBe("https://mcp.example.com/v1/mcp");
      expect(server.scopes).toEqual(["tools"]);
    }),
  );

  it.effect("falls back to OpenID configuration appended to an issuer path", () =>
    Effect.gen(function* () {
      const issuer = "https://auth.example.com/tenant";
      const { server } = yield* discoverWith(
        {
          "https://mcp.example.com/.well-known/oauth-protected-resource": json({
            authorization_servers: [issuer],
          }),
          "https://auth.example.com/tenant/.well-known/openid-configuration": authServer(issuer),
        },
        "https://mcp.example.com/mcp",
        ["configured"],
      );
      expect(server.issuer).toBe(issuer);
      expect(server.scopes).toEqual(["configured"]);
    }),
  );

  it.effect("treats the MCP origin as the issuer when no protected-resource document exists", () =>
    Effect.gen(function* () {
      const { server } = yield* discoverWith(
        {
          "https://mcp.example.com/.well-known/oauth-authorization-server":
            authServer("https://mcp.example.com"),
        },
        "https://mcp.example.com/mcp",
      );
      expect(server.issuer).toBe("https://mcp.example.com");
      expect(server.scopes).toEqual([]);
    }),
  );

  it.effect("rejects metadata it cannot trust", () =>
    Effect.gen(function* () {
      const prm = {
        "https://mcp.example.com/.well-known/oauth-protected-resource": json({
          authorization_servers: ["https://auth.example.com"],
        }),
      };
      const metadataUrl = "https://auth.example.com/.well-known/oauth-authorization-server";
      const cases: ReadonlyArray<[Readonly<Record<string, Route>>, string]> = [
        [{ ...prm, [metadataUrl]: authServer("https://other.example.com") }, "different issuer"],
        [
          {
            ...prm,
            [metadataUrl]: authServer("https://auth.example.com", {
              token_endpoint: "http://auth.example.com/token",
            }),
          },
          "insecure endpoint",
        ],
        [
          {
            ...prm,
            [metadataUrl]: authServer("https://auth.example.com", {
              code_challenge_methods_supported: ["plain"],
            }),
          },
          "PKCE",
        ],
        [
          {
            "https://mcp.example.com/.well-known/oauth-protected-resource": json({
              authorization_servers: ["http://auth.example.com"],
            }),
          },
          "usable authorization server",
        ],
        [prm, "No OAuth metadata"],
      ];
      for (const [routes, message] of cases) {
        const error = yield* discoverWith(routes, "https://mcp.example.com/mcp").pipe(Effect.flip);
        expect(error.message).toContain(message);
      }
    }),
  );
});

describe("ExternalMcpOAuthClient.requestToken", () => {
  const tokenEndpoint = "https://auth.example.com/token";
  const publicClient = { clientId: "t3", tokenEndpointAuthMethod: "none" } as const;

  it.effect("only treats a dead grant or client as permanent", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<[Route, boolean]> = [
        [json({ error: "invalid_grant" }, 400), true],
        [json({ error: "invalid_client" }, 401), true],
        [json({ error: "temporarily_unavailable" }, 400), false],
        [() => new Response("nope", { status: 400 }), true],
        [() => new Response("busy", { status: 429 }), false],
        [() => new Response("down", { status: 500 }), false],
        [() => "unreachable", false],
        [json({ access_token: "a", token_type: "mac" }), false],
      ];
      for (const [route, permanent] of cases) {
        const http = fakeHttp({ [tokenEndpoint]: route });
        const error = yield* OAuthClient.requestToken(tokenEndpoint, publicClient, {
          grant_type: "refresh_token",
          refresh_token: "r1",
        }).pipe(Effect.provideService(HttpClient.HttpClient, http.client), Effect.flip);
        expect(error.permanent).toBe(permanent);
      }
    }),
  );

  it.effect("keeps an unrotated refresh token and dates expiry from the clock", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(1_000_000);
      let body = new URLSearchParams();
      const http = fakeHttp({
        [tokenEndpoint]: (sent) => {
          body = sent;
          return Response.json({ access_token: "a2", token_type: "Bearer", expires_in: 60 });
        },
      });
      const token = yield* OAuthClient.requestToken(
        tokenEndpoint,
        { clientId: "id:1", clientSecret: "s&1", tokenEndpointAuthMethod: "client_secret_basic" },
        { grant_type: "refresh_token", refresh_token: "r1" },
      ).pipe(Effect.provideService(HttpClient.HttpClient, http.client));
      expect(token).toEqual({ accessToken: "a2", refreshToken: "r1", expiresAt: 1_060_000 });
      expect(body.has("client_id")).toBe(false);
      expect(http.sent[0]?.headers.authorization).toBe(`Basic ${btoa("id%3A1:s%261")}`);
    }),
  );
});

describe("ExternalMcpOAuthClient helpers", () => {
  it("reads one auth-param from a Bearer challenge", () => {
    const header = 'Bearer realm="mcp", resource_metadata="https://x/prm",scope="a b"';
    expect(OAuthClient.readChallengeParam(header, "resource_metadata")).toBe("https://x/prm");
    expect(OAuthClient.readChallengeParam(header, "scope")).toBe("a b");
    expect(OAuthClient.readChallengeParam(header, "SCOPE")).toBe("a b");
    expect(OAuthClient.readChallengeParam('Bearer xscope="evil"', "scope")).toBeUndefined();
    expect(OAuthClient.readChallengeParam(undefined, "scope")).toBeUndefined();
  });

  it("accepts HTTPS and loopback HTTP endpoints only", () => {
    const cases: ReadonlyArray<[string, boolean]> = [
      ["https://auth.example.com/token", true],
      ["http://localhost:8080/token", true],
      ["http://127.0.0.1/token", true],
      ["http://[::1]/token", true],
      ["http://auth.example.com/token", false],
      ["http://localhost.example.com/token", false],
      ["https://user:pass@auth.example.com/token", false],
      ["javascript:alert(1)", false],
      ["not a url", false],
    ];
    for (const [value, secure] of cases) {
      expect([value, OAuthClient.isSecureEndpoint(value)]).toEqual([value, secure]);
    }
  });
});
