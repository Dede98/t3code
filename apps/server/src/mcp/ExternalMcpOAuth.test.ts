import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ExternalMcpServerId,
  type ExternalMcpServerConfig,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as ExternalMcpOAuth from "./ExternalMcpOAuth.ts";

const serverId = ExternalMcpServerId.make("linear");
const MCP_URL = "https://mcp.example.com/mcp";
const ISSUER = "https://auth.example.com";

const settingsWith = (servers: Record<string, ExternalMcpServerConfig>): ServerSettings => ({
  ...DEFAULT_SERVER_SETTINGS,
  externalMcpServers: servers as ServerSettings["externalMcpServers"],
});
const oauthServer: ExternalMcpServerConfig = {
  url: MCP_URL,
  headers: [],
  enabled: true,
  auth: { type: "oauth" },
};

type RefreshMode = "ok" | "invalid_grant" | "down";

const withOAuth = <A, E>(
  body: (h: {
    readonly oauth: ExternalMcpOAuth.ExternalMcpOAuthShape;
    readonly stored: Map<string, Uint8Array>;
    readonly settings: SubscriptionRef.SubscriptionRef<ServerSettings>;
    readonly tokenBodies: Array<URLSearchParams>;
    readonly registrations: () => number;
    readonly setRefresh: (mode: RefreshMode) => void;
    readonly gateRefresh: Effect.Effect<{
      readonly entered: Deferred.Deferred<void>;
      readonly release: Deferred.Deferred<void>;
    }>;
  }) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const stored = new Map<string, Uint8Array>();
    const settings = yield* SubscriptionRef.make(settingsWith({ [serverId]: oauthServer }));
    const tokenBodies: Array<URLSearchParams> = [];
    let registrations = 0;
    let refreshMode: RefreshMode = "ok";
    let gate: { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> } | undefined;

    const tokenResponse = (form: URLSearchParams) =>
      Effect.gen(function* () {
        if (form.get("grant_type") === "authorization_code") {
          return Response.json({
            access_token: "access-1",
            refresh_token: "refresh-1",
            expires_in: 3600,
          });
        }
        if (gate !== undefined) {
          yield* Deferred.succeed(gate.entered, undefined);
          yield* Deferred.await(gate.release);
        }
        if (refreshMode === "invalid_grant") {
          return Response.json({ error: "invalid_grant" }, { status: 400 });
        }
        if (refreshMode === "down") return new Response("down", { status: 503 });
        return Response.json({ access_token: "access-2", expires_in: 3600 });
      });

    const routes: Record<string, (form: URLSearchParams) => Effect.Effect<Response>> = {
      [MCP_URL]: () => Effect.succeed(new Response(null, { status: 401 })),
      "https://mcp.example.com/.well-known/oauth-protected-resource": () =>
        Effect.succeed(Response.json({ authorization_servers: [ISSUER] })),
      [`${ISSUER}/.well-known/oauth-authorization-server`]: () =>
        Effect.succeed(
          Response.json({
            issuer: ISSUER,
            authorization_endpoint: `${ISSUER}/authorize`,
            token_endpoint: `${ISSUER}/token`,
            registration_endpoint: `${ISSUER}/register`,
          }),
        ),
      [`${ISSUER}/register`]: () =>
        Effect.sync(() => {
          registrations += 1;
          return Response.json({ client_id: "dyn-client" });
        }),
      [`${ISSUER}/token`]: (form) => {
        tokenBodies.push(form);
        return tokenResponse(form);
      },
    };
    const http = HttpClient.make((request) => {
      const form = new URLSearchParams(
        request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "",
      );
      const route = routes[request.url];
      return (route ? route(form) : Effect.succeed(new Response(null, { status: 404 }))).pipe(
        Effect.map((response) => HttpClientResponse.fromWeb(request, response)),
      );
    });

    const dependencies = Layer.mergeAll(
      NodeCrypto.layer,
      Layer.succeed(HttpClient.HttpClient, http),
      Layer.mock(ServerSecretStore.ServerSecretStore)({
        get: (name) => Effect.sync(() => Option.fromNullishOr(stored.get(name))),
        set: (name, value) => Effect.sync(() => void stored.set(name, value)),
        remove: (name) => Effect.sync(() => void stored.delete(name)),
      }),
      Layer.mock(ServerSettingsService)({
        getSettings: SubscriptionRef.get(settings),
        streamChanges: SubscriptionRef.changes(settings),
      }),
    );
    return yield* Effect.gen(function* () {
      const oauth = yield* ExternalMcpOAuth.ExternalMcpOAuth;
      return yield* body({
        oauth,
        stored,
        settings,
        tokenBodies,
        registrations: () => registrations,
        setRefresh: (mode) => {
          refreshMode = mode;
        },
        gateRefresh: Effect.gen(function* () {
          const next = {
            entered: yield* Deferred.make<void>(),
            release: yield* Deferred.make<void>(),
          };
          gate = next;
          return next;
        }),
      });
    }).pipe(Effect.provide(ExternalMcpOAuth.layer.pipe(Layer.provide(dependencies))));
  });

const startFlow = (oauth: ExternalMcpOAuth.ExternalMcpOAuthShape) =>
  Effect.gen(function* () {
    const status = yield* oauth.start({ serverId, callbackMode: "client" });
    const flow = status.flow;
    if (flow === null) return yield* Effect.die("start opened no flow");
    return { flow, url: new URL(flow.authorizationUrl) };
  });

/** Browsers can hide `http://`, so the pasted redirect carries no scheme. */
const signIn = (oauth: ExternalMcpOAuth.ExternalMcpOAuthShape) =>
  Effect.gen(function* () {
    const { flow, url } = yield* startFlow(oauth);
    const redirect = new URL(url.searchParams.get("redirect_uri") ?? "");
    const state = url.searchParams.get("state") ?? "";
    const status = yield* oauth.complete({
      serverId,
      flowId: flow.flowId,
      callbackUrl: `${redirect.host}${redirect.pathname}?code=code-1&state=${state}`,
    });
    return { status, url };
  });

const currentStatus = (oauth: ExternalMcpOAuth.ExternalMcpOAuthShape) =>
  oauth.statuses.pipe(
    Stream.runHead,
    Effect.map((statuses) => Option.getOrThrow(statuses)[serverId]),
  );

const storedRecord = (stored: Map<string, Uint8Array>) => {
  const [bytes] = stored.values();
  return bytes === undefined ? undefined : JSON.parse(new TextDecoder().decode(bytes));
};

const base64Url = (bytes: ArrayBuffer) =>
  btoa(String.fromCharCode(...new Uint8Array(bytes)))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/u, "");

describe("ExternalMcpOAuth", () => {
  it.effect("opens a PKCE flow for a pasted callback with the MCP resource indicator", () =>
    withOAuth(({ oauth, registrations }) =>
      Effect.gen(function* () {
        const { flow, url } = yield* startFlow(oauth);
        expect(flow.serverReceivesCallback).toBe(false);
        expect(`${url.origin}${url.pathname}`).toBe(`${ISSUER}/authorize`);
        expect(url.searchParams.get("client_id")).toBe("dyn-client");
        expect(url.searchParams.get("code_challenge_method")).toBe("S256");
        expect(url.searchParams.get("code_challenge")).toMatch(/^[\w-]{43}$/u);
        expect(url.searchParams.get("state")).toMatch(/^[\w-]{32}$/u);
        expect(url.searchParams.get("resource")).toBe(MCP_URL);
        expect(url.searchParams.get("redirect_uri")).toMatch(
          /^http:\/\/127\.0\.0\.1:\d+\/callback$/u,
        );
        expect(registrations()).toBe(1);
      }),
    ),
  );

  it.effect("exchanges a pasted callback for a token and reports the server connected", () =>
    withOAuth(({ oauth, stored, tokenBodies }) =>
      Effect.gen(function* () {
        const { status, url } = yield* signIn(oauth);
        expect(status).toMatchObject({
          state: "connected",
          issuer: ISSUER,
          flow: null,
          message: null,
        });
        expect([...(yield* oauth.connectedServerIds)]).toEqual([serverId]);
        const exchange = tokenBodies.at(-1);
        expect(exchange?.get("code")).toBe("code-1");
        expect(exchange?.get("resource")).toBe(MCP_URL);
        const verifier = exchange?.get("code_verifier") ?? "";
        const digest = yield* Effect.promise(() =>
          crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier)),
        );
        expect(url.searchParams.get("code_challenge")).toBe(base64Url(digest));
        expect(storedRecord(stored)?.token?.accessToken).toBe("access-1");
      }),
    ),
  );

  it.effect("refreshes an expiring token once for concurrent callers", () =>
    withOAuth(({ oauth, tokenBodies, gateRefresh }) =>
      Effect.gen(function* () {
        yield* signIn(oauth);
        expect(yield* oauth.accessToken(serverId)).toEqual(Option.some("access-1"));
        yield* TestClock.adjust("59 minutes");
        const gate = yield* gateRefresh;
        const first = yield* Effect.forkChild(oauth.accessToken(serverId));
        const second = yield* Effect.forkChild(oauth.accessToken(serverId));
        yield* Deferred.await(gate.entered);
        yield* Deferred.succeed(gate.release, undefined);
        expect(yield* Fiber.join(first)).toEqual(Option.some("access-2"));
        expect(yield* Fiber.join(second)).toEqual(Option.some("access-2"));
        const refreshes = tokenBodies.filter((form) => form.get("grant_type") === "refresh_token");
        expect(refreshes.map((form) => form.get("refresh_token"))).toEqual(["refresh-1"]);
        expect(refreshes[0]?.get("resource")).toBe(MCP_URL);
      }),
    ),
  );

  it.effect("keeps a still-valid token through a transient refresh failure", () =>
    withOAuth(({ oauth, setRefresh }) =>
      Effect.gen(function* () {
        yield* signIn(oauth);
        setRefresh("down");
        yield* TestClock.adjust("59 minutes");
        expect(yield* oauth.accessToken(serverId)).toEqual(Option.some("access-1"));
        yield* TestClock.adjust("1 minute");
        expect(yield* oauth.accessToken(serverId)).toEqual(Option.none());
        expect((yield* currentStatus(oauth))?.state).toBe("connected");
      }),
    ),
  );

  it.effect("asks for a new sign-in when the refresh grant is rejected", () =>
    withOAuth(({ oauth, setRefresh, stored }) =>
      Effect.gen(function* () {
        yield* signIn(oauth);
        setRefresh("invalid_grant");
        yield* TestClock.adjust("1 hour");
        expect(yield* oauth.accessToken(serverId)).toEqual(Option.none());
        const status = yield* currentStatus(oauth);
        expect(status?.state).toBe("needs-login");
        expect(status?.message).toContain("invalid_grant");
        expect((yield* oauth.connectedServerIds).has(serverId)).toBe(false);
        expect(storedRecord(stored)).toMatchObject({ clientId: "dyn-client" });
        expect(storedRecord(stored)?.token).toBeUndefined();
      }),
    ),
  );

  it.effect("deletes the stored credential when the server leaves settings", () =>
    withOAuth(({ oauth, stored, settings }) =>
      Effect.gen(function* () {
        yield* signIn(oauth);
        expect(stored.size).toBe(1);
        yield* SubscriptionRef.set(settings, settingsWith({}));
        yield* oauth.connectedServerIdsChanges.pipe(
          Stream.filter((ids) => !ids.has(serverId)),
          Stream.runHead,
        );
        expect(stored.size).toBe(0);
      }),
    ),
  );

  it.effect("logout drops the token but reuses the client registration", () =>
    withOAuth(({ oauth, stored, registrations }) =>
      Effect.gen(function* () {
        yield* signIn(oauth);
        const status = yield* oauth.logout({ serverId });
        expect(status.state).toBe("signed-out");
        expect((yield* oauth.connectedServerIds).size).toBe(0);
        expect(yield* oauth.accessToken(serverId)).toEqual(Option.none());
        expect(storedRecord(stored)).toMatchObject({ clientId: "dyn-client" });
        expect(storedRecord(stored)?.token).toBeUndefined();
        yield* signIn(oauth);
        expect(registrations()).toBe(1);
      }),
    ),
  );
});

describe("readCallback", () => {
  const flow = {
    redirectUri: "http://127.0.0.1:50000/callback",
    state: "s1",
    issuer: "https://auth.example.com/",
  };

  it("accepts the redirect with or without its scheme", () => {
    for (const value of [
      "http://127.0.0.1:50000/callback?code=c1&state=s1",
      "  127.0.0.1:50000/callback?code=c1&state=s1&iss=https%3A%2F%2Fauth.example.com ",
    ]) {
      expect(ExternalMcpOAuth.readCallback(value, flow)).toEqual({ code: "c1" });
    }
  });

  it("returns the authorization server's error", () => {
    expect(
      ExternalMcpOAuth.readCallback(
        "127.0.0.1:50000/callback?error=access_denied&error_description=User%20said%20no&state=s1",
        flow,
      ),
    ).toEqual({ error: "User said no" });
    expect(
      ExternalMcpOAuth.readCallback("127.0.0.1:50000/callback?error=access_denied&state=s1", flow),
    ).toEqual({ error: "access_denied" });
  });

  it("rejects a redirect that does not belong to this sign-in", () => {
    for (const value of [
      "127.0.0.1:50000/callback?code=c1&state=other",
      "127.0.0.1:50000/callback?code=c1",
      "127.0.0.1:50000/callback?code=c1&state=s1&state=s1",
      "127.0.0.1:50001/callback?code=c1&state=s1",
      "https://127.0.0.1:50000/callback?code=c1&state=s1",
      "127.0.0.1:50000/elsewhere?code=c1&state=s1",
      "127.0.0.1:50000/callback?code=c1&state=s1&iss=https%3A%2F%2Fevil.example.com",
      "127.0.0.1:50000/callback?state=s1",
      "",
    ]) {
      expect(() => ExternalMcpOAuth.readCallback(value, flow), value).toThrow();
    }
  });
});
