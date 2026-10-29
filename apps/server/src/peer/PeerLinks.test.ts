import { NodeHttpServer } from "@effect/platform-node";
import { type ExecutionEnvironmentDescriptor, OrchestratorMcpFailure } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { Tool, Toolkit } from "effect/ai";
import {
  FetchHttpClient,
  HttpClient,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import { ChildProcessSpawner } from "effect/process";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  descriptorOf,
  layerLinkingEnvironment,
  servePeer,
  type ServedPeer,
} from "./PeerLinks.testkit.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";

/** What B's probe saw: the calling client and the modes it may start work with. */
const Probe = Schema.Struct({ caller: Schema.String, modes: Schema.String });

const ProbeToolkit = Toolkit.make(
  Tool.make("probe", {
    description: "Reports the calling client and the modes it may start work with.",
    success: Probe,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
  Tool.make("refuse", {
    description: "Always refuses.",
    success: Probe,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
  }),
);

const probeHandlers = McpToolAccess.toLayer(ProbeToolkit, {
  probe: McpToolAccess.reads(() =>
    McpInvocationContext.McpInvocationContext.pipe(
      Effect.map((scope) => {
        const modes = McpInvocationContext.clientModeCeiling(scope.client);
        return {
          caller: scope.client?.label ?? "none",
          modes: `${modes.runtimeMode}/${modes.interactionMode}`,
        };
      }),
    ),
  ),
  refuse: McpToolAccess.reads(() =>
    Effect.fail(
      new OrchestratorMcpFailure({ code: "thread_not_found", message: "No such thread." }),
    ),
  ),
});

const serveB = (descriptor: ExecutionEnvironmentDescriptor) =>
  servePeer(descriptor, McpHttpServer.toolkitRegistration(ProbeToolkit, probeHandlers));

/** Environment A, which links to B. */
const makeA = (
  descriptor: ExecutionEnvironmentDescriptor,
  secrets?: typeof ServerSecretStore.layer,
  network?: Layer.Layer<HttpClient.HttpClient>,
  tailscale?: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>,
) =>
  layerLinkingEnvironment(descriptor, secrets, network, tailscale).pipe(
    Layer.build,
    Effect.map((context) => ({
      links: Context.get(context, PeerLinks.PeerLinks),
      peer: Context.get(context, PeerMcpClient.PeerMcpClient),
      sql: Context.get(context, SqlClient.SqlClient),
      settings: Context.get(context, ServerSettings.ServerSettingsService),
    })),
  );

const laptop = descriptorOf("environment-laptop", "Laptop");
const box = descriptorOf("environment-box", "Box");
const probe = (
  a: Effect.Success<ReturnType<typeof makeA>>,
  limits: PeerMcpClient.PeerCallLimits = { runtimeMode: "full-access", interactionMode: "default" },
  tool = "probe",
) =>
  a.peer.call({
    environmentId: box.environmentId,
    tool,
    arguments: {},
    limits,
    success: Probe,
  });

const linkedSessions = (b: ServedPeer) => b.linkedSessions;

/** Another environment on a socket of its own, answering as `descriptor`. */
const serveApart = (descriptor: ExecutionEnvironmentDescriptor) =>
  Layer.build(Layer.fresh(NodeHttpServer.layerTest)).pipe(
    Effect.flatMap((server) => serveB(descriptor).pipe(Effect.provideContext(server))),
  );

it.effect("links with a pairing code, and the peer holds its agents to the limits they carry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);

      const refused = yield* a.links
        .link({ url: b.url, pairingCode: "not-a-code", access: "auto" })
        .pipe(Effect.flip);
      expect(refused.reason).toBe("pairing_rejected");
      expect(yield* linkedSessions(b)).toEqual([]);

      const pairing = yield* b.auth.issuePairingCredential();
      const linked = yield* a.links.link({
        url: b.url,
        pairingCode: pairing.credential,
        access: "auto",
      });
      expect(linked).toMatchObject({
        environmentId: box.environmentId,
        label: "Box",
        urls: [b.url],
        access: "auto",
      });
      // B lists the link under A's name, where it can be revoked.
      expect((yield* linkedSessions(b)).map((session) => session.client.label)).toEqual([
        "T3 Code · Laptop",
      ]);
      expect(
        (yield* a.links.list).map(({ environmentId, status }) => [environmentId, status]),
      ).toEqual([[box.environmentId, "reachable"]]);

      // The caller's limits narrow the link's access; they never widen it.
      expect(
        yield* probe(a, { runtimeMode: "approval-required", interactionMode: "plan" }),
      ).toEqual({ caller: "T3 Code · Laptop", modes: "approval-required/plan" });
      expect(yield* probe(a)).toEqual({ caller: "T3 Code · Laptop", modes: "auto/default" });

      // A refusal keeps the peer's reason.
      const notFound = yield* probe(a, undefined, "refuse").pipe(Effect.flip);
      expect(notFound).toMatchObject({ code: "thread_not_found", message: "No such thread." });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "each machine's Linked environments beta gates links, and a link waits while it is off",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        const pairing = yield* b.auth.issuePairingCredential();
        const link = a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });

        yield* a.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        expect(yield* link.pipe(Effect.flip)).toMatchObject({
          reason: "disabled",
          message: PeerLinks.LINKED_ENVIRONMENTS_OFF,
        });
        expect(yield* Ref.get(b.bearers)).toEqual([]);

        yield* a.settings.updateSettings({ linkedEnvironmentsEnabled: true });
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        const refusedThere = yield* link.pipe(Effect.flip);
        expect(refusedThere.reason).toBe("pairing_rejected");
        expect(refusedThere.message).toContain("Linked environments is off on that machine");
        expect(yield* linkedSessions(b)).toEqual([]);

        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: true });
        yield* link;
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        // Not final, so a task already running there picks up again once it is back on.
        const waiting = yield* probe(a).pipe(Effect.flip);
        expect(waiting.code).toBe("orchestration_error");
        expect(waiting.message).toContain("has Linked environments turned off");
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: true });
        expect(yield* probe(a)).toEqual({ caller: "T3 Code · Laptop", modes: "auto/default" });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a link whose row cannot be stored keeps none of its token", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const held = yield* Ref.make<ReadonlySet<string>>(new Set());
      const noted = Layer.effect(
        ServerSecretStore.ServerSecretStore,
        Effect.map(ServerSecretStore.make, (store) =>
          ServerSecretStore.ServerSecretStore.of({
            ...store,
            set: (name, value) =>
              store
                .set(name, value)
                .pipe(Effect.andThen(Ref.update(held, (names) => new Set([...names, name])))),
            remove: (name) =>
              store
                .remove(name)
                .pipe(
                  Effect.andThen(
                    Ref.update(held, (names) => new Set([...names].filter((at) => at !== name))),
                  ),
                ),
          }),
        ),
      );
      const a = yield* makeA(laptop, noted);
      yield* a.sql`
        CREATE TRIGGER refuse_link BEFORE INSERT ON peer_environment_links
        BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END
      `;
      const pairing = yield* b.auth.issuePairingCredential();
      const failed = yield* a.links
        .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
        .pipe(Effect.flip);
      expect(failed.reason).toBe("storage");
      expect(yield* Ref.get(held)).toEqual(new Set());
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("two links racing to one peer leave the stored access matching the token", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const [first, second] = yield* Effect.all(
        [b.auth.issuePairingCredential(), b.auth.issuePairingCredential()],
        { concurrency: 2 },
      );
      yield* Effect.all(
        [
          a.links.link({ url: b.url, pairingCode: first.credential, access: "read-only" }),
          a.links.link({ url: b.url, pairingCode: second.credential, access: "full-access" }),
        ],
        { concurrency: 2 },
      );
      const [stored] = yield* a.links.list;
      // The token sent is the one issued with the access the row records.
      const { modes } = yield* probe(a);
      expect(modes).toBe(
        stored!.access === "read-only" ? "approval-required/default" : "full-access/default",
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** A bare HTTP server on a socket of its own, answering with `handle`. */
const serveRaw = <E, R>(
  handle: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
) =>
  Layer.build(Layer.fresh(NodeHttpServer.layerTest)).pipe(
    Effect.flatMap((server) =>
      Effect.gen(function* () {
        yield* HttpServer.serve(Effect.flatMap(HttpServerRequest.HttpServerRequest, handle)).pipe(
          Layer.build,
        );
        const address = (yield* HttpServer.HttpServer).address;
        if (address._tag === "UnixPathAddress") return yield* Effect.die("expected a TCP address");
        return `http://127.0.0.1:${address.port}`;
      }).pipe(Effect.provideContext(server)),
    ),
  );

it.effect("never follows a peer's redirect with the pairing code", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const received = yield* Ref.make<ReadonlyArray<string>>([]);
      const elsewhere = yield* serveRaw((request) =>
        Ref.update(received, (seen) => [...seen, `${request.method} ${request.url}`]).pipe(
          Effect.as(HttpServerResponse.empty({ status: 404 })),
        ),
      );
      // Answers as B, then sends every linking request on to another address.
      const front = yield* serveRaw((request) =>
        request.url.startsWith("/.well-known/t3/environment")
          ? HttpClient.get(`${b.url}${request.url}`).pipe(
              Effect.flatMap((response) => response.text),
              Effect.map((body) =>
                HttpServerResponse.text(body, { contentType: "application/json" }),
              ),
              Effect.provide(FetchHttpClient.layer),
              Effect.orDie,
            )
          : Effect.succeed(
              HttpServerResponse.empty({
                status: 307,
                headers: { location: `${elsewhere}${request.url}` },
              }),
            ),
      );
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links
        .link({ url: front, pairingCode: pairing.credential, access: "auto" })
        .pipe(Effect.flip);
      expect(yield* Ref.get(received)).toEqual([]);
      expect(yield* a.links.list).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("never sends a link's token to an address that stops answering as that peer", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      yield* probe(a);
      expect((yield* Ref.get(b.bearers)).length).toBeGreaterThan(0);

      // The address now answers as another environment, as a reused LAN address would.
      yield* Ref.set(b.descriptor, descriptorOf("environment-impostor", "Impostor"));
      yield* Ref.set(b.bearers, []);
      const failed = yield* probe(a).pipe(Effect.flip);
      expect(failed.message).toContain("Box did not answer");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["unreachable"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

const BOX_TAILNET_ADDRESS = "100.100.1.2";

/** Carries the tailnet address to the local socket, as Tailscale would. */
const layerTailnetRoute = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const fetch = yield* FetchHttpClient.Fetch;
    return HttpClient.transform(client, (response) =>
      Effect.provideService(response, FetchHttpClient.Fetch, (input, init) =>
        fetch(String(input).replace(BOX_TAILNET_ADDRESS, "127.0.0.1"), init),
      ),
    );
  }),
).pipe(Layer.provide(FetchHttpClient.layer));

/** This machine's `tailscale status`: running with Box as a peer while `listed` holds. */
const layerTailscaleStatus = (listed: Ref.Ref<boolean>) =>
  Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make(() =>
      Effect.map(Ref.get(listed), (running) =>
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.make(
            new TextEncoder().encode(
              JSON.stringify({
                BackendState: running ? "Running" : "Stopped",
                Peer: { "nodekey:box": { TailscaleIPs: [BOX_TAILNET_ADDRESS] } },
              }),
            ),
          ),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
    ),
  );

it.effect("sends a link's token to a Tailscale IPv4 address only while Tailscale lists it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const listed = yield* Ref.make(false);
      const a = yield* makeA(laptop, undefined, layerTailnetRoute, layerTailscaleStatus(listed));
      const tailnetUrl = b.url.replace("127.0.0.1", BOX_TAILNET_ADDRESS);

      // Unlisted, 100.64.0.0/10 may be a carrier's or WARP's: the pairing code never goes there.
      const pairing = yield* b.auth.issuePairingCredential();
      const refused = yield* a.links
        .link({ url: tailnetUrl, pairingCode: pairing.credential, access: "auto" })
        .pipe(Effect.flip);
      expect(refused.message).toContain("plain http");
      expect(yield* Ref.get(b.probes)).toBe(0);

      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      yield* a.sql`UPDATE peer_environment_links SET urls_json = ${JSON.stringify([tailnetUrl])}`;
      expect(Option.isNone(yield* a.links.describe(tailnetUrl))).toBe(true);

      yield* Ref.set(listed, true);
      yield* TestClock.adjust("5 seconds");
      expect(Option.map(yield* a.links.describe(tailnetUrl), (peer) => peer.label)).toEqual(
        Option.some("Box"),
      );
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["reachable"]);
      yield* Ref.set(b.bearers, []);
      yield* probe(a);
      expect((yield* Ref.get(b.bearers)).length).toBeGreaterThan(0);

      yield* Ref.set(listed, false);
      yield* TestClock.adjust("5 seconds");
      yield* Ref.set(b.bearers, []);
      const failed = yield* probe(a).pipe(Effect.flip);
      expect(failed.message).toContain("Box did not answer");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["unreachable"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("reads one stored link without asking the peer", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      yield* Ref.set(b.probes, 0);

      const stored = yield* a.links.get(box.environmentId);
      expect(Option.map(stored, (link) => link.label)).toEqual(Option.some("Box"));
      expect(Option.isNone(yield* a.links.get(laptop.environmentId))).toBe(true);
      expect(yield* Ref.get(b.probes)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("tells the agent to link again once the peer revokes the link or it expires", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      yield* probe(a);

      const [session] = yield* linkedSessions(b);
      yield* b.auth.revokeSession(session!.sessionId);
      const revoked = yield* probe(a).pipe(Effect.flip);
      expect(revoked.code).toBe("capability_denied");
      expect(revoked.message).toContain("revoked there");

      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      // A link made with a peer that later answers without the fence, as
      // one made before the fence or a peer moved back to such a version.
      yield* Ref.set(b.descriptor, descriptorOf("environment-box", "Box", { linkFence: false }));
      yield* Ref.set(b.bearers, []);
      const unfenced = yield* probe(a).pipe(Effect.flip);
      expect(unfenced.code).toBe("capability_denied");
      expect(unfenced.message).toContain("cannot keep a linked agent's limits");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      yield* Ref.set(b.descriptor, box);

      // A link stored before links were marked: its session there is not a
      // link's, so the peer would not fence its work even though it can.
      yield* a.sql`UPDATE peer_environment_links SET fenced = 0`;
      // Linking again is its only way back, as for an expired link.
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["expired"]);
      const legacy = yield* probe(a).pipe(Effect.flip);
      expect(legacy.code).toBe("capability_denied");
      expect(legacy.message).toContain("Link it again");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* probe(a);

      yield* TestClock.adjust(Duration.days(31));
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["expired"]);
      yield* Ref.set(b.bearers, []);
      const expired = yield* probe(a).pipe(Effect.flip);
      expect(expired.code).toBe("capability_denied");
      expect(expired.message).toContain("expired");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "refuses to link itself, plain http off this machine and the tailnet, or a peer that cannot hold a linked agent to its limits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(
          descriptorOf("environment-box", "Box", { mcpModeLimitHeader: false }),
        );
        const a = yield* makeA(laptop);
        const pairing = yield* b.auth.issuePairingCredential();
        const older = yield* a.links
          .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
          .pipe(Effect.flip);
        expect(older.reason).toBe("incompatible");
        // Nor one whose link's work could reach the user's own threads there.
        yield* Ref.set(b.descriptor, descriptorOf("environment-box", "Box", { linkFence: false }));
        const unfenced = yield* a.links
          .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
          .pipe(Effect.flip);
        expect(unfenced.reason).toBe("incompatible");

        // Plain http to a LAN address would carry the code and token in clear.
        const lan = yield* a.links
          .link({
            url: "http://192.168.1.20:3773",
            pairingCode: pairing.credential,
            access: "auto",
          })
          .pipe(Effect.flip);
        expect(lan.message).toContain("plain http");
        const lanAlternate = yield* a.links
          .link({
            url: b.url,
            alternateUrls: ["http://192.168.1.20:3773"],
            pairingCode: pairing.credential,
            access: "auto",
          })
          .pipe(Effect.flip);
        expect(lanAlternate.message).toContain("plain http");

        yield* Ref.set(b.descriptor, laptop);
        const self = yield* a.links
          .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
          .pipe(Effect.flip);
        expect(self.reason).toBe("self");
        // Neither attempt spent the pairing code on a session.
        expect(yield* linkedSessions(b)).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "linking a picked machine skips an address that answers as another, and never signs in there",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const impostor = yield* serveApart(descriptorOf("environment-impostor", "Impostor"));
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        const pairing = yield* b.auth.issuePairingCredential();
        const linked = yield* a.links.link({
          url: impostor.url,
          alternateUrls: [b.url],
          pairingCode: pairing.credential,
          access: "auto",
          expectedEnvironmentId: box.environmentId,
        });
        expect(linked).toMatchObject({ environmentId: box.environmentId, urls: [b.url] });
        yield* probe(a);
        expect(yield* linkedSessions(impostor)).toEqual([]);
        expect(yield* Ref.get(impostor.bearers)).toEqual([]);
        expect((yield* linkedSessions(b)).length).toBe(1);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("linking a picked machine that answers nowhere names it and links nothing", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const impostor = yield* serveApart(descriptorOf("environment-impostor", "Impostor"));
      const a = yield* makeA(laptop);
      const failed = yield* a.links
        .link({
          url: impostor.url,
          alternateUrls: ["http://192.168.1.20:3773"],
          pairingCode: "unused",
          access: "auto",
          expectedEnvironmentId: box.environmentId,
          expectedLabel: "Box",
        })
        .pipe(Effect.flip);
      expect(failed.reason).toBe("unreachable");
      expect(failed.message).toBe(
        `This environment could not reach Box at ${impostor.url}, http://192.168.1.20:3773.`,
      );
      expect(yield* a.links.list).toEqual([]);
      expect(yield* linkedSessions(impostor)).toEqual([]);

      // Every address refused: the message says why.
      const refused = yield* a.links
        .link({
          url: "http://192.168.1.20:3773",
          pairingCode: "unused",
          access: "auto",
          expectedEnvironmentId: box.environmentId,
          expectedLabel: "Box",
        })
        .pipe(Effect.flip);
      expect(refused.message).toContain("could not reach Box");
      expect(refused.message).toContain("plain http");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
