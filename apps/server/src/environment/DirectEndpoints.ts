import type { ServerDirectEndpoint } from "@t3tools/contracts";
import {
  buildTailscaleHttpsBaseUrl,
  probeTailscaleHttpsEndpoint,
  readTailscaleStatus,
} from "@t3tools/tailscale";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/http";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";

import { isTailnetHost } from "@t3tools/shared/hostClassification";

import * as ServerConfig from "../config.ts";
import {
  formatHostForUrl,
  isLoopbackHost,
  isPrivateNetworkAddress,
  isWildcardHost,
  VIRTUAL_INTERFACE,
} from "../startupAccess.ts";

type NetworkInterfacesMap = ReturnType<typeof NodeOS.networkInterfaces>;

// Stay below the config discovery timeout so slow Tailscale drops only the tailnet name.
const TAILSCALE_ENDPOINT_TIMEOUT = Duration.seconds(3);
// Laptops switch networks and Tailscale can connect after startup; a check reads only interfaces.
const ADDRESS_CHECK_INTERVAL = Duration.seconds(30);

export class DirectEndpoints extends Context.Service<
  DirectEndpoints,
  {
    readonly resolve: () => Effect.Effect<ReadonlyArray<ServerDirectEndpoint>>;
    /** This machine's own plain-http endpoints, read now and then every 30 seconds. */
    readonly streamBoundEndpoints: Stream.Stream<ReadonlyArray<ServerDirectEndpoint>>;
  }
>()("t3/environment/DirectEndpoints") {}

/**
 * `bound` in place of the plain-http endpoints. A Tailscale Serve address stays
 * as proven when the client subscribed: a probe during a network switch can fail.
 */
export const replaceBoundEndpoints = (
  current: ReadonlyArray<ServerDirectEndpoint>,
  bound: ReadonlyArray<ServerDirectEndpoint>,
): ReadonlyArray<ServerDirectEndpoint> => [
  ...bound,
  ...current.filter((endpoint) => !endpoint.httpBaseUrl.startsWith("http:")),
];

// Credentials travel over HTTP; public IPs leak them and hostnames can resolve differently per client.
const isAdvertisableAddress = (address: string): boolean =>
  NodeNet.isIPv4(address) && !address.startsWith("127.") && isPrivateNetworkAddress(address);

// macOS utun names need Tailscale's IPv6 range to identify them; WARP shares its IPv4 range.
const isTailscaleInterface = (
  name: string,
  entries: ReadonlyArray<NodeOS.NetworkInterfaceInfo>,
): boolean =>
  /^tailscale/i.test(name) ||
  entries.some((entry) => entry.family === "IPv6" && isTailnetHost(entry.address));

export function resolveBoundEndpoints(input: {
  readonly host: string | undefined;
  readonly port: number;
  readonly interfaces: NetworkInterfacesMap;
  /** A cloud VM's private network is its provider's, which no client shares. */
  readonly onCloudVm?: boolean;
}): ReadonlyArray<ServerDirectEndpoint> {
  if (isLoopbackHost(input.host)) return [];
  const tailscaleAddresses = new Set(
    Object.entries(input.interfaces).flatMap(([name, entries = []]) =>
      isTailscaleInterface(name, entries) ? entries.map((entry) => entry.address) : [],
    ),
  );
  const addresses = isWildcardHost(input.host)
    ? Object.entries(input.interfaces)
        .flatMap(([name, entries]) => (VIRTUAL_INTERFACE.test(name) ? [] : (entries ?? [])))
        .filter(
          (entry) =>
            !entry.internal && entry.family === "IPv4" && isAdvertisableAddress(entry.address),
        )
        .map((entry) => entry.address)
    : [input.host!].filter(isAdvertisableAddress);
  return [...new Set(addresses)]
    .filter((address) => !input.onCloudVm || tailscaleAddresses.has(address))
    .map((address) => ({
      kind: tailscaleAddresses.has(address) ? "tailnet" : "lan",
      httpBaseUrl: `http://${formatHostForUrl(address)}:${input.port}/`,
    }));
}

/**
 * AWS, GCP, Azure, DigitalOcean, Hetzner, Linode, Vultr, and Oracle all serve
 * instance metadata at this address. A plain TCP connect skips HTTP proxies,
 * which would otherwise answer for a machine that is not in a cloud.
 */
const CLOUD_METADATA_ADDRESS = "169.254.169.254";
const CLOUD_METADATA_TIMEOUT_MS = 300;

const probeCloudMetadata = Effect.callback<boolean>((resume) => {
  const socket = NodeNet.connect({
    host: CLOUD_METADATA_ADDRESS,
    port: 80,
    timeout: CLOUD_METADATA_TIMEOUT_MS,
  });
  let settled = false;
  const settle = (found: boolean) => {
    if (settled) return;
    settled = true;
    socket.destroy();
    resume(Effect.succeed(found));
  };
  socket.once("connect", () => settle(true));
  socket.once("timeout", () => settle(false));
  socket.once("error", () => settle(false));
  return Effect.sync(() => socket.destroy());
});

/** @public */
export const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const httpClient = yield* HttpClient.HttpClient;
  // Only the service default listens on every interface without being asked,
  // so only it checks once whether it runs in a cloud. The check starts now so
  // the first client does not wait for its timeout.
  const onCloudVm = config.privateNetworkOnly
    ? yield* Effect.cached(probeCloudMetadata)
    : Effect.succeed(false);
  yield* Effect.forkDetach(onCloudVm);

  const boundEndpoints = Effect.gen(function* () {
    return resolveBoundEndpoints({
      host: config.host,
      port: config.port,
      interfaces: NodeOS.networkInterfaces(),
      onCloudVm: yield* onCloudVm,
    });
  });

  const resolve = Effect.gen(function* () {
    const endpoints = [...(yield* boundEndpoints)];
    // Serve setup can fail without stopping startup; advertise only after it answers as this server.
    if (config.tailscaleServeEnabled) {
      const servedUrl = yield* readTailscaleStatus.pipe(
        Effect.map((status) => status.magicDnsName),
        Effect.flatMap((magicDnsName) => {
          if (magicDnsName === null) return Effect.succeed(null);
          const httpBaseUrl = buildTailscaleHttpsBaseUrl({
            magicDnsName,
            servePort: config.tailscaleServePort,
          });
          return probeTailscaleHttpsEndpoint({ baseUrl: httpBaseUrl }).pipe(
            Effect.map((reachable) => (reachable ? httpBaseUrl : null)),
          );
        }),
        Effect.timeoutOption(TAILSCALE_ENDPOINT_TIMEOUT),
        Effect.map(Option.flatMap(Option.fromNullishOr)),
        Effect.orElseSucceed(() => Option.none<string>()),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );
      if (Option.isSome(servedUrl)) {
        endpoints.push({ kind: "tailnet", httpBaseUrl: servedUrl.value });
      }
    }
    return endpoints;
  });

  return DirectEndpoints.of({
    resolve: Effect.fn("DirectEndpoints.resolve")(function () {
      return resolve;
    }),
    streamBoundEndpoints: isLoopbackHost(config.host)
      ? Stream.empty
      : Stream.tick(ADDRESS_CHECK_INTERVAL).pipe(Stream.mapEffect(() => boundEndpoints)),
  });
});

export const layer = Layer.effect(DirectEndpoints, make);
