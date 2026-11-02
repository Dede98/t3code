import type {
  ExternalMcpServerConfig,
  ProviderInstanceId,
  ServerSettings,
} from "@t3tools/contracts";

export interface ExternalMcpServer extends ExternalMcpServerConfig {
  readonly name: string;
}

/** Served by T3 next to `/mcp`; it attaches the server's OAuth token to every request. */
export const EXTERNAL_MCP_PROXY_PATH_PREFIX = "/mcp-external/";

const isOAuth = (server: ExternalMcpServerConfig) => server.auth?.type === "oauth";

/**
 * OAuth servers are attached only while signed in, so a sign-in or sign-out changes the
 * resolved set and rebuilds provider instances like any other MCP settings change.
 */
export function resolveExternalMcpServers(
  settings: Pick<ServerSettings, "externalMcpServers">,
  instanceId: ProviderInstanceId,
  connectedOAuthServers: ReadonlySet<string> = new Set(),
): ReadonlyArray<ExternalMcpServer> {
  return Object.entries(settings.externalMcpServers).flatMap(([name, server]) => {
    if (!server.enabled) return [];
    if (server.providerInstances && !server.providerInstances.includes(instanceId)) return [];
    if (isOAuth(server) && !connectedOAuthServers.has(name)) return [];
    return [{ name, ...server }];
  });
}

/**
 * Points OAuth servers at T3's proxy with the session's own MCP credential. The upstream
 * token stays on the server, and its refreshes never touch a running provider session.
 */
export function proxyOAuthExternalMcpServers(
  servers: ReadonlyArray<ExternalMcpServer>,
  session: { readonly endpoint: string; readonly authorizationHeader: string },
): ReadonlyArray<ExternalMcpServer> {
  return servers.map((server) =>
    isOAuth(server)
      ? {
          ...server,
          url: new URL(
            `${EXTERNAL_MCP_PROXY_PATH_PREFIX}${encodeURIComponent(server.name)}`,
            session.endpoint,
          ).toString(),
          headers: [{ name: "Authorization", value: session.authorizationHeader, sensitive: true }],
        }
      : server,
  );
}

export function externalMcpHeadersRecord(server: ExternalMcpServer): Record<string, string> {
  return Object.fromEntries(server.headers.map((header) => [header.name, header.value]));
}
